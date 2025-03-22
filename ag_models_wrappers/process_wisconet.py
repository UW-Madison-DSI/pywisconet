from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import asyncio
import aiohttp
import pytz
import os
import pickle
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
from ag_models_wrappers.forecasting_models import *
from itertools import islice

# Constants - Optimized
try:
    CPU_COUNT = os.cpu_count()
    MAX_WORKERS = min(CPU_COUNT * 5, 150)  # More reasonable worker limit
except:
    MAX_WORKERS = 150

BATCH_SIZE = 20  # Process stations in batches of 10
BASE_URL = "https://wisconet.wisc.edu/api/v1"
STATIONS_TO_EXCLUDE = ['MITEST1', 'WNTEST1']
MIN_DAYS_ACTIVE = 38
STATIONS_CACHE_FILE = "wisconsin_stations_cache.csv"
CACHE_EXPIRY_DAYS = 7  # How often to refresh the cache
MEASUREMENTS_CACHE_DIR = "station_measurements_cache"
os.makedirs(MEASUREMENTS_CACHE_DIR, exist_ok=True)

# Map measures to corresponding columns
MEASURE_MAP_HR = {
    2: "60min_air_temp_f_avg",
    10: "60min_dew_point_f_avg",
    19: "60min_relative_humidity_pct_avg",
    57: "60min_wind_speed_mph_max"
}


# Define the backoff/retry function for API requests
async def api_call_with_retry(session, url, params, max_retries=3):
    """Make API calls with retry logic and exponential backoff"""
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Too Many Requests
                    wait_time = 2 ** attempt  # Exponential backoff
                    print(f"Rate limited, waiting {wait_time}s before retry")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"Error: {response.status} for URL: {url}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1)
                    else:
                        return None
        except Exception as e:
            print(f"Request failed: {e} for URL: {url}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
            else:
                return None
    return None


# Session setup with proper connection pooling
async def get_async_session():
    """Create a properly configured aiohttp session"""
    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=50,  # More reasonable connection limit
            ttl_dns_cache=300  # Cache DNS lookups
        ),
        timeout=aiohttp.ClientTimeout(total=60)  # Add timeout
    )


async def api_call_wisconet_data_async(session, station_id, end_time):
    """Asynchronous function to retrieve hourly weather data and create daily aggregations"""
    try:
        end_date = datetime.strptime(end_time, "%Y-%m-%d")
        start_date = end_date - timedelta(days=MIN_DAYS_ACTIVE)

        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        params = {
            "start_time": start_timestamp,
            "end_time": end_timestamp,
            "fields": "60min_relative_humidity_pct_avg,60min_air_temp_f_avg,60min_dew_point_f_avg,60min_wind_speed_mph_max"
        }

        endpoint = f'/stations/{station_id}/measures'

        # Use the retry function instead of direct session.get
        data = await api_call_with_retry(session, f"{BASE_URL}{endpoint}", params)

        if not data:
            return None

        data = data.get("data", [])
        if not data:
            return None

        df = pd.DataFrame(data)
        if df.empty:
            return None

        # Rest of the processing remains the same as in the original function
        result_df = pd.DataFrame({
            'o_collection_time': pd.to_datetime(df['collection_time'], unit='s'),
            'collection_time': pd.to_datetime(df['collection_time'], unit='s').dt.tz_localize(
                'UTC').dt.tz_convert(
                'US/Central'),
            '60min_air_temp_f_avg': np.nan,
            '60min_dew_point_f_avg': np.nan,
            '60min_wind_speed_mph_max': np.nan,
            '60min_relative_humidity_pct_avg': np.nan
        })

        for measure_id, column_name in MEASURE_MAP_HR.items():
            result_df[column_name] = df['measures'].apply(
                lambda measures: next((m[1] for m in measures if m[0] == measure_id), np.nan))

        result_df['hour'] = result_df['collection_time'].dt.hour

        result_df['rh_night_above_90'] = np.where(
            (result_df['60min_relative_humidity_pct_avg'] >= 90) & (
                    (result_df['hour'] >= 20) | (result_df['hour'] <= 6)),
            1, 0
        )

        result_df['rh_day_above_80'] = np.where(
            (result_df['60min_relative_humidity_pct_avg'] >= 80),
            1, 0
        )
        result_df['collection_time'] = pd.to_datetime(result_df['collection_time'])
        result_df['date'] = result_df['collection_time'].dt.strftime('%Y-%m-%d')

        daily_rh_above_90 = result_df.groupby('date').agg(
            nhours_rh_above_90=('rh_night_above_90', 'sum'),
            hours_rh_above_80_day=('rh_day_above_80', 'sum'),
            rh_max=('60min_relative_humidity_pct_avg', 'max'),
            rh_min=('60min_relative_humidity_pct_avg', 'min'),
            rh_avg=('60min_relative_humidity_pct_avg', 'mean'),
            max_ws=('60min_wind_speed_mph_max', 'max'),
            air_temp_max_f=('60min_air_temp_f_avg', 'max'),
            air_temp_min_f=('60min_air_temp_f_avg', 'min'),
            air_temp_avg_f=('60min_air_temp_f_avg', 'mean'),
            min_dp=('60min_dew_point_f_avg', 'min'),
            max_dp=('60min_dew_point_f_avg', 'max'),
            avg_dp=('60min_dew_point_f_avg', 'mean')
        ).reset_index()

        daily_rh_above_90['min_dp_c'] = fahrenheit_to_celsius(daily_rh_above_90['min_dp'])
        daily_rh_above_90['air_temp_max_c'] = fahrenheit_to_celsius(daily_rh_above_90['air_temp_max_f'])
        daily_rh_above_90['air_temp_min_c'] = fahrenheit_to_celsius(daily_rh_above_90['air_temp_min_f'])
        daily_rh_above_90['air_temp_avg_c'] = daily_rh_above_90[['air_temp_max_c', 'air_temp_min_c']].mean(axis=1)
        daily_rh_above_90['rh_above_90_night_14d_ma'] = daily_rh_above_90['nhours_rh_above_90'].rolling(
            window=14, min_periods=14).mean()
        daily_rh_above_90['rh_above_80_day_30d_ma'] = daily_rh_above_90['hours_rh_above_80_day'].rolling(
            window=30, min_periods=30).mean()

        # Calculate moving averages
        daily_rh_above_90['air_temp_min_c_21d_ma'] = daily_rh_above_90['air_temp_min_c'].rolling(window=21,
                                                                                                 min_periods=21).mean()
        daily_rh_above_90['air_temp_max_c_30d_ma'] = daily_rh_above_90['air_temp_max_c'].rolling(window=30,
                                                                                                 min_periods=30).mean()
        daily_rh_above_90['air_temp_avg_c_30d_ma'] = daily_rh_above_90['air_temp_avg_c'].rolling(window=30,
                                                                                                 min_periods=30).mean()
        daily_rh_above_90['rh_max_30d_ma'] = daily_rh_above_90['rh_max'].rolling(window=30, min_periods=30).mean()
        daily_rh_above_90['max_ws_30d_ma'] = daily_rh_above_90['max_ws'].rolling(window=30, min_periods=30).mean()
        daily_rh_above_90['dp_min_30d_c_ma'] = daily_rh_above_90['min_dp_c'].rolling(window=30, min_periods=30).mean()

        return daily_rh_above_90

    except Exception as e:
        print(f"Failed to retrieve or process hourly data for station {station_id}: {e}")
        traceback.print_exc()
        return None


async def one_day_measurements_async(session, station_id, end_time, days):
    """Asynchronous version of one_day_measurements with caching"""
    try:
        # Check cache first
        cache_file = f"{MEASUREMENTS_CACHE_DIR}/{station_id}_{end_time}_{days}.pkl"

        if os.path.exists(cache_file):
            cache_mod_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
            cache_age_hours = (datetime.now() - cache_mod_time).total_seconds() / 3600

            if cache_age_hours < 6:  # Use cache if less than 6 hours old
                print(f"Using cached data for station {station_id}")
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)

        if days is None or days == 0:
            days = 1
        elif days > 7:
            days = 7  # Limit to 7 days for performance

        # Run API call to get daily data
        daily_rh_above_90 = await api_call_wisconet_data_async(session, station_id, end_time)

        # Validate the data
        if daily_rh_above_90 is None:
            print(f"No RH data found for station {station_id}.")
            return None

        # Process the data - optimize by limiting to needed days first
        # Merge on the 'date' column
        combined_df = daily_rh_above_90[['date', 'rh_above_90_night_14d_ma',
                                         'rh_above_80_day_30d_ma',
                                         'air_temp_max_c_30d_ma', 'air_temp_min_c_21d_ma',
                                         'air_temp_avg_c_30d_ma', 'rh_max_30d_ma', 'max_ws_30d_ma',
                                         'dp_min_30d_c_ma']]

        combined_df['station_id'] = station_id
        combined_df = combined_df.sort_values('date', ascending=False).head(days)

        # Save to cache
        with open(cache_file, 'wb') as f:
            pickle.dump(combined_df, f)

        return combined_df

    except Exception as e:
        print(f"Error in processing data for station {station_id}: {e}")
        return None


# New function to process stations in batches
async def process_stations_in_batches(session, station_ids, input_date, days):
    """Process stations in batches rather than one at a time"""
    all_results = []

    # Split station_ids into batches
    for i in range(0, len(station_ids), BATCH_SIZE):
        batch = station_ids[i:i + BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1} with {len(batch)} stations")

        # Create tasks for all stations in this batch
        tasks = [one_day_measurements_async(session, station_id, input_date, days)
                 for station_id in batch]

        # Run the batch tasks concurrently
        batch_results = await asyncio.gather(*tasks)

        # Filter out None results and add to results list
        valid_results = [res for res in batch_results if res is not None]
        all_results.extend(valid_results)

        # Add a small delay between batches to avoid overloading the API
        if i + BATCH_SIZE < len(station_ids):
            await asyncio.sleep(0.5)

    if not all_results:
        return None

    # Combine all batch results
    return all_results


def compute_risks(df_chunk):
    """Compute risk metrics for a chunk of data"""
    df_chunk = df_chunk.copy()

    # Apply risk calculations using vectorized operations where possible
    # For tarspot risk
    tarspot_results = df_chunk.apply(
        lambda row: pd.Series(
            calculate_tarspot_risk_function(
                row['air_temp_avg_c_30d_ma'],
                row['rh_max_30d_ma'],
                row['rh_above_90_night_14d_ma']
            )
        ),
        axis=1
    )
    df_chunk[['tarspot_risk', 'tarspot_risk_class']] = tarspot_results

    # For gray leaf spot risk
    gls_results = df_chunk.apply(
        lambda row: pd.Series(
            calculate_gray_leaf_spot_risk_function(
                row['air_temp_min_c_21d_ma'],
                row['dp_min_30d_c_ma']
            )
        ),
        axis=1
    )
    df_chunk[['gls_risk', 'gls_risk_class']] = gls_results

    # For frogeye leaf spot risk
    fe_results = df_chunk.apply(
        lambda row: pd.Series(
            calculate_frogeye_leaf_spot_function(
                row['air_temp_max_c_30d_ma'],
                row['rh_above_80_day_30d_ma']
            )
        ),
        axis=1
    )
    df_chunk[['fe_risk', 'fe_risk_class']] = fe_results

    # For white mold irrigated risk
    whitemold_irr_results = df_chunk.apply(
        lambda row: pd.Series(
            calculate_irrigated_risk(
                row['air_temp_max_c_30d_ma'],
                row['rh_max_30d_ma']
            )
        ),
        axis=1
    )

    df_chunk[['whitemold_irr_30in_risk', 'whitemold_irr_15in_risk', 'whitemold_irr_15in_class',
              'whitemold_irr_30in_class']] = whitemold_irr_results

    # For white mold non-irrigated risk
    whitemold_nirr_results = df_chunk.apply(
        lambda row: pd.Series(
            calculate_non_irrigated_risk(
                row['air_temp_max_c_30d_ma'],
                row['rh_max_30d_ma'],
                row['max_ws_30d_ma']
            )
        ),
        axis=1
    )
    df_chunk[['whitemold_nirr_risk', 'whitemold_nirr_risk_class']] = whitemold_nirr_results

    return df_chunk


# Improved function to split data into roughly equal chunks
def chunk_dataframe(df, num_chunks):
    """Split DataFrame into chunks based on optimal size"""
    if len(df) <= 100:
        # For small dataframes, just use a single chunk
        return [df]

    # For larger dataframes, create chunks of reasonable size
    optimal_chunk_size = min(max(len(df) // num_chunks, 100), 1000)
    return [df.iloc[i:i + optimal_chunk_size] for i in range(0, len(df), optimal_chunk_size)]


async def get_stations_with_caching(session, input_date):
    """
    Retrieves station data with caching mechanism to avoid unnecessary API calls.
    Only fetches from API if cache doesn't exist, is expired, or it's the first day of the month
    (to check for new stations).
    """
    today = datetime.now()
    is_first_day = today.day == 8  # Check if it's the first day of the month

    # Check if cache exists and is not expired
    if os.path.exists(STATIONS_CACHE_FILE) and not is_first_day:
        # Check cache file modification time
        cache_mod_time = datetime.fromtimestamp(os.path.getmtime(STATIONS_CACHE_FILE))
        cache_age_days = (today - cache_mod_time).days

        if cache_age_days < CACHE_EXPIRY_DAYS:
            print(f"Using cached station data (last updated {cache_age_days} days ago)")
            return pd.read_csv(STATIONS_CACHE_FILE)

    # If we get here, we need to fetch fresh data
    print("Fetching fresh station data from API...")
    allstations_url = (
        f"https://connect.doit.wisc.edu/pywisconet_wrapper/wisconet/active_stations/"
        f"?min_days_active=15&start_date={input_date}"
    )

    async with session.get(allstations_url) as response:
        if response.status == 200:
            stations_data = await response.json()
            allstations = pd.DataFrame(stations_data)

            # Save to cache file
            allstations.to_csv(STATIONS_CACHE_FILE, index=False)
            print(f"Updated station cache with {len(allstations)} stations")

            return allstations
        else:
            # If API call fails but cache exists, use cache as fallback
            if os.path.exists(STATIONS_CACHE_FILE):
                print(f"API call failed (status {response.status}). Using cached station data as fallback.")
                return pd.read_csv(STATIONS_CACHE_FILE)
            else:
                print(f"Error fetching station data, status code {response.status}")
                return None


async def retrieve_tarspot_all_stations_async(input_date, input_station_id=None, days=1):
    """
    Main asynchronous function to retrieve and process data for all stations.
    """
    FINAL_COLUMNS = [
        'station_id', 'date', 'forecasting_date', 'location',
        'station_name', 'city', 'county', 'latitude',
        'longitude', 'region', 'state',
        'station_timezone',
        'tarspot_risk', 'tarspot_risk_class',
        'gls_risk', 'gls_risk_class',
        'fe_risk', 'fe_risk_class',
        'whitemold_nirr_risk', 'whitemold_nirr_risk_class',
        'whitemold_irr_30in_risk', 'whitemold_irr_15in_risk',
        'whitemold_irr_15in_class', 'whitemold_irr_30in_class'
    ]

    # Create a single session for all API operations
    async with (await get_async_session()) as session:
        # Retrieve all active stations data
        ct = pytz.timezone('America/Chicago')
        today_ct = datetime.now(ct)

        # If it's not day 1 at 6 am CT, load from backup and filter
        if not (today_ct.day == 1 and today_ct.hour == 6):
            df = pd.read_csv('stations_backup.csv')
            df['earliest_api_date'] = pd.to_datetime(df['earliest_api_date'], utc=True).dt.tz_localize(None)
            # Exclude stations in the exclusion list
            df = df[~df['station_id'].isin(STATIONS_TO_EXCLUDE)]
            # Filter by earliest_api_date
            input_date_transformed = pd.to_datetime(input_date)
            date_limit = input_date_transformed - pd.Timedelta(days=32)
            allstations = df[df['earliest_api_date'] < pd.to_datetime(date_limit)]
        else:
            allstations_url = (
                f"https://connect.doit.wisc.edu/pywisconet_wrapper/wisconet/active_stations/"
                f"?min_days_active=15&start_date={input_date}"
            )

            async with session.get(allstations_url) as response:
                if response.status == 200:
                    stations_data = await response.json()
                    allstations = pd.DataFrame(stations_data)
                    allstations = allstations[~allstations['station_id'].isin(STATIONS_TO_EXCLUDE)]
                    allstations.to_csv('stations_backup.csv')
                else:
                    return None

        # Check if a specific station or a list of stations is provided
        if input_station_id:
            # Check if input_station_id is a comma-separated string
            if isinstance(input_station_id, str) and ',' in input_station_id:
                input_str = input_station_id
                station_list = [s.strip() for s in input_str.split(",")]
                stations = allstations[allstations['station_id'].isin(station_list)]

                # Process stations in batches
                results = await process_stations_in_batches(session, station_list, input_date, days)
                if not results:
                    return None

                all_results = pd.concat(results, ignore_index=True)
            else:
                # Process as a single station if input_station_id is not comma separated
                stations = allstations[allstations['station_id'] == input_station_id]
                all_results = await one_day_measurements_async(session, input_station_id, input_date, days)

                if all_results is None:
                    return None
        else:
            # Process all stations in batches
            stations = allstations[~allstations['station_id'].isin(STATIONS_TO_EXCLUDE)]
            station_ids = stations['station_id'].values

            # Process stations in batches
            results = await process_stations_in_batches(session, station_ids, input_date, days)
            if not results:
                return None

            all_results = pd.concat(results, ignore_index=True)

        # Merge station info with API data
        daily_data = stations.merge(all_results, on='station_id', how='inner')

        # Compute risk metrics in parallel using ProcessPoolExecutor for CPU-bound tasks
        num_workers = min(max(len(daily_data) // 100, 1), CPU_COUNT)  # Adjust based on data size and CPU cores

        chunks = chunk_dataframe(daily_data, num_workers)

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(compute_risks, chunk) for chunk in chunks]
            processed_chunks = [future.result() for future in as_completed(futures)]

        # Combine processed chunks
        if not processed_chunks:
            return None

        daily_data = pd.concat(processed_chunks, ignore_index=True)

        # Post-process date columns
        daily_data['date'] = pd.to_datetime(daily_data['date'])
        daily_data['state'] = 'WI'
        #daily_data['forecasting_date'] = (daily_data['date']).dt.strftime('%Y-%m-%d')
        daily_data['forecasting_date'] = (daily_data['date'] + timedelta(days=1)).dt.strftime('%Y-%m-%d')

        # Return only the required columns
        return daily_data[FINAL_COLUMNS]


# Entry point function for running the parallelized code
def main(input_date, input_station_id=None, days=1):
    """Run the async function using asyncio event loop"""
    return asyncio.run(retrieve_tarspot_all_stations_async(input_date, input_station_id, days))
