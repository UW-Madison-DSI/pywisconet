from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from starlette.middleware.wsgi import WSGIMiddleware
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from pytz import timezone
from typing import List, Dict, Any
from pydantic import BaseModel
import numpy as np
import asyncio
import time

from pywisconet.data import *
from pywisconet.process import *

from ag_models_wrappers.process_ibm_risk import *
from ag_models_wrappers.process_wisconet import *

app = FastAPI()

# Endpoint for querying station fields by station_id
@app.get("/station_fields/{station_id}")
def station_fields_query(station_id: str):
    """
    Retrieve the fields for a given station based on its station_id.

    Args:
        station_id (str): The ID of the station to retrieve fields for.

    Returns:
        dict: The fields associated with the specified station.
    """
    try:
        result = station_fields(station_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Station {station_id} not found")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint for querying bulk measures for a given station and date range
@app.get('/bulk_measures/{station_id}')
def bulk_measures_query(
    station_id: str,
    start_date: str = Query(..., description="Start date in format YYYY-MM-DD (e.g., 2024-07-01) assumed CT"),
    end_date: str = Query(..., description="End date in format YYYY-MM-DD (e.g., 2024-07-02) assumed CT"),
    measurements: str = Query(..., description="Measurements (e.g., AIRTEMP, DEW_POINT, WIND_SPEED, RELATIVE_HUMIDITY, ALL the units are F, M/S and % respectively, all means the last 4)"),
    frequency: str = Query(..., description="Frequency of measurements (e.g., MIN60, MIN5, DAILY)")
):
    """
    Query bulk measurements for a given station, date range, and measurement type.

    Args:
        station_id (str): The ID of the station to query data for.
        start_date (str): The start date for querying data.
        end_date (str): The end date for querying data.
        measurements (str): The type of measurements to query.
        frequency (str): The frequency of measurements.

    Returns:
        dict: The bulk measurement data for the specified parameters.
    """
    cols = ['collection_time', 'collection_time_ct', 'hour_ct',
            'value', 'id', 'collection_frequency',
            'final_units', 'measure_type','qualifier', 'source_field',
            'standard_name', 'units_abbrev']

    if measurements is not None:
        # Retrieve fields for the station
        this_station_fields = station_fields(station_id)

        if measurements == 'ALL':
            filtered_field_standard_names = filter_fields(
                this_station_fields,
                criteria=[
                    MeasureType.RELATIVE_HUMIDITY,
                    MeasureType.AIRTEMP,
                    MeasureType.DEW_POINT,
                    MeasureType.WIND_SPEED,
                    CollectionFrequency[frequency]
                ]
            )
        # Filter the fields based on the specific measurement type
        elif measurements == 'RELATIVE_HUMIDITY':
            filtered_field_standard_names = filter_fields(
                this_station_fields,
                criteria=[MeasureType.RELATIVE_HUMIDITY, CollectionFrequency[frequency], Units.PCT]
            )
        elif measurements == 'AIRTEMP':
            filtered_field_standard_names = filter_fields(
                this_station_fields,
                criteria=[MeasureType.AIRTEMP, CollectionFrequency[frequency], Units.FAHRENHEIT]
            )
        elif measurements == 'DEW_POINT':
            filtered_field_standard_names = filter_fields(
                this_station_fields,
                criteria=[MeasureType.DEW_POINT, CollectionFrequency[frequency], Units.FAHRENHEIT]
            )
        elif measurements == 'WIND_SPEED':
            filtered_field_standard_names = filter_fields(
                this_station_fields,
                criteria=[MeasureType.WIND_SPEED, CollectionFrequency[frequency], Units.METERSPERSECOND]
            )

        # Fetch data for the date range
        bulk_measure_response = bulk_measures(
            station_id,
            start_date,
            end_date,
            filtered_field_standard_names
        )
        df = bulk_measures_to_df(bulk_measure_response)
        df['collection_time_utc'] = pd.to_datetime(df['collection_time']).dt.tz_localize('UTC')
        df['collection_time_ct'] = df['collection_time_utc'].dt.tz_convert('US/Central')
        df['hour_ct'] = df['collection_time_ct'].dt.hour
        
        return df[cols].to_dict(orient="records")


# Endpoint for querying data from IBM
@app.get("/wisconet/active_stations/")
def stations_query(
        min_days_active: int,
        start_date: str = Query(..., description="Start date in format YYYY-MM-DD (e.g., 2024-07-01)")
):
    """
    Retrieve all stations based on the minimum number of active days and the start date.

    Args:
        min_days_active (int): The minimum number of days a station should have been active.
        start_date (str): The start date for filtering stations.

    Returns:
        dict: A list of stations matching the provided criteria.
    """
    try:
        start_date = datetime.strptime(start_date.strip(), "%Y-%m-%d").replace(tzinfo=ZoneInfo("UTC"))
        result = all_stations(min_days_active, start_date)
        if result is None:
            raise HTTPException(status_code=404, detail="Stations not found")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint for querying data from IBM
@app.get("/ag_models_wrappers/ibm")
def all_data_from_ibm_query(
    forecasting_date: str,  # Passed as part of the URL path
    latitude: float = Query(..., description="Latitude of the location"),
    longitude: float = Query(..., description="Longitude of the location"),
    token: str = Query(..., description="API token")
):
    """
    Query weather data using the IBM Weather API.

    Args:
        forecasting_date (str): The date for the forecast (YYYY-MM-DD).
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.

    Returns:
        dict: Cleaned daily weather data as JSON serializable records.
    """
    try:
        if token!='':
            weather_data = get_weather(latitude, longitude, forecasting_date)
            df = weather_data['daily']
            df_cleaned = df.replace([np.inf, -np.inf, np.nan], None).where(pd.notnull(df), None)
            return df_cleaned.to_dict(orient="records")
        else:
            return {"Invalid token": 400}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid input: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


# Endpoint for querying data from Wisconet. The retrieved information corresponds with daily aggregations.
@app.get("/ag_models_wrappers/wisconet")
def all_data_from_wisconet_query(
    forecasting_date: str,
    risk_days: int = 1,
    station_id: str = None
):
    """
    Query weather data for a given date and station from Wisconet.

    Args:
        forecasting_date (str): The date for which to retrieve weather data.
        station_id (str, optional): The station ID to filter by.

    Returns:
        dict: Cleaned weather data as JSON serializable records.
    """
    try:
        #df = retrieve_tarspot_all_stations_optimized(input_date=forecasting_date, input_station_id=station_id, days=risk_days)
        df = main(input_date=forecasting_date, input_station_id=station_id, days=risk_days)
        df_cleaned = df.replace([np.inf, -np.inf, np.nan], None).where(pd.notnull(df), None)
        return df_cleaned.to_dict(orient="records")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid input: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

# Root endpoint
@app.get("/")
def read_root():
    return {"message": "Welcome to our Open Source Crop-Disease Forecasting Tool API"}

# Create a WSGI application
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.types import ASGIApp

def create_wsgi_app():
    """
    Create a WSGI app to handle HTTP requests for the FastAPI application.
    """
    async def app(scope, receive, send):
        if scope["type"] == "http":
            await app(scope, receive, send)
        else:
            await send({
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-type", b"text/plain")]
            })
            await send({
                "type": "http.response.body",
                "body": b"Not Found"
            })

    return app

wsgi_app = WSGIMiddleware(create_wsgi_app())

