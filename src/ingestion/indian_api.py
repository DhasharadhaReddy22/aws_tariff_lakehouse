from typing import List, Dict, Any, Generator
import os
import json
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from pathlib import Path

from src.utils.api_client import APIClient

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

headers = {
        "X-Api-Key": os.getenv("INDIAN_STOCK_API")
    }

indian_api_client = APIClient(
    base_url="https://fuel.indianapi.in",
    timeout=30,
    max_retries=3,
    headers=headers
)

def convert_to_jsonl(data: List[Dict[str, Any]]) -> str:
    if not data or len(data) == 0:
        return ""
    jsonl_data = "\n".join([json.dumps(record) for record in data]) + "\n" # Convert each dictionary to a JSON string and join with newlines
    return jsonl_data

def get_cities() -> List[Dict[str, str]]:
    response = indian_api_client.get("/cities")
    final = {}
    for record in response:
        final[record.get("name")] = record.get("value")
    return final

def get_states() -> List[Dict[str, str]]:
    response = indian_api_client.get("/states")
    final = {}
    for record in response:
        final[record.get("name")] = record.get("value")
    return final

def fetch_live_fuel_prices_india(
    fuel_type: str = "petrol", 
    location_type: str = "state"
) -> List[Dict[str, Any]]:
    print(f"Extracting live {fuel_type} prices for {location_type}")
    
    params = {
        "fuel_type": fuel_type,
        "location_type": location_type
    }
    
    raw_response = indian_api_client.get("/live_fuel_price", params=params)
    
    if not raw_response:
        print(f"No data returned for {fuel_type}/{location_type}")
        return []
    
    if isinstance(raw_response, list):
        return raw_response
    elif isinstance(raw_response, dict) and "data" in raw_response:
        return raw_response["data"]
    else:
        return [raw_response]

def upload_live_fuel_prices(
    data_records: List[Dict[str, Any]],
    fuel_type: str = "petrol",
    location_type: str = "state",
) -> str:
    if not data_records:
        print("No data records provided for upload")
        return None
    
    execution_date = datetime.now(timezone.utc)
    
    print(f"Uploading {len(data_records)} {fuel_type} records for {location_type}")
    
    date_str = execution_date.strftime("%Y-%m-%d")
    hour_str = execution_date.strftime("%H")
    timestamp_str = execution_date.strftime("%Y%m%d_%H%M%S")

    s3_key = f"bronze/source=indian_api/live_fuel/fuel_type={fuel_type}/ingestion_date={date_str}/hour_{hour_str}.jsonl"

    jsonl_content = "\n".join([json.dumps(record) for record in data_records])
    
    success = s3_manager.upload_json_content(
        json_content=jsonl_content,
        bucket="raw-data",
        key=s3_key
    )
    
    if success:
        print(f"Upload successful: s3://raw-data/{s3_key}")
        return s3_key
    else:
        print("Upload failed")
        return None

def fetch_historic_fuel_prices_india(
    fuel_type: str = "petrol",
    location_type: str = "state", 
    location: str = "Gujarat",
    output_size: int = 3
) -> List[Dict[str, Any]]:
    print(f"Extracting historical {fuel_type} prices for {location_type}")

    params = {
        "fuel_type": fuel_type,
        "location_type": location_type,
        "location": location,
        "n": output_size + 1
    }

    raw_response = indian_api_client.get("/historical_fuel_price", params=params)

    if not raw_response:
        print(f"No data returned for {fuel_type}/{location_type}")
        return []
    
    if isinstance(raw_response, list):
        return raw_response
    elif isinstance(raw_response, dict) and "data" in raw_response:
        return raw_response["data"]
    else:
        return [raw_response]

def upload_historical_fuel_prices(
    data_records: List[Dict[str, Any]],
    fuel_type: str = "petrol",
    location_type: str = "state",
    execution_date: datetime = None
) -> str:
    """
    Task 2: Pure upload - takes data and uploads to MinIO
    """
    if not data_records:
        print("No data records provided for upload")
        return None
    
    if execution_date is None:
        execution_date = datetime.now(timezone.utc)
    
    print(f"Uploading {len(data_records)} {fuel_type} records for {location_type}")
    
    date_str = execution_date.strftime("%Y-%m-%d")
    hour_str = execution_date.strftime("%H")
    timestamp_str = execution_date.strftime("%Y%m%d_%H%M%S")
    
    s3_key = f"bronze/source=indian_api/historic_fuel/fuel_type={fuel_type}/location_type={location_type}/ingested_{timestamp_str}.jsonl"
    
    jsonl_content = "\n".join([json.dumps(record) for record in data_records])
    
    success = s3_manager.upload_json_content(
        json_content=jsonl_content,
        bucket="raw-data",
        key=s3_key
    )
    
    if success:
        print(f"Upload successful: s3://raw-data/{s3_key}")
        return s3_key
    else:
        print("Upload failed")
        return None

if __name__ == "__main__":
    # cities = get_cities()
    # print(cities)

    # states = get_states()
    # print(states)

    # live_fuel_data = fetch_live_fuel_prices_india(fuel_type="petrol", location_type="city")
    # print(live_fuel_data)

    historic_fuel = fetch_historic_fuel_prices_india(
        fuel_type="petrol",
        location_type="state",
        location="Gujarat",
        output_size=3)
    print(historic_fuel)