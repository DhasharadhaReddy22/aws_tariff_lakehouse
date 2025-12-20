from datetime import datetime, timezone
from enum import Enum
from typing import List, Dict, Any

from src.utils.api_client import APIClient
from src.utils.bucket_client import bucket_client
from src.utils.config import config
from src.utils.logger import get_logger
from .ingestion_utils import build_raw_key, create_filename

logger = get_logger(__name__, caller_file_path=__file__)

DOMAIN = "energy"
SOURCE_NAME = "indian_api"
FUEL_BASE_URL = "https://fuel.indianapi.in"
FUEL_API_KEY = config.get("INDIAN_FUEL_API_KEY")
if not FUEL_API_KEY:
    raise RuntimeError("INDIAN_FUEL_API_KEY is not configured")

headers = {
    "X-Api-Key": FUEL_API_KEY
}

indian_fuel_client = APIClient(
    base_url=FUEL_BASE_URL,
    headers=headers,
    timeout=30,
    max_retries=3,
    backoff_base=2,
    backoff_cap=10
)

class FuelDatasetType(str, Enum):
    DIESEL_CITY_LIVE = "diesel_city_live"
    DIESEL_STATE_LIVE = "diesel_state_live"
    PETROL_CITY_LIVE = "petrol_city_live"
    PETROL_STATE_LIVE = "petrol_state_live"
    DIESEL_CITY_HISTORICAL = "diesel_city_historical"
    DIESEL_STATE_HISTORICAL = "diesel_state_historical"
    PETROL_CITY_HISTORICAL = "petrol_city_historical"
    PETROL_STATE_HISTORICAL = "petrol_state_historical"

def fetch_fuel_prices_raw(
    dataset: FuelDatasetType,
    location: str | None = None,
    output_size: int | None = None,
) -> List[Dict[str, Any]]:
    """
    Fetch fuel prices based on FuelDatasetType.
    Dataset types are in the format (all caps): {fuel_type}_{location_type}_{mode}
    example: DIESEL_CITY_LIVE
    where:
      fuel_type: petrol | diesel
      location_type: city | state
      mode: live | historical
    """

    parts = dataset.value.split("_")
    fuel_type = parts[0]          # petrol | diesel
    location_type = parts[1]      # city | state
    mode = parts[2]               # live | historical

    if mode == "historical":
        if not location:
            raise ValueError("location is required for historical datasets")
        if output_size is None:
            raise ValueError("output_size is required for historical datasets")

        endpoint = "/historical_fuel_price"
        params = {
            "fuel_type": fuel_type,
            "location_type": location_type,
            "location": location,
            "n": output_size + 1,
        }
    else:
        endpoint = "/live_fuel_price"
        params = {
            "fuel_type": fuel_type,
            "location_type": location_type,
        }

    logger.info(f"Fetching fuel prices | dataset={dataset.value}")
    resp = indian_fuel_client.get(endpoint, params=params)

    if not resp["ok"]:
        raise RuntimeError(f"Indian fuel API failed: {resp['error']}")

    data = resp["data"]
    records: List[Dict[str, Any]] = []

    for row in data:
        record = {
            # Common business fields
            "fuel_type": fuel_type,
            "location_type": location_type,
            "price": row.get("price"),
            "change": row.get("change"),

            # Location identity
            "location": row.get("city") or row.get("state") or row.get("name"),

            # Ingestion metadata
            "source": SOURCE_NAME,
            "dataset": (
                f"{dataset.value}_{location.lower()}"
                if mode == "historical"
                else dataset.value
            ),

            # Transport metadata
            "request_url": resp["url"],
            "received_at": resp["received_at"],
        }

        # Historical-only fields
        if mode == "historical":
            record["date"] = row.get("date")

        records.append(record)

    logger.info(f"Fetched {len(records)} records for dataset={dataset.value}")
    return records

def write_fuel_raw_to_s3(
    records: List[Dict[str, Any]],
    dataset: FuelDatasetType,
) -> None:
    if not records:
        logger.warning("No Indian Fuel records to write to s3")
        raise ValueError("No Indian Fuel records to write")

    ingested_at = datetime.now(timezone.utc).isoformat()
    for r in records:
        r["ingested_at"] = ingested_at

    key = build_raw_key(
        domain=DOMAIN,
        source=SOURCE_NAME,
        dataset=records[0]["dataset"],  # resolved dataset name
        ingestion_date=ingested_at[:10],
        filename=create_filename(records[0]["dataset"], ingested_at, ".jsonl"),
    )

    logger.info(f"Writing raw data to s3://{bucket_client.bucket_name}/{key}")
    bucket_client.put_jsonl(key, records)
    logger.info(f"Wrote {len(records)} records to s3://{bucket_client.bucket_name}/{key}")

def run_fuel_ingestion(
    dataset: FuelDatasetType,
    location: str | None = None,
    output_size: int | None = None,
) -> None:
    records = fetch_fuel_prices_raw(
        dataset=dataset,
        location=location,
        output_size=output_size,
    )
    write_fuel_raw_to_s3(records, dataset)


if __name__ == "__main__":
    run_fuel_ingestion(FuelDatasetType.PETROL_STATE_LIVE)

    run_fuel_ingestion(
        FuelDatasetType.DIESEL_CITY_HISTORICAL,
        location="Delhi",
        output_size=7,
    )