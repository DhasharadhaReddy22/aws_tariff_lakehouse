from datetime import datetime, timezone
from typing import Dict, Any, List

from requests.structures import CaseInsensitiveDict

from src.utils.api_client import APIClient
from src.utils.bucket_client import bucket_client
from src.utils.config import config
from src.utils.logger import get_logger
from .ingestion_utils import build_raw_key, create_filename


logger = get_logger(__name__, caller_file_path=__file__)

DOMAIN = "commodities"
SOURCE_NAME = "metals_dev"
DATASET = "spot_prices"
BASE_URL = "https://api.metals.dev"
API_KEY = config.get("METALS_DEV_API_KEY")

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"

metals_client = APIClient(
    base_url=BASE_URL,
    headers=headers,
    timeout=30,
    max_retries=3,
    backoff_base=2,
    backoff_cap=10,
)

def fetch_metals_latest_raw(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch latest spot prices from Metals.dev and return flattened raw records.
    """

    if not API_KEY:
        raise RuntimeError("METALS_DEV_API_KEY is not configured")

    request_params = {
        **params,
        "api_key": API_KEY,
    }

    logger.info("Fetching Metals.dev latest spot prices")

    resp = metals_client.get("/v1/latest", params=request_params)

    if not resp["ok"]:
        raise RuntimeError(f"Metals.dev API failed: {resp['error']}")

    payload = resp["data"]

    if payload.get("status") != "success":
        raise RuntimeError(
            f"Metals.dev API returned error status: {payload.get('status')}"
        )

    currency = payload.get("currency")
    unit = payload.get("unit")
    metals = payload.get("metals", {})
    metal_timestamp = payload.get("timestamps", {}).get("metal")

    records: List[Dict[str, Any]] = []

    for metal, rate in metals.items():
        records.append({
            # Business data
            "metal": metal,
            "rate": float(rate),
            "currency": currency,
            "unit": unit,
            "market_time": metal_timestamp,  # already UTC from API

            # Ingestion metadata
            "source": SOURCE_NAME,
            "dataset": DATASET,

            # Transport metadata
            "request_url": resp["url"],
            "received_at": resp["received_at"],
        })

    logger.info(f"Fetched {len(records)} metal spot price records")
    return records


def write_metals_raw_to_s3(records: List[Dict[str, Any]]) -> None:
    """
    Write Metals.dev raw records to S3 using ingestion-date partitioning.
    """

    if not records:
        logger.warning("No Metals.dev records to write")
        return
    
    ingested_at = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["ingested_at"] = ingested_at

    key = build_raw_key(
        domain=DOMAIN,
        source=SOURCE_NAME,
        dataset=DATASET,
        ingestion_date=ingested_at[:10],  # YYYY-MM-DD
        filename=create_filename("metals_spot_prices", ingested_at, ".jsonl"),
    )

    logger.info(f"Writing Metals.dev raw data to s3://{bucket_client.bucket_name}/{key}")
    bucket_client.put_jsonl(key, records)
    logger.info(f"Wrote Metals.dev raw data to s3://{bucket_client.bucket_name}/{key}")


def run_metals_ingestion(params: Dict[str, Any]) -> None:
    """
    Orchestrates a single Metals.dev ingestion run.
    """

    records = fetch_metals_latest_raw(params=params)
    write_metals_raw_to_s3(records=records)


if __name__ == "__main__":
    params = {
        "currency": "USD",
        "unit": "toz",
    }

    run_metals_ingestion(params)