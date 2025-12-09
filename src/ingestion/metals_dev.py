from typing import List, Dict, Any
import os
from requests.structures import CaseInsensitiveDict
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
import logging

from src.utils.api_client import APIClient

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(name)s | %(asctime)s] "
           "- [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"

metals_dev_client = APIClient(
    base_url="https://api.metals.dev",
    headers=headers,
    timeout=30,
    max_retries=3
)

def fetch_metals_latest(params: Dict[str, Any] = {}) -> List[Dict[str, Any]]:
    """
    Fetch latest spot prices for metals and currencies from Metals API,
    flatten into list of JSON records with metadata.

    Args:
        params (Dict[str, Any]): Additional query params

    Returns:
        List[Dict[str, Any]]: Flattened list of JSON records
    """
    api_key = os.getenv("METALS_DEV_API_KEY")
    if not api_key:
        logger.error("METALS_DEV_API_KEY is not set!")
        return []

    request_params = params.copy()
    request_params["api_key"] = api_key
    fetched_time = datetime.now(timezone.utc).isoformat()

    logger.info("Fetching RAW data from Metals.dev")

    raw_response, sanitized_url = metals_dev_client.get("/v1/latest", params=request_params)
    if not raw_response or raw_response.get("status") != "success":
        logger.warning("No data returned for metals API, skipping.")
        return []

    all_records: List[Dict[str, Any]] = []

    currency = raw_response.get("currency")
    unit = raw_response.get("unit")
    ts_metal = raw_response.get("timestamps", {}).get("metal")

    # ---- Metals ----
    for metal, rate in raw_response.get("metals", {}).items():
        all_records.append({
            "currency": currency,
            "unit": unit,
            "metal": metal,
            "rate": float(rate),
            "_fetched_at": fetched_time,
            "_source": __name__,
            "_sanitized_url": sanitized_url
        })

    logger.info(f"Fetched {len(all_records)} records from Metals.dev")
    return all_records

if __name__ == "__main__":  
    params = {
        "currency": "USD",
        "unit": "g"
    }

    data = fetch_metals_latest(params=params)
    print(data)