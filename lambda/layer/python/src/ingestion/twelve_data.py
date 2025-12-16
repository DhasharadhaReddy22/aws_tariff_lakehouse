from datetime import datetime, timezone
from typing import List, Dict, Any

from src.utils.api_client import APIClient
from src.utils.bucket_client import bucket_client
from src.utils.config import config
from src.utils.logger import get_logger
from .ingestion_utils import *

logger = get_logger(__name__, caller_file_path=__file__)

DOMAIN = "commodities"
SOURCE_NAME = "twelvedata"
DATASET = "exchange_rates"
BASE_URL = "https://api.twelvedata.com"
API_KEY = config.get("TWELVE_DATA_API_KEY")

twelve_data_api = APIClient(
    base_url=BASE_URL,
    timeout=30,
    max_retries=3,
    backoff_base=2,
    backoff_cap=10
)

def fetch_twelvedata_time_series_raw(
    symbols: List[str],
    params: Dict[str, Any],
    ingested_at: str,
) -> List[Dict[str, Any]]:
    """
    Fetch Twelve Data time-series data and return flattened raw records.
    """

    if not symbols:
        logger.warning("No symbols provided for Twelve Data ingestion")
        return []

    if not API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY is not configured")

    records: List[Dict[str, Any]] = []

    for symbol in symbols:
        symbol_params = {
            **params,
            "symbol": symbol,
            "apikey": API_KEY,
        }

        logger.info(f"Fetching Twelve Data time series for symbol={symbol}")

        resp = twelve_data_api.get("/time_series", params=symbol_params)

        if not resp["ok"]:
            logger.error(f"Twelve Data API call failed for symbol={symbol}: {resp['error']}")
            raise RuntimeError("Twelve Data ingestion failed")

        payload = resp["data"]
        meta = payload.get("meta", {})
        values = payload.get("values", [])

        for row in values:
            records.append({
                # Business data
                "symbol": meta.get("symbol"),
                "interval": meta.get("interval"),
                "currency_base": meta.get("currency_base"),
                "currency_quote": meta.get("currency_quote"),
                "type": meta.get("type"),
                "market_time": normalize_utc_datetime(row.get("datetime")),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),

                # Ingestion metadata
                "source": SOURCE_NAME,
                "dataset": DATASET,
                "ingested_at": ingested_at,

                # Transport metadata
                "received_at": resp["received_at"],
                "request_url": resp["url"],
            })

        logger.info(f"Fetched {len(values)} records for symbol={symbol}")

    logger.info(f"Fetched total {len(records)} Twelve Data records")
    return records


def write_twelvedata_raw_to_s3(
    records: List[Dict[str, Any]],
    ingested_at: str,
) -> None:
    """
    Write Twelve Data raw records to S3 using ingestion-date partitioning.
    """

    if not records:
        logger.warning("No Twelve Data records to write")
        return

    key = build_raw_key(
        domain=DOMAIN,
        source=SOURCE_NAME,
        dataset=DATASET,
        ingestion_date=ingested_at[:10],  # YYYY-MM-DD
        filename=create_filename("twelvedata_timeseries", ingested_at, ".jsonl")
    )

    bucket_client.put_jsonl(key, records)

    logger.info(f"Wrote Twelve Data raw data to s3://{bucket_client.bucket_name}/{key}")

def run_twelvedata_ingestion(
    symbols: List[str],
    params: Dict[str, Any],
) -> None:
    """
    Orchestrates a single Twelve Data ingestion run.
    """

    ingested_at = datetime.now(timezone.utc).isoformat()

    records = fetch_twelvedata_time_series_raw(
        symbols=symbols,
        params=params,
        ingested_at=ingested_at,
    )

    write_twelvedata_raw_to_s3(
        records=records,
        ingested_at=ingested_at,
    )

if __name__ == "__main__":
    symbols = ["XAU/USD", "USD/INR"]
    params = {
        "interval": "1h",
        "dp": 4,
        "timezone": "utc",
        "exchange": "NASDAQ",
        "format": "JSON",
        "outputsize": 12
    }

    commodities_data = run_twelvedata_ingestion(symbols, params)
    print(commodities_data)