import os
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

from src.utils.api_client import APIClient

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(name)s | %(asctime)s] "
           "- [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

twelve_data_api = APIClient(
    base_url="https://api.twelvedata.com",
    timeout=30,
    max_retries=3
)


def fetch_twelvedata_time_series(symbols: List[str], params: Dict[str, Any] = {}) -> List[Dict[str, Any]]:
    """
    Fetch time series data from Twelve Data API and flatten into list of JSON records
    with both API metadata and ingestion metadata included.

    Args:
        symbols (List[str]): List of symbols like ['XAU/USD', 'XAG/USD', 'USD/INR']
        params (Dict[str, Any]): Additional parameters for the API request

    Returns:
        List[Dict[str, Any]]: Flattened list of JSON records
    """
    api_key = os.getenv("TWELVE_DATA_API_KEY")
    if not api_key:
        logger.error("TWELVE_DATA_API_KEY is not set!")
        return []

    fetched_time = datetime.now(timezone.utc).isoformat()
    all_records = []

    for symbol in symbols:
        symbol_params = params.copy()
        symbol_params["symbol"] = symbol
        symbol_params["apikey"] = api_key

        logger.info(f"Fetching RAW data for symbol: {symbol}")

        raw_response, sanitized_url = twelve_data_api.get("/time_series", params=symbol_params)

        # sanitized_url = APIClient.sanitize_url(
        #     f"{twelve_data_api.base_url}/time_series",
        #     params=symbol_params
        # )

        if not raw_response or "values" not in raw_response:
            logger.warning(f"No valid data returned for {symbol}, skipping.")
            continue

        api_meta = raw_response.get("meta", {})

        # Flatten values into individual records
        for row in raw_response["values"]:
            all_records.append({
                "symbol": api_meta.get("symbol"),
                "frequency": api_meta.get("interval"),
                "currency_base": api_meta.get("currency_base"),
                "currency_quote": api_meta.get("currency_quote"),
                "instrument_type": api_meta.get("type"),
                "datetime": row.get("datetime"),
                "open_value": float(row.get("open")),
                "high_value": float(row.get("high")),
                "low_value": float(row.get("low")),
                "close_value": float(row.get("close")),
                "_fetched_at": fetched_time,
                "_source": __name__,
                "_sanitized_url": sanitized_url
            })

        logger.info(f"Fetched {len(raw_response['values'])} records for {symbol}")

    return all_records


if __name__ == "__main__":
    symbols = ["XAU/USD", "XAG/USD", "USD/INR"]
    params = {
        "interval": "1h",
        "dp": 4,
        "timezone": "utc",
        "exchange": "NASDAQ",
        "format": "JSON",
        "outputsize": 12
    }

    commodities_data = fetch_twelvedata_time_series(symbols, params)
    print(commodities_data)