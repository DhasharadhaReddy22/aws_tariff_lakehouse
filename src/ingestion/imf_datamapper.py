import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

from src.utils.api_client import APIClient

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(name)s | %(asctime)s] - [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

imf_client = APIClient(
    base_url="https://www.imf.org",
    timeout=30,
    max_retries=3
)


def get_imf_indicators_label_list(indicators_list: List[str] = []) -> Dict[str, str]:
    """Fetch IMF indicator labels (symbols -> human-readable labels)."""
    all_indicators = imf_client.get("/external/datamapper/api/v1/indicators")
    indicators_symbols = all_indicators.get("indicators").keys()
    indicators_labels = {}
    for symbol in indicators_symbols:
        if not indicators_list or symbol in indicators_list:
            indicators_labels[symbol] = all_indicators["indicators"][symbol]["label"]
    return indicators_labels


def fetch_imf_indicators_raw(indicator_codes: List[str], params: Dict[str, Any] = {}) -> List[Dict[str, Any]]:
    """
    Fetch IMF indicators and return flattened records.
    Each record contains both the API response value and metadata.
    """
    if not indicator_codes:
        return []

    fetched_time = datetime.now(timezone.utc).isoformat()
    years_url = ",".join(map(str, params.get("years")))
    countries_url = "/".join(params.get("countries"))
    indicators_url = "/".join(indicator_codes)
    url = f"/external/datamapper/api/v1/{indicators_url}/{countries_url}?periods={years_url}"
    data, sanitized_url = imf_client.get(url)

    # Flatten IMF API response into long-format records
    data_records = []
    values = data.get("values", {})
    for indicator, country_data in values.items():
        for country, year_data in country_data.items():
            for year, value in year_data.items():
                data_records.append({
                    "indicator": indicator,
                    "country": country,
                    "year": int(year),
                    "value": value,
                    "_source": "International Monetary Fund",
                    "_ingestion_time": fetched_time,
                    "_request_url": sanitized_url
                })

    logger.info(f"Fetched {len(data_records)} records.")
    return data_records


if __name__ == "__main__":
    params = {
        "years": [2022, 2023, 2024],
        "countries": ["IND", "USA"]
    }

    result = fetch_imf_indicators_raw(["NGDP_RPCH", "NGDPD"], params=params)
    print(result)