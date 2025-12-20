from datetime import datetime, timezone
from typing import List, Dict, Any

from src.utils.api_client import APIClient
from src.utils.bucket_client import bucket_client
from src.utils.logger import get_logger
from .ingestion_utils import *

logger = get_logger(__name__, caller_file_path=__file__)

SOURCE_NAME = "imf"
DOMAIN = "macroeconomics"
DATASET = "indicators"
IMF_BASE_URL = "https://www.imf.org"

imf_client = APIClient(
    base_url=IMF_BASE_URL,
    timeout=30,
    max_retries=3
)

def fetch_imf_indicators_raw(
    indicator_codes: List[str],
    params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Fetch IMF indicators and return flattened raw records.
    """

    if not indicator_codes:
        logger.warning("No indicator codes provided")
        return []

    years = params.get("years")
    countries = params.get("countries")

    if not years or not countries:
        raise ValueError("Both 'years' and 'countries' must be provided")

    indicators_url = "/".join(indicator_codes)
    countries_url = "/".join(countries)
    years_url = ",".join(map(str, years))

    endpoint = (
        f"/external/datamapper/api/v1/"
        f"{indicators_url}/{countries_url}"
        f"?periods={years_url}"
    )
    
    logger.info(f"Fetching IMF indicators from endpoint: {endpoint}")
    resp = imf_client.get(endpoint)
    logger.info("API Response received from IMF")

    if not resp["ok"]:
        logger.error(f"IMF API call failed: {resp['error']}")
        raise RuntimeError("IMF ingestion failed")

    values = resp["data"].get("values", {})

    records: List[Dict[str, Dict[str, Dict[str, int]]]] = []

    for indicator, country_data in values.items():
        for country, year_data in country_data.items():
            for year, value in year_data.items():
                records.append({
                    # Business fields
                    "indicator": indicator,
                    "country": country,
                    "year": int(year),
                    "value": value,

                    # Ingestion metadata
                    "source": SOURCE_NAME,
                    "dataset": DATASET,

                    # Transport metadata (from api_client)
                    "request_url": resp["url"],
                    "received_at": resp["received_at"],
                })

    logger.info(f"Fetched {len(records)} IMF records for indicators={indicator_codes}, countries={countries}")
    return records

def write_imf_raw_to_s3(records: List[Dict[str, Any]]) -> None:
    """
    Write IMF raw records to S3 using ingestion-date partitioning.
    """

    if not records:
        logger.error("No IMF records to write to S3")
        raise ValueError("No IMF records to write")

    ingested_at = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["ingested_at"] = ingested_at

    key = build_raw_key(
        domain=DOMAIN,
        source=SOURCE_NAME,
        dataset=DATASET,
        ingestion_date=ingested_at[:10],  # YYYY-MM-DD
        filename=create_filename("imf_indicators", ingested_at, ".jsonl")
    )

    logger.info(f"Writing IMF raw data to s3://{bucket_client.bucket_name}/{key}")
    bucket_client.put_jsonl(key, records)
    logger.info(f"Wrote IMF raw data to s3://{bucket_client.bucket_name}/{key}")

def run_imf_ingestion(indicator_codes: List[str], params: Dict[str, Any]) -> None:
    """
    Orchestrates a single IMF ingestion run.
    """
    records = fetch_imf_indicators_raw(indicator_codes=indicator_codes, params=params)
    write_imf_raw_to_s3(records=records)

if __name__ == "__main__":
    params = {
        "years": [2021],
        "countries": ["IND", "USA"]
    }

    result = run_imf_ingestion(["NGDP_RPCH", "NGDPD"], params=params)
    print(result)