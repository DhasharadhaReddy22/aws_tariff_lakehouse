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
    max_retries=3,
    backoff_base=2,
    backoff_cap=10,
    request_interval=1.1
)

def fetch_imf_indicators_raw(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch IMF indicators and return flattened raw records.
    """
    years = params.get("years", [])
    countries = params.get("countries", [])
    indicator_codes = params.get("indicator_codes")

    if not indicator_codes:
        raise ValueError("Both 'indicator_codes' and 'countries' must be provided")

    indicators_url = "/".join(indicator_codes)
    countries_url = "/".join(countries)
    years_url = ",".join(map(str, years))

    if years and countries:
        endpoint = (
            f"/external/datamapper/api/v1/"
            f"{indicators_url}/{countries_url}"
            f"?periods={years_url}"
        )
    elif not years and countries:
        endpoint = (
            f"/external/datamapper/api/v1/"
            f"{indicators_url}/{countries_url}"
        )
    elif years and not countries:
        endpoint = (
            f"/external/datamapper/api/v1/"
            f"{indicators_url}"
            f"?periods={years_url}"
        )
    else:
        endpoint = (
            f"/external/datamapper/api/v1/"
            f"{indicators_url}"
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

def write_imf_raw_to_s3(records: List[Dict[str, Any]]) -> Dict[str, Any]:
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
    return {"keys": [key], "record_count": len(records), "ingested_at": ingested_at}

def run_imf_ingestion(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Orchestrates a single IMF ingestion run.
    """
    records = fetch_imf_indicators_raw(params=params)
    write_result = write_imf_raw_to_s3(records=records)
    return {"domain": DOMAIN, "source": SOURCE_NAME, "dataset": DATASET, **write_result}

if __name__ == "__main__":
    params = {
        # "years": [2021],
        "indicator_codes": ["NGDP_RPCH", "NGDPD"],
        "countries": ["IND", "USA"]
    }

    result = run_imf_ingestion(params=params)
    print(result)