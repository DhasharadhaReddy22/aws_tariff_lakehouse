from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from enum import Enum
from dateutil.relativedelta import relativedelta

from src.utils.api_client import APIClient
from src.utils.bucket_client import bucket_client
from src.utils.config import config
from src.utils.logger import get_logger
from .ingestion_utils import build_raw_key, create_filename


logger = get_logger(__name__, caller_file_path=__file__)

DOMAIN = "financial_markets"
SOURCE_NAME = "financial_modeling_prep"
BASE_URL = "https://financialmodelingprep.com"
API_KEY = config.get("FMP_API_KEY")

fmp_client = APIClient(
    base_url=BASE_URL,
    timeout=30,
    max_retries=3,
    backoff_base=2,
    backoff_cap=10,
)

class DatasetType(str, Enum):
    MARKET_RISK_PREMIUM = "market_risk_premium"

    SECTOR_SNAPSHOT = "sector_snapshot"
    INDUSTRY_SNAPSHOT = "industry_snapshot"

    SECTOR_HISTORICAL = "sector_historical"
    INDUSTRY_HISTORICAL = "industry_historical"

def validate_date_within_last_30_days(date_str: str) -> None:
    """
    Raises ValueError if date_str (YYYY-MM-DD) is older than 30 days.
    """
    try:
        input_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        logger.error("date must be in YYYY-MM-DD format")
        raise ValueError("date must be in YYYY-MM-DD format")

    now = datetime.now(timezone.utc)
    if input_date < now - timedelta(days=30):
        logger.error(f"Provided date {date_str} is older than 30 days from today ({now.date()})")
        raise ValueError("date must be within the last 30 days")

def fetch_market_risk_premium_raw(countries: List[str] | None = None,) -> List[Dict[str, Any]]:
    """
        This metric represents the difference between the expected return of the stock market and the risk-free rate.
        It is used to assess the additional return that investors require for taking on the higher risk associated.
    """
    if not API_KEY:
        raise RuntimeError("FMP_API_KEY is not configured")

    resp = fmp_client.get("/stable/market-risk-premium", params={"apikey": API_KEY})

    if not resp["ok"]:
        raise RuntimeError(f"FMP market risk premium failed: {resp['error']}")

    records: List[Dict[str, Any]] = []

    for row in resp["data"]:
        if countries and row.get("country") not in countries:
            # skipping countries not in the provided list
            continue

        records.append({
            # Business data
            "country": row.get("country"),
            "continent": row.get("continent"),
            "country_risk_premium": float(row.get("countryRiskPremium", 0)),
            "total_equity_risk_premium": float(row.get("totalEquityRiskPremium", 0)),

            # Metadata
            "source": SOURCE_NAME,
            "dataset": DatasetType.MARKET_RISK_PREMIUM.value,

            # Transport metadata
            "request_url": resp["url"],
            "received_at": resp["received_at"],
        })

    return records

def fetch_performance_raw(
    level: str,                 # "sector" | "industry"
    mode: str,                  # "snapshot" | "historical"
    entities: List[str],        # list of sectors or industries
    params: Dict[str, Any],     # date or from/to params depending on mode, along with api_key
) -> List[Dict[str, Any]]:

    if level not in ("sector", "industry"):
        raise ValueError("level must be 'sector' or 'industry'")

    if mode not in ("snapshot", "historical"):
        raise ValueError("mode must be 'snapshot' or 'historical'")

    if not API_KEY:
        raise RuntimeError("FMP_API_KEY is not configured")

    if not entities:
        logger.warning(f"No {level}s provided")
        return []

    if mode=="snapshot" and "date" in params:
        validate_date_within_last_30_days(params["date"])
        
    params = {**params, "apikey": API_KEY}
    records: List[Dict[str, Any]] = []

    # Dataset resolution via enum
    if level == "sector" and mode == "snapshot":
        dataset = DatasetType.SECTOR_SNAPSHOT
    elif level == "sector" and mode == "historical":
        dataset = DatasetType.SECTOR_HISTORICAL
    elif level == "industry" and mode == "snapshot":
        dataset = DatasetType.INDUSTRY_SNAPSHOT
    else:
        dataset = DatasetType.INDUSTRY_HISTORICAL

    # Snapshot
    if mode == "snapshot":
        endpoint = f"/stable/{level}-performance-snapshot"

        resp = fmp_client.get(endpoint, params=params)

        if not resp["ok"]:
            raise RuntimeError(f"FMP {level} snapshot failed: {resp['error']}")

        for row in resp["data"]:
            if row.get(level) not in entities:
                continue

            records.append({
                "date": row.get("date"),
                level: row.get(level),
                "exchange": row.get("exchange"),
                "average_change": float(row.get("averageChange", 0)),

                "source": SOURCE_NAME,
                "dataset": dataset.value,

                "request_url": resp["url"],
                "received_at": resp["received_at"],
            })

    # Historical
    else:
        endpoint = f"/stable/historical-{level}-performance"

        for entity in entities:
            params[level] = entity

            resp = fmp_client.get(endpoint, params=params)

            if not resp["ok"]:
                raise RuntimeError(
                    f"FMP historical {level} failed for {entity}: {resp['error']}"
                )

            for row in resp["data"]:
                records.append({
                    "date": row.get("date"),
                    level: row.get(level),
                    "exchange": row.get("exchange"),
                    "average_change": float(row.get("averageChange", 0)),

                    "source": SOURCE_NAME,
                    "dataset": dataset.value,

                    "request_url": resp["url"],
                    "received_at": resp["received_at"],
                })

    return records

def write_fmp_raw_to_s3(records: List[Dict[str, Any]], dataset: DatasetType) -> None:

    if not records:
        logger.error("No FMP records to write")
        raise ValueError("No FMP records to write")

    ingested_at = datetime.now(timezone.utc).isoformat()

    for record in records:
        record["ingested_at"] = ingested_at

    key = build_raw_key(
        domain=DOMAIN,
        source=SOURCE_NAME,
        dataset=dataset.value,
        ingestion_date=ingested_at[:10],
        filename=create_filename(dataset.value, ingested_at, ".jsonl"),
    )

    logger.info(f"Writing FMP raw data to s3://{bucket_client.bucket_name}/{key}")
    bucket_client.put_jsonl(key, records)
    logger.info(f"Wrote FMP data to s3://{bucket_client.bucket_name}/{key}")

def run_fmp_ingestion(dataset: DatasetType, **kwargs) -> None:

    if dataset == DatasetType.MARKET_RISK_PREMIUM:
        records = fetch_market_risk_premium_raw(**kwargs)

    elif dataset in (
        DatasetType.SECTOR_SNAPSHOT,
        DatasetType.SECTOR_HISTORICAL,
    ):
        records = fetch_performance_raw(
            level="sector",
            mode="snapshot" if dataset == DatasetType.SECTOR_SNAPSHOT else "historical",
            **kwargs,
        )

    elif dataset in (
        DatasetType.INDUSTRY_SNAPSHOT,
        DatasetType.INDUSTRY_HISTORICAL,
    ):
        records = fetch_performance_raw(
            level="industry",
            mode="snapshot" if dataset == DatasetType.INDUSTRY_SNAPSHOT else "historical",
            **kwargs,
        )

    else:
        raise ValueError(f"Unsupported dataset: {dataset}")

    write_fmp_raw_to_s3(records, dataset)

if __name__ == "__main__":
    run_fmp_ingestion(
        dataset=DatasetType.MARKET_RISK_PREMIUM,
        countries=["United States", "India"],
    )

    run_fmp_ingestion(
        dataset=DatasetType.SECTOR_SNAPSHOT,
        entities=["Utilities", "Energy"],
        params={"date": "2025-12-01"},
    )

    run_fmp_ingestion(
        dataset=DatasetType.INDUSTRY_HISTORICAL,
        entities=["Consumer Electronics"],
        params={
            "from": "2025-07-01",
            "to": "2025-08-01",
            "exchange": "NASDAQ",
        },
    )
