from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from enum import Enum

from src.utils.api_client import APIClient
from src.utils.config import config
from src.utils.bucket_client import bucket_client
from src.utils.logger import get_logger
from src.ingestion.ingestion_utils import build_raw_key, create_filename

logger = get_logger(__name__, caller_file_path=__file__)

SOURCE_NAME = "alphavantage"
BASE_URL = "https://www.alphavantage.co"
API_KEY = config.get("ALPHA_VANTAGE_API_KEY")
if not API_KEY:
    raise RuntimeError("ALPHA_VANTAGE_API_KEY is not configured")

alpha_client = APIClient(
    base_url=BASE_URL,
    timeout=30,
    max_retries=3,
    backoff_base=2,
    backoff_cap=10,
)

class DatasetType(str, Enum):
    TIME_SERIES_DAILY = "time_series_daily"
    COMMODITY = "commodity_prices"

class DomainType(str, Enum):
    MARKET = "american_markets"
    COMMODITIES = "commodities"

def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

def fetch_daily_stock_prices_raw(symbols: List[str], outputsize: str = "compact") -> List[Dict[str, Any]]:
    """
    Fetch daily OHLCV stock prices from Alpha Vantage.
    Flattened to one record per symbol per trading day.
    """

    if not symbols:
        logger.error("No symbols provided for DAILY stock prices ingestion")
        raise ValueError("Symbols list is empty")
    
    records: List[Dict[str, Any]] = []

    for symbol in symbols:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": outputsize,
            "datatype": "json",
            "apikey": API_KEY,
        }

        logger.info(f"Fetching DAILY stock prices | symbol={symbol}")
        resp = alpha_client.get("/query", params=params)

        if not resp["ok"]:
            logger.error(f"Alpha Vantage DAILY failed | symbol={symbol} | {resp['error']}")
            raise RuntimeError("Alpha Vantage stock ingestion failed")

        payload = resp["data"]
        # Identify the time series key dynamically to access the data
        ts_key = next((k for k in payload.keys() if "Time Series" in k), None)
        if not ts_key:
            logger.warning(f"No time series found | symbol={symbol}")
            continue

        for date, metrics in payload[ts_key].items():
            records.append({
                # Business fields
                "symbol": symbol,
                "trade_date": date,
                "open": _safe_float(metrics["1. open"]),
                "high": _safe_float(metrics["2. high"]),
                "low": _safe_float(metrics["3. low"]),
                "close": _safe_float(metrics["4. close"]),
                "volume": int(metrics["5. volume"]),

                # Ingestion metadata
                "source": "alphavantage",
                "dataset": DatasetType.TIME_SERIES_DAILY.value,

                # Transport metadata
                "request_url": resp["url"],
                "received_at": resp["received_at"],
            })

        logger.info(f"Fetched {len(payload[ts_key])} daily records for {symbol}")

    logger.info(f"Fetched total {len(records)} DAILY stock records")
    return records

def fetch_commodity_prices_raw(functions: List[str], interval: str = "daily") -> List[Dict[str, Any]]:
    """
    Fetch commodity price time series from Alpha Vantage (WTI, NATURAL_GAS).
    Flattened to one record per date.
    """

    if interval not in {"daily", "weekly", "monthly"}:
        logger.error(f"Invalid interval specified: {interval}")
        raise ValueError(f"Invalid interval: {interval}")

    records: List[Dict[str, Any]] = []
    for function in functions:
        params = {
            "function": function,
            "interval": interval,
            "datatype": "json",
            "apikey": API_KEY,
        }

        logger.info(f"Fetching commodity prices | function={function}, interval={interval}")
        resp = alpha_client.get("/query", params=params)

        if not resp["ok"]:
            logger.error(f"Commodity API failed | {resp['error']}")
            raise RuntimeError("Alpha Vantage commodity ingestion failed")

        payload = resp["data"]
        
        data = payload.get("data", [])
        for row in data:
            records.append({
                # Business fields
                "commodity": payload.get("name"),
                "symbol": function,
                "interval": payload.get("interval"),
                "date": row["date"],
                "value": _safe_float(row["value"]),
                "unit": payload.get("unit"),

                # Ingestion metadata
                "source": "alphavantage",
                "dataset": DatasetType.COMMODITY.value,

                # Transport metadata
                "request_url": resp["url"],
                "received_at": resp["received_at"],
            })

        logger.info(f"Fetched {len(records)} commodity records | {function}")
    
    logger.info(f"Fetched total {len(records)} commodity records")
    return records

def write_alphavantage_raw_to_s3(domain:DomainType, dataset: DatasetType,  records: List[Dict[str, Any]], entity_key_fn=None) -> Dict[str, Any]:
    """
    Generic raw writer for Alphavantage endpoints.
    Entity key function is passed in use cases where data needs to be fanned-out to unique files per entity.
    """

    if not records:
        logger.warning(f"No {domain.value} records to write to s3")
        raise ValueError(f"No {domain.value} records to write to s3")

    ingested_at = datetime.now(timezone.utc).isoformat()
    for r in records:
        r["ingested_at"] = ingested_at

    keys = []
    total_records = 0
    if entity_key_fn is None: # No fanning out, write all records to a single file
        key = build_raw_key(
            domain=domain.value,
            source=SOURCE_NAME,
            dataset=dataset.value,
            ingestion_date=ingested_at[:10],
            filename=create_filename(dataset.value, ingested_at, ".jsonl"),
        )
        keys.append(key)
        logger.info(f"Writing Alphavantage {domain.value} raw data → s3://{bucket_client.bucket_name}/{key}")
        bucket_client.put_jsonl(key, records)
        logger.info(f"Wrote {len(records)} records to s3://{bucket_client.bucket_name}/{key}")
        total_records += len(records)
        return {"keys": keys, "record_count": total_records, "ingested_at": ingested_at}

    # Entity-aware fanout
    grouped = {}
    for r in records:
        entity = entity_key_fn(r)
        if not entity:
            raise ValueError("Entity key resolved to None")
        grouped.setdefault(entity, []).append(r)

    for entity, entity_records in grouped.items():
        key = build_raw_key(
            domain=domain.value,
            source=SOURCE_NAME,
            dataset=dataset.value,
            ingestion_date=ingested_at[:10],
            filename=create_filename(f"{dataset.value}_{entity}", ingested_at, ".jsonl"),
        )
        keys.append(key)
        logger.info(f"Writing Alphavantage {domain.value} raw data for entity '{entity}' → s3://{bucket_client.bucket_name}/{key}")
        bucket_client.put_jsonl(key, entity_records)
        logger.info(f"Wrote {len(entity_records)} records to s3://{bucket_client.bucket_name}/{key}")
        total_records += len(entity_records)
    return {"keys": keys, "record_count": total_records, "ingested_at": ingested_at}

def run_daily_stock_prices_ingestion(symbols: List[str], outputsize: str = "compact") -> Dict[str, Any]:
    """
    Orchestrates a single daily stock prices ingestion run.
    """
    records = fetch_daily_stock_prices_raw(symbols=symbols, outputsize=outputsize)
    write_result = write_alphavantage_raw_to_s3(
        domain=DomainType.MARKET,
        dataset=DatasetType.TIME_SERIES_DAILY,
        records=records,
        entity_key_fn=lambda r: r["symbol"],
    )
    return {"domain": DomainType.MARKET.value, "source": SOURCE_NAME, "dataset": DatasetType.TIME_SERIES_DAILY.value, **write_result}

def run_commodity_prices_ingestion(functions: List[str], interval: str = "daily") -> Dict[str, Any]:
    """
    Orchestrates a single commodity prices ingestion run.
    """
    records = fetch_commodity_prices_raw(functions=functions, interval=interval)
    write_result = write_alphavantage_raw_to_s3(
        domain=DomainType.COMMODITIES,
        dataset=DatasetType.COMMODITY,
        records=records,
        entity_key_fn=lambda r: r["symbol"],
    )
    return {"domain": DomainType.COMMODITIES.value, "source": SOURCE_NAME, "dataset": DatasetType.COMMODITY.value, **write_result}

if __name__ == "__main__":
    
    stocks = ["AAPL", "MSFT"]
    run_daily_stock_prices_ingestion(symbols=stocks, outputsize="compact")

    commodities = ["WTI", "NATURAL_GAS"]
    run_commodity_prices_ingestion(functions=commodities, interval="daily")