from datetime import datetime, timezone
from typing import List, Dict, Any
from enum import Enum

from src.utils.api_client import APIClient
from src.utils.bucket_client import bucket_client
from src.utils.config import config
from src.utils.logger import get_logger
from .ingestion_utils import build_raw_key, create_filename

logger = get_logger(__name__, caller_file_path=__file__)

DOMAIN = "indian_markets"
SOURCE_NAME = "indian_stock_api"
BASE_URL = "https://stock.indianapi.in"

API_KEY = config.get("INDIAN_STOCK_API_KEY")
if not API_KEY:
    raise RuntimeError("INDIAN_STOCK_API_KEY is not configured")

headers = {
    "X-Api-Key": API_KEY
}

class StockDatasetType(str, Enum):
    STOCK_METADATA = "stock"
    HISTORICAL_DATA = "historical_data"
    HISTORICAL_STATS = "historical_stats"
    TRENDING = "trending"
    NSE_MOST_ACTIVE = "nse_most_active"
    PRICE_SHOCKERS = "price_shockers"
    INDUSTRY_SEARCH = "industry_search"
    FETCH_52_WEEK_HLD = "52_week_high_low_data"

stocks_client = APIClient(
    base_url=BASE_URL,
    headers=headers,
    timeout=30,
    max_retries=3,
    backoff_base=2,
    backoff_cap=10,
)

def fetch_stock_metadata_raw(stock_names: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch stock metadata for Indian equities.
    This is a low-cadence / occasional ingestion.
    """

    if not stock_names:
        logger.warning("No stock stock_names provided")
        return []

    records: List[Dict[str, Any]] = []

    for stock_name in stock_names:
        params = {"name": stock_name}

        logger.info(f"Fetching stock metadata | name={stock_name}")
        resp = stocks_client.get("/stock", params=params)

        if not resp["ok"]:
            logger.error(f"Stock metadata API failed | name={stock_name} | {resp['error']}")
            raise RuntimeError("Indian stocks ingestion failed")

        records.append({
            "data": resp["data"],

            # Ingestion metadata
            "source": SOURCE_NAME,
            "dataset": StockDatasetType.STOCK_METADATA.value,

            # Transport metadata
            "request_url": resp["url"],
            "received_at": resp["received_at"],
        })

    logger.info(f"Fetched metadata for {len(records)} stocks")
    return records

def fetch_historical_prices_raw(stock_names: List[str], period: str, filter: str) -> List[Dict[str, Any]]:
    """
    Fetch historical price-related data for a given stock from Indian Stock API.

    Period values: 1m, 6m, 1yr, 3yr, 5yr, 10yr, max
    Filter values: default, price, pe, sm, evebitda, ptb, mcs
    """
    if not stock_names:
        logger.warning("No stock stock_names provided")
        raise ValueError("No stock stock_names provided")
    
    if period not in {"1m", "6m", "1yr", "3yr", "5yr", "10yr", "max"}:
        logger.error(f"Invalid period value: {period}")
        raise ValueError(f"Invalid period value: {period}")
    
    if filter not in {"default", "price", "pe", "sm", "evebitda", "ptb", "mcs"}:
        logger.error(f"Invalid filter value: {filter}")
        raise ValueError(f"Invalid filter value: {filter}")
    
    records: List[Dict[str, Any]] = []
    for stock_name in stock_names:
        params = {
            "stock_name": stock_name,
            "period": period,
            "filter": filter,
        }

        logger.info(f"Fetching historical data | stock_name={stock_name}, period={period}, filter={filter}")
        resp = stocks_client.get("/historical_data", params=params)

        if not resp["ok"]:
            logger.error(f"Historical data API failed | stock_name={stock_name} | {resp['error']}")
            raise RuntimeError("Indian stocks historical ingestion failed")

        # ONE raw record per stock per request
        records.append({
            # Business payload
            "stock_name": stock_name,
            "period": period,
            "filter": filter,
            "data": resp["data"],

            # Ingestion metadata
            "source": SOURCE_NAME,
            "dataset": StockDatasetType.HISTORICAL_DATA.value,

            # Transport metadata
            "request_url": resp["url"],
            "received_at": resp["received_at"],
        })

        logger.info(f"Fetched historical data for stock_name={stock_name} (datasets={len(resp['data'].get('datasets', []))})")
    
    logger.info(f"Fetched historical data for {len(records)} stocks")
    return records

def fetch_historical_stats_raw(stock_names: List[str], stats_list: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch historical financial statistics for Indian stocks.

    stats values: quarter_results, yoy_results, balancesheet, cashflow, ratios, shareholding_pattern_quarterly, shareholding_pattern_yearly
    """
    if not stock_names:
        logger.warning("No stock stock_names provided")
        raise ValueError("No stock stock_names provided")

    if not stats_list or not all(stats in {"quarter_results", "yoy_results", "balancesheet", "cashflow", "ratios", "shareholding_pattern_quarterly", "shareholding_pattern_yearly"} for stats in stats_list):
        logger.error(f"Invalid stats type: {stats_list}")
        raise ValueError(f"Invalid stats type: {stats_list}")

    records: List[Dict[str, Any]] = []

    for stock_name in stock_names:
        for stats in stats_list:
            params = {
                "stock_name": stock_name,
                "stats": stats,
            }

            logger.info(f"Fetching historical stats | stock={stock_name}, stats={stats}")
            resp = stocks_client.get("/historical_stats", params=params)

            if not resp["ok"]:
                logger.error(f"Historical stats API failed | stock={stock_name} | stats={stats} | {resp['error']}")
                raise RuntimeError("Indian stocks historical stats ingestion failed")

            records.append({
                # Business payload
                "stock_name": stock_name,
                "stats_type": stats,
                "data": resp["data"],

                # Ingestion metadata
                "source": SOURCE_NAME,
                "dataset": StockDatasetType.HISTORICAL_STATS.value,

                # Transport metadata
                "request_url": resp["url"],
                "received_at": resp["received_at"],
            })

        logger.info(f"Fetched stats={stats} for stock={stock_name}")

    logger.info(f"Fetched {len(stats_list)} historical stats for {len(stock_names)} stocks")
    return records

def fetch_trending_stocks_raw() -> List[Dict[str, Any]]:
    """
    Fetch trending stocks snapshot (top 3 gainers & losers).
    """

    logger.info("Fetching trending stocks snapshot")
    resp = stocks_client.get("/trending")

    if not resp["ok"]:
        logger.error(f"Trending stocks API failed | {resp['error']}")
        raise RuntimeError("Indian stocks trending ingestion failed")

    records = [{
        # Business payload (raw snapshot)
        "data": resp["data"],

        # Ingestion metadata
        "source": SOURCE_NAME,
        "dataset": StockDatasetType.TRENDING.value,

        # Transport metadata
        "request_url": resp["url"],
        "received_at": resp["received_at"],
    }]

    logger.info("Fetched trending stocks snapshot")
    return records

def fetch_price_shockers_raw() -> List[Dict[str, Any]]:
    """
    Fetch price shockers snapshot, covering BSE and NSE exchanges.
    """

    logger.info("Fetching price shockers snapshot")
    resp = stocks_client.get("/price_shockers")

    if not resp["ok"]:
        logger.error(f"Price shockers API failed | {resp['error']}")
        raise RuntimeError("Indian stocks price shockers ingestion failed")

    records = [{
        # Business payload (raw snapshot list)
        "data": resp["data"],

        # Ingestion metadata
        "source": SOURCE_NAME,
        "dataset": StockDatasetType.PRICE_SHOCKERS.value,

        # Transport metadata
        "request_url": resp["url"],
        "received_at": resp["received_at"],
    }]

    logger.info(f"Fetched price shockers snapshot (count={len(resp['data'])})")
    return records

def fetch_nse_most_active_raw() -> List[Dict[str, Any]]:
    """
    Fetch NSE most active stocks snapshot.
    """

    logger.info("Fetching NSE most active stocks snapshot")
    resp = stocks_client.get("/NSE_most_active")

    if not resp["ok"]:
        logger.error(f"NSE most active API failed | {resp['error']}")
        raise RuntimeError("Fetching Indian stocks NSE most active failed")

    records = [{
        # Business payload (raw snapshot list)
        "data": resp["data"],

        # Ingestion metadata
        "source": SOURCE_NAME,
        "dataset": StockDatasetType.NSE_MOST_ACTIVE.value,

        # Transport metadata
        "request_url": resp["url"],
        "received_at": resp["received_at"],
    }]

    logger.info(f"Fetched NSE most active snapshot (count={len(resp['data'])})")
    return records

def write_indian_stocks_raw_to_s3(dataset: StockDatasetType,  records: List[Dict[str, Any]], entity_key_fn=None) -> None:
    """
    Generic raw writer for Indian stocks endpoints.
    Entity key function is passed in use cases where data needs to be fanned-out to unique files per entity.
    """

    if not records:
        logger.warning("No Indian Stocks records to write to s3")
        raise ValueError("No Indian Stocks records to write to s3")

    ingested_at = datetime.now(timezone.utc).isoformat()
    for r in records:
        r["ingested_at"] = ingested_at

    if entity_key_fn is None: # No fanning out, write all records to a single file
        key = build_raw_key(
            domain=DOMAIN,
            source=SOURCE_NAME,
            dataset=dataset.value,
            ingestion_date=ingested_at[:10],
            filename=create_filename(dataset.value, ingested_at, ".jsonl"),
        )
        logger.info(f"Writing Indian stock raw data → s3://{bucket_client.bucket_name}/{key}")
        bucket_client.put_jsonl(key, records)
        logger.info(f"Wrote {len(records)} records to s3://{bucket_client.bucket_name}/{key}")
        return

    # Entity-aware fanout
    grouped = {}
    for r in records:
        entity = entity_key_fn(r)
        if not entity:
            raise ValueError("Entity key resolved to None")
        grouped.setdefault(entity, []).append(r)

    for entity, entity_records in grouped.items():
        key = build_raw_key(
            domain=DOMAIN,
            source=SOURCE_NAME,
            dataset=dataset.value,
            ingestion_date=ingested_at[:10],
            filename=create_filename(f"{dataset.value}_{entity}", ingested_at, ".jsonl"),
        )
        logger.info(f"Writing Indian stock raw data for entity '{entity}' → s3://{bucket_client.bucket_name}/{key}")
        bucket_client.put_jsonl(key, entity_records)
        logger.info(f"Wrote {len(entity_records)} records to s3://{bucket_client.bucket_name}/{key}")

def run_stock_metadata_ingestion(stock_names: List[str],) -> None:
    """
    Orchestrates stock metadata ingestion.
    """

    records = fetch_stock_metadata_raw(stock_names=stock_names)
    write_indian_stocks_raw_to_s3(
        dataset=StockDatasetType.STOCK_METADATA, 
        records=records, 
        entity_key_fn=lambda r: r["data"]["companyProfile"]["exchangeCodeNse"]
    )

def run_historical_prices_ingestion(stock_names: List[str], period: str = "5yr", filter: str = "price") -> None:
    """
    Orchestrates historical prices ingestion for a given stock.
    """
    records = fetch_historical_prices_raw(stock_names=stock_names, period=period, filter=filter)
    write_indian_stocks_raw_to_s3(
        dataset=StockDatasetType.HISTORICAL_DATA, 
        records=records,
        entity_key_fn=lambda r: r["stock_name"]
    )

def run_historical_stats_ingestion(stock_names: List[str], stats_list: List[str]) -> None:
    """
    Orchestrates historical stats ingestion for given stocks.
    """
    records = fetch_historical_stats_raw(stock_names=stock_names, stats_list=stats_list)
    write_indian_stocks_raw_to_s3(
        dataset=StockDatasetType.HISTORICAL_STATS, 
        records=records,
        entity_key_fn=lambda r: r["stock_name"]
    )

def run_trending_ingestion() -> None:
    """
    Orchestrates a single trending snapshot ingestion.
    Intended to be run frequently (e.g., hourly).
    """
    records = fetch_trending_stocks_raw()
    write_indian_stocks_raw_to_s3(dataset=StockDatasetType.TRENDING, records=records)

def run_price_shockers_ingestion() -> None:
    """
    Orchestrates a single price shockers snapshot ingestion.
    Intended to be run frequently (e.g., daily).
    """
    records = fetch_price_shockers_raw()
    write_indian_stocks_raw_to_s3(dataset=StockDatasetType.PRICE_SHOCKERS, records=records)

def run_nse_most_active_ingestion() -> None:
    """
    Orchestrates a single NSE most active snapshot ingestion.
    Intended to be run frequently (e.g., daily).
    """
    records = fetch_nse_most_active_raw()
    write_indian_stocks_raw_to_s3(dataset=StockDatasetType.NSE_MOST_ACTIVE, records=records)

if __name__ == "__main__":
    stocks = ["TITAN", "BHARATFORG"]

    # run_stock_metadata_ingestion(stock_names=stock)
    # run_historical_prices_ingestion(stock_names=stock, period="5yr", filter="price")
    # run_historical_stats_ingestion(stock_names=stocks, stats_list=["quarter_results", "yoy_results"])
    # run_trending_ingestion()
    # run_price_shockers_ingestion()
    run_nse_most_active_ingestion()

    