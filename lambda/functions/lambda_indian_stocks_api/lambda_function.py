from typing import Dict, Any
from datetime import datetime, timezone

from src.utils.logger import get_logger
from src.ingestion.indian_stocks_api import (
    StockDatasetType,
    run_stock_metadata_ingestion,
    run_historical_prices_ingestion,
    run_historical_stats_ingestion,
    run_trending_ingestion,
    run_price_shockers_ingestion,
    run_nse_most_active_ingestion,
    run_52_week_hld_ingestion,
)

logger = get_logger(__name__, caller_file_path=__file__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Indian Stock API Lambda entrypoint.

    Expected event structure:
    {
        "dataset": "STOCK_METADATA" | "HISTORICAL_DATA" | "HISTORICAL_STATS"
                   | "TRENDING" | "PRICE_SHOCKERS" | "NSE_MOST_ACTIVE"
                   | "FETCH_52_WEEK_HLD",
        "params": { ... }
    }
    """

    logger.info(f"Received event: {event}")
    lambda_exec_ts = datetime.now(timezone.utc).isoformat()

    try:
        dataset = event.get("dataset")
        params = event.get("params", {})

        if not dataset:
            raise ValueError("Missing required field: dataset")

        try:
            dataset_enum = StockDatasetType[dataset]
        except ValueError:
            raise ValueError(f"Unsupported Indian Stock dataset: {dataset}")

        if dataset_enum == StockDatasetType.STOCK_METADATA:
            result = run_stock_metadata_ingestion(
                stock_names=params.get("stock_names"),
            )

        elif dataset_enum == StockDatasetType.HISTORICAL_DATA:
            result = run_historical_prices_ingestion(
                stock_names=params.get("stock_names"),
                period=params.get("period", "5yr"),
                filter=params.get("filter", "price"),
            )

        elif dataset_enum == StockDatasetType.HISTORICAL_STATS:
            result = run_historical_stats_ingestion(
                stock_names=params.get("stock_names"),
                stats_list=params.get("stats_list"),
            )

        elif dataset_enum == StockDatasetType.TRENDING:
            result = run_trending_ingestion()

        elif dataset_enum == StockDatasetType.PRICE_SHOCKERS:
            result = run_price_shockers_ingestion()

        elif dataset_enum == StockDatasetType.NSE_MOST_ACTIVE:
            result = run_nse_most_active_ingestion()

        elif dataset_enum == StockDatasetType.FETCH_52_WEEK_HLD:
            result = run_52_week_hld_ingestion()

        else:
            raise ValueError(f"Unhandled dataset: {dataset_enum.value}")

        return {
            "status": "SUCCESS",
            "lambda_exec_ts": lambda_exec_ts,
            **result,
        }

    except Exception as e:
        logger.exception("Indian Stock Lambda execution failed")

        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            "lambda_exec_ts": lambda_exec_ts,
            "retryable": True,
        }