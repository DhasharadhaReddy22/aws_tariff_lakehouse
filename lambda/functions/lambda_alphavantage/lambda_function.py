from typing import Dict, Any
from datetime import datetime, timezone

from src.utils.logger import get_logger
from src.ingestion.alphavantage import (
    run_daily_stock_prices_ingestion,
    run_commodity_prices_ingestion,
)

logger = get_logger(__name__, caller_file_path=__file__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    AlphaVantage Lambda entrypoint.

    Expected event structure:
    {
        "dataset": "TIME_SERIES_DAILY" | "COMMODITY",
        "params": { ... }
    }
    """

    logger.info(f"Received event: {event}")

    try:
        dataset = event.get("dataset")
        params = event.get("params", {})

        if not dataset:
            raise ValueError("Missing required field: dataset")

        lambda_exec_ts = datetime.now(timezone.utc).isoformat()
        if dataset == "TIME_SERIES_DAILY":
            result = run_daily_stock_prices_ingestion(
                symbols=params.get("symbols"),
                outputsize=params.get("outputsize", "compact"),
            )

        elif dataset == "COMMODITY":
            result = run_commodity_prices_ingestion(
                functions=params.get("functions"),
                interval=params.get("interval", "daily"),
            )

        else:
            raise ValueError(f"Unsupported dataset: {dataset}")

        return {
            "status": "SUCCESS",
            "lambda_exec_ts": lambda_exec_ts,
            **result,
        }

    except Exception as e:
        logger.exception("AlphaVantage Lambda execution failed")

        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            "lambda_exec_ts": lambda_exec_ts,
            "retryable": True,
        }