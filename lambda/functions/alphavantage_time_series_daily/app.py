from typing import Dict, Any
from datetime import datetime, timezone

from src.utils.logger import get_logger
from src.ingestion.alphavantage import run_daily_stock_prices_ingestion

logger = get_logger(__name__, caller_file_path=__file__)

def handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    logger.info(f"Received event: {event}")

    try:
        result = run_daily_stock_prices_ingestion(
            symbols=event["symbols"],
            outputsize=event.get("outputsize", "compact"),
        )

        return {
            "status": "SUCCESS",
            "executed_at": datetime.now(timezone.utc).isoformat(),
            **result,
        }

    except Exception as e:
        logger.exception("Lambda ingestion for run_daily_stock_prices_ingestion failed")

        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "retryable": True,
        }