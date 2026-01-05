from typing import Dict, Any
from datetime import datetime, timezone

from src.utils.logger import get_logger
from src.ingestion.alphavantage import run_commodity_prices_ingestion

logger = get_logger(__name__, caller_file_path=__file__)

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    request_id = context.aws_request_id
    logger.info(f"Lambda invocation started | request_id={request_id}")
    if "dag_id" in event and "run_id" in event:
        logger.info(
            "Invocation context",
            extra={
                "dataset": "commodity_prices",
                "source": "alphavantage",
                "airflow_dag_id": event.get("dag_id"),
                "run_id": event.get("run_id"),
            }
        )
    logger.info(f"Received event: {event}")

    try:
        result = run_commodity_prices_ingestion(
            functions=event["functions"],
            interval=event.get("interval", "daily"),
        )

        return {
            "status": "SUCCESS",
            "executed_at": datetime.now(timezone.utc).isoformat(),
            **result,
        }

    except Exception as e:
        logger.exception("Lambda ingestion for run_commodity_prices_ingestion failed")

        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "retryable": True,
        }