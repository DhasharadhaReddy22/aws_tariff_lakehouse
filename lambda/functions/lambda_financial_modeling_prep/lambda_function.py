from typing import Dict, Any
from datetime import datetime, timezone

from src.utils.logger import get_logger
from src.ingestion.financial_modeling_prep import run_fmp_ingestion, DatasetType

logger = get_logger(__name__, caller_file_path=__file__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Financial Modeling Prep Lambda entrypoint.

    Expected event structure:
    {
        "dataset": "MARKET_RISK_PREMIUM"
                   | "SECTOR_SNAPSHOT"
                   | "SECTOR_HISTORICAL"
                   | "INDUSTRY_SNAPSHOT"
                   | "INDUSTRY_HISTORICAL",
        "params": { ... }
    }
    """

    logger.info(f"Received event: {event}")

    lambda_exec_ts = datetime.now(timezone.utc).isoformat()

    try:
        dataset_raw = event.get("dataset")
        params = event.get("params", {})

        if not dataset_raw:
            raise ValueError("Missing required field: dataset")

        try:
            dataset = DatasetType[dataset_raw]
        except ValueError:
            raise ValueError(f"Unsupported dataset: {dataset_raw}")

        result = run_fmp_ingestion(dataset=dataset, **params)

        return {
            "status": "SUCCESS",
            "lambda_exec_ts": lambda_exec_ts,
            **result,
        }

    except Exception as e:
        logger.exception("FMP Lambda execution failed")

        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            "lambda_exec_ts": lambda_exec_ts,
            "retryable": True,
        }