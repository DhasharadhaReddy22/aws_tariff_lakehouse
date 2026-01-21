from typing import Dict, Any
from datetime import datetime, timezone

from src.utils.logger import get_logger
from src.ingestion.twelve_data import run_twelvedata_ingestion

logger = get_logger(__name__, caller_file_path=__file__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Twelve Data Lambda entrypoint.

    Expected event structure:
    {
        "symbols": ["XAU/USD", "USD/INR"],
        "params": {
            "interval": "1h",
            "outputsize": 12,
            ...
        }
    }
    """

    logger.info(f"Received event: {event}")
    api_params = event.get("api_params")
    lambda_exec_ts = datetime.now(timezone.utc).isoformat()

    try:
        symbols = api_params.get("symbols")
        params = api_params.get("params", {})

        if not symbols:
            raise ValueError("Missing required field: symbols")

        result = run_twelvedata_ingestion(
            symbols=symbols,
            params=params,
        )

        return {
            "status": "SUCCESS",
            "lambda_exec_ts": lambda_exec_ts,
            **result,
        }

    except Exception as e:
        logger.exception("Twelve Data Lambda execution failed")

        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            "lambda_exec_ts": lambda_exec_ts,
            "retryable": True,
        }