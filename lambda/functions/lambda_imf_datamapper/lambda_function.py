from typing import Dict, Any
from datetime import datetime, timezone

from src.utils.logger import get_logger
from src.ingestion.imf_datamapper import run_imf_ingestion

logger = get_logger(__name__, caller_file_path=__file__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    IMF Datamapper Lambda entrypoint.

    Expected event structure:
    {
        "indicator_codes": ["NGDP_RPCH", "NGDPD", ...],
        "params": {
            "years": [2021, 2022],
            "countries": ["IND", "USA"]
        }
    }
    """

    logger.info(f"Received event: {event}")

    lambda_exec_ts = datetime.now(timezone.utc).isoformat()

    try:
        indicator_codes = event.get("indicator_codes")
        params = event.get("params", {})

        if not indicator_codes:
            raise ValueError("Missing required field: indicator_codes")

        if not params:
            raise ValueError("Missing required field: params")

        result = run_imf_ingestion(
            indicator_codes=indicator_codes,
            params=params,
        )

        return {
            "status": "SUCCESS",
            "lambda_exec_ts": lambda_exec_ts,
            **result,
        }

    except Exception as e:
        logger.exception("IMF Lambda execution failed")

        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            "lambda_exec_ts": lambda_exec_ts,
            "retryable": True,
        }