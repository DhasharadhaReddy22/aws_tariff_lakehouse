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
        "params": {
            "indicator_codes": ["NGDP_RPCH", "NGDPD", ...],
            "years": [2021, 2022],
            "countries": ["IND", "USA"]
        }
    }
    """

    logger.info(f"Received event: {event}")
    api_params = event.get("api_params")
    lambda_exec_ts = datetime.now(timezone.utc).isoformat()

    try:
        params = api_params.get("params")

        if not params:
            raise ValueError("Missing required field: params")

        if not params.get("indicator_codes", {}):
            raise ValueError("Missing required field: indicator_codes")

        result = run_imf_ingestion(params=params)

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