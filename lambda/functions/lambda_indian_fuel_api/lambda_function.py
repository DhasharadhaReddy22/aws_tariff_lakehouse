from typing import Dict, Any
from datetime import datetime, timezone

from src.utils.logger import get_logger
from src.ingestion.indian_fuel_api import (
    run_fuel_ingestion,
    FuelDatasetType,
)

logger = get_logger(__name__, caller_file_path=__file__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Indian Fuel API Lambda entrypoint.

    Expected event structure:
    {
        "dataset": "PETROL_STATE_LIVE" | "DIESEL_CITY_HISTORICAL" | ...,
        "params": {
            "location": "Delhi",        # required for historical
            "output_size": 7            # required for historical
        }
    }
    """

    logger.info(f"Received event: {event}")
    api_params = event.get("api_params")
    lambda_exec_ts = datetime.now(timezone.utc).isoformat()

    try:
        dataset = api_params.get("dataset")
        params = api_params.get("params", {})

        if not dataset:
            raise ValueError("Missing required field: dataset")

        try:
            dataset_enum = FuelDatasetType[dataset]
        except ValueError:
            raise ValueError(f"Unsupported Fuel dataset: {dataset}")

        result = run_fuel_ingestion(
            dataset=dataset_enum,
            location=params.get("location", None),
            output_size=params.get("output_size", None),
        )

        return {
            "status": "SUCCESS",
            "lambda_exec_ts": lambda_exec_ts,
            **result,
        }

    except Exception as e:
        logger.exception("Indian Fuel Lambda execution failed")

        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            "lambda_exec_ts": lambda_exec_ts,
            "retryable": True,
        }