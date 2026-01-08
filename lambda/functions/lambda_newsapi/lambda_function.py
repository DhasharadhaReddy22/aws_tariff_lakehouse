from typing import Dict, Any
from datetime import datetime, timezone

from src.utils.logger import get_logger
from src.ingestion.newsapi import (
    run_newsapi_top_headlines_ingestion,
    run_newsapi_everything_ingestion,
)

logger = get_logger(__name__, caller_file_path=__file__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    NewsAPI Lambda entrypoint.

    Expected event structure:
    {
        "dataset": "TOP_HEADLINES" | "EVERYTHING",
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

        if dataset == "TOP_HEADLINES":
            result = run_newsapi_top_headlines_ingestion(
                country=params.get("country"),
                category=params.get("category"),
                sources=params.get("sources"),
                q=params.get("q"),
                page_size=params.get("page_size", 100),
                page=params.get("page", 1),
            )

        elif dataset == "EVERYTHING":
            if not params.get("q"):
                raise ValueError("'q' is required for NewsAPI EVERYTHING dataset")

            result = run_newsapi_everything_ingestion(
                q=params["q"],
                search_in=params.get("search_in"),
                sources=params.get("sources"),
                domains=params.get("domains"),
                exclude_domains=params.get("exclude_domains"),
                from_dt=params.get("from_dt"),
                to_dt=params.get("to_dt"),
                language=params.get("language"),
                sort_by=params.get("sort_by", "publishedAt"),
                page_size=params.get("page_size", 100),
                page=params.get("page", 1),
            )

        else:
            raise ValueError(f"Unsupported dataset: {dataset}")

        return {
            "status": "SUCCESS",
            "lambda_exec_ts": lambda_exec_ts,
            **result,
        }

    except Exception as e:
        logger.exception("NewsAPI Lambda execution failed")

        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            "lambda_exec_ts": lambda_exec_ts,
            "retryable": True,
        }