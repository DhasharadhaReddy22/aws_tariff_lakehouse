from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from enum import Enum

from src.utils.api_client import APIClient
from src.utils.config import config
from src.utils.bucket_client import bucket_client
from src.utils.logger import get_logger
from .ingestion_utils import build_raw_key, create_filename

logger = get_logger(__name__, caller_file_path=__file__)

BASE_URL = "https://newsapi.org"
DOMAIN = "news"
SOURCE_NAME = "newsapi"

class DatasetType(str, Enum):
    TOP_HEADLINES = "top_headlines"
    EVERYTHING = "everything"

NEWSAPI_KEY = config.get("NEWSAPI_API_KEY")
if not NEWSAPI_KEY:
    raise RuntimeError("NEWSAPI_API_KEY is not configured")

newsapi_client = APIClient(
    base_url=BASE_URL,
    timeout=30,
    max_retries=3,
    backoff_base=2,
    backoff_cap=10,
)


def fetch_newsapi_top_headlines_raw(
    country: Optional[str] = None,
    category: Optional[str] = None,
    sources: Optional[str] = None,
    q: Optional[str] = "American Tariffs on India",
    page_size: int = 100,
    page: int = 1,
) -> List[Dict[str, Any]]:
    """
    Fetch top headlines from NewsAPI and return flattened raw records.
    This function ONLY indexes articles (no scraping).
    Args:
        country: The 2-letter ISO 3166-1 code of the country you want to get headlines for.
        category: The category you want to get headlines for.
        sources: A comma-separated string of identifiers for the news sources or blogs you want headlines from.
        q: Keywords or phrases to search for in the article title and body.
        page_size: The number of results to return per page (maximum 100).
        page: The page number to retrieve.
    Returns:
        A list of flattened article records.
    """

    params = {
        "pageSize": page_size,
        "page": page,
    }

    if country:
        params["country"] = country
    if category:
        params["category"] = category
    if sources:
        params["sources"] = sources
    if q:
        params["q"] = q

    logger.info(f"Fetching NewsAPI top headlines | params={params}")
    params["apiKey"] = NEWSAPI_KEY
    resp = newsapi_client.get("/v2/top-headlines", params=params)

    if not resp["ok"]:
        logger.error(f"NewsAPI top-headlines failed: {resp['error']}")
        raise RuntimeError("NewsAPI top-headlines ingestion failed")

    payload = resp["data"]
    articles = payload.get("articles", [])

    records: List[Dict[str, Any]] = []

    for article in articles:
        records.append({
            # Article identity
            "article_url": article.get("url"),
            "title": article.get("title"),
            "author": article.get("author"),
            "source_news": article.get("source", {}).get("name"),
            "source_id": article.get("source", {}).get("id"),
            "description": article.get("description"),
            "published_at": article.get("publishedAt"),

            # Ingestion metadata
            "source_api": SOURCE_NAME,
            "dataset": DatasetType.TOP_HEADLINES.value,

            # Transport metadata
            "request_url": resp["url"],
            "received_at": resp["received_at"],
        })

    logger.info(f"Fetched {len(records)} top-headline articles from NewsAPI")
    return records

def fetch_newsapi_everything_raw(
    q: str,
    search_in: Optional[str] = None,
    sources: Optional[str] = None,
    domains: Optional[str] = None,
    exclude_domains: Optional[str] = None,
    from_dt: Optional[str] = None,
    to_dt: Optional[str] = None,
    language: Optional[str] = None,
    sort_by: str = "publishedAt",
    page_size: int = 100,
    page: int = 1,
) -> List[Dict[str, Any]]:
    """
    Fetch articles from NewsAPI 'everything' endpoint and return flattened raw records.
    This function ONLY indexes articles (no scraping).
    """

    if not q:
        raise ValueError("Parameter 'q' is required for NewsAPI everything endpoint")

    params = {
        "q": q,
        "sortBy": sort_by,
        "pageSize": page_size,
        "page": page,
    }

    # Optional filters
    if search_in:
        params["searchIn"] = search_in
    if sources:
        params["sources"] = sources
    if domains:
        params["domains"] = domains
    if exclude_domains:
        params["excludeDomains"] = exclude_domains
    if from_dt:
        params["from"] = from_dt
    if to_dt:
        params["to"] = to_dt
    if language:
        params["language"] = language

    logger.info(f"Fetching NewsAPI everything | params={params}")
    params["apiKey"] = NEWSAPI_KEY

    resp = newsapi_client.get("/v2/everything", params=params)

    if not resp["ok"]:
        logger.error(f"NewsAPI everything failed: {resp['error']}")
        raise RuntimeError("NewsAPI everything ingestion failed")

    payload = resp["data"]
    articles = payload.get("articles", [])

    records: List[Dict[str, Any]] = []

    for article in articles:
        records.append({
            # Article identity
            "article_url": article.get("url"),
            "title": article.get("title"),
            "author": article.get("author"),
            "source_news": article.get("source", {}).get("name"),
            "source_id": article.get("source", {}).get("id"),
            "description": article.get("description"),
            "published_at": article.get("publishedAt"),

            # Ingestion metadata
            "source_api": SOURCE_NAME,
            "dataset": DatasetType.EVERYTHING.value,

            # Transport metadata
            "request_url": resp["url"],
            "received_at": resp["received_at"],
        })

    logger.info(f"Fetched {len(records)} articles from NewsAPI everything")
    return records

def write_newsapi_raw(dataset: DatasetType, records: List[Dict[str, Any]]) -> None:
    """
    Write raw records to storage.
    Args:
        dataset: The dataset type (top_headlines or everything).
        records: The list of raw records to write.
    """

    if not records:
        logger.warning(f"No records to write for dataset={dataset.value}")
        raise ValueError(f"No records to write for dataset={dataset.value}")
    
    ingested_at = datetime.now(timezone.utc).isoformat()
    for r in records:
        r["ingested_at"] = ingested_at

    key = build_raw_key(
        domain=DOMAIN,
        source=SOURCE_NAME,
        dataset=dataset.value,  # resolved dataset name
        ingestion_date=ingested_at[:10],
        filename=create_filename(dataset.value, ingested_at, ".jsonl"),
    )

    logger.info(f"Writing {len(records)} records to s3://{bucket_client.bucket_name}/{key}")
    bucket_client.put_jsonl(key=key, records=records)
    logger.info(f"Wrote {len(records)} records to s3://{bucket_client.bucket_name}/{key}")

def run_newsapi_top_headlines_ingestion(
    country: Optional[str] = None,
    category: Optional[str] = None,
    sources: Optional[str] = None,
    q: Optional[str] = "American Tariffs on India",
    page_size: int = 100,
    page: int = 1,
) -> None:

    records = fetch_newsapi_top_headlines_raw(
        country=country,
        category=category,
        sources=sources,
        q=q,
        page_size=page_size,
        page=page,
    )
    write_newsapi_raw(DatasetType.TOP_HEADLINES, records)

def run_newsapi_everything_ingestion(
    q: str,
    search_in: Optional[str] = None,
    sources: Optional[str] = None,
    domains: Optional[str] = None,
    exclude_domains: Optional[str] = None,
    from_dt: Optional[str] = None,
    to_dt: Optional[str] = None,
    language: Optional[str] = None,
    sort_by: str = "publishedAt",
    page_size: int = 100,
    page: int = 1,
) -> None:
    """
    Orchestrates NewsAPI 'everything' ingestion.
    """

    records = fetch_newsapi_everything_raw(
        q=q,
        search_in=search_in,
        sources=sources,
        domains=domains,
        exclude_domains=exclude_domains,
        from_dt=from_dt,
        to_dt=to_dt,
        language=language,
        sort_by=sort_by,
        page_size=page_size,
        page=page,
    )

    write_newsapi_raw(DatasetType.EVERYTHING, records)

if __name__ == "__main__":

    # run_newsapi_top_headlines_ingestion(
    #     country="in", 
    #     category="business", 
    #     q='"US tariffs" AND India', 
    #     page_size=50, 
    #     page=1
    # )

    run_newsapi_everything_ingestion(
        q='"US tariffs" AND India AND impact',
        language="en",
        sort_by="publishedAt",
        page_size=100,
    )