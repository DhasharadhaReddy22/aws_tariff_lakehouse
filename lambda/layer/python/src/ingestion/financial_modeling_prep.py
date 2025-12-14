from typing import List, Dict, Any, Generator
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from pathlib import Path
import logging

from src.utils.api_client import APIClient

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(name)s | %(asctime)s] "
           "- [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

fmp_client = APIClient(
    base_url="https://financialmodelingprep.com",
    timeout=30,
    max_retries=3
)

def fetch_market_risk_premium(countries: List[str] = None) -> List[Dict[str, Any]]:
    """Fetch country-level risk premiums with ingestion metadata."""
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        logger.error("FMP_API_KEY is not set!")
        return []

    fetched_time = datetime.now(timezone.utc).isoformat()
    params = {"apikey": api_key}

    raw_response, sanitized_url = fmp_client.get("/stable/market-risk-premium", params=params)
    if not raw_response:
        logger.warning("No data returned for market risk premium.")
        return []

    all_records = []
    for record in raw_response:
        if countries and record.get("country") not in countries:
            continue
        all_records.append({
            "country": record.get("country"),
            "continent": record.get("continent"),
            "countryRiskPremium": float(record.get("countryRiskPremium", 0)),
            "totalEquityRiskPremium": float(record.get("totalEquityRiskPremium", 0)),
            "_fetched_at": fetched_time,
            "_source": __name__,
            "_sanitized_url": sanitized_url
        })

    logger.info(f"Fetched {len(all_records)} risk premium records.")
    return all_records


def fetch_sector_perf_snapshot_us(sectors: List[str] = None, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """Fetch snapshot of sector performance in the US."""
    if not params or "date" not in params:
        logger.error("Date parameter is required and within a months range!")
        return []

    today = datetime.now()
    one_month_ago = today - relativedelta(months=1)
    given_date = datetime.strptime(params["date"], "%Y-%m-%d")
    if given_date.date() < one_month_ago.date():
        logger.error(f"Date {given_date.date()} is older than one month from today.")
        return []

    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        logger.error("FMP_API_KEY is not set!")
        return []

    fetched_time = datetime.now(timezone.utc).isoformat()
    sector_params = params.copy()
    sector_params["apikey"] = api_key

    raw_response, sanitized_url = fmp_client.get("/stable/sector-performance-snapshot", params=sector_params)
    if not raw_response:
        logger.warning("No data returned for sectors.")
        return []

    all_records = []
    for record in raw_response:
        if sectors and record.get("sector") not in sectors:
            continue
        all_records.append({
            "date": record.get("date"),
            "sector": record.get("sector"),
            "exchange": record.get("exchange"),
            "averageChange": float(record.get("averageChange", 0)),
            "_fetched_at": fetched_time,
            "_source": __name__,
            "_sanitized_url": sanitized_url
        })

    logger.info(f"Fetched {len(all_records)} sector snapshot records.")
    return all_records

def fetch_industry_perf_snapshot_us(industries: List[str] = None, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """Fetch snapshot of industry performance in the US."""
    if not params or "date" not in params:
        logger.error("Date parameter is required!")
        return []

    today = datetime.now()
    one_month_ago = today - relativedelta(months=1)
    given_date = datetime.strptime(params["date"], "%Y-%m-%d")
    if given_date.date() < one_month_ago.date():
        logger.error(f"Date {given_date.date()} is older than one month from today.")
        return []

    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        logger.error("FMP_API_KEY is not set!")
        return []

    fetched_time = datetime.now(timezone.utc).isoformat()
    industry_params = params.copy()
    industry_params["apikey"] = api_key

    raw_response, sanitized_url = fmp_client.get("/stable/industry-performance-snapshot", params=industry_params)
    if not raw_response:
        logger.warning("No data returned for industries.")
        return []

    all_records = []
    for record in raw_response:
        if industries and record.get("industry") not in industries:
            continue
        all_records.append({
            "date": record.get("date"),
            "industry": record.get("industry"),
            "exchange": record.get("exchange"),
            "averageChange": float(record.get("averageChange", 0)),
            "_fetched_at": fetched_time,
            "_source": __name__,
            "_sanitized_url": sanitized_url
        })

    logger.info(f"Fetched {len(all_records)} industry snapshot records.")
    return all_records

def fetch_historic_sector_perf_us(sectors: List[str] = None, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """Fetch historical sector performance in the US."""
    if not params or "from" not in params or "to" not in params:
        logger.error("Both 'from' and 'to' parameters are required!")
        return []

    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        logger.error("FMP_API_KEY is not set!")
        return []

    if sectors is None or len(sectors)==0:
        logger.warning("No sector(s) were provided.")
        return []

    fetched_time = datetime.now(timezone.utc).isoformat()
    hist_params = params.copy()
    hist_params["apikey"] = api_key

    all_records = []
    for sector in sectors:
        hist_params["sector"] = sector
        raw_response, sanitized_url = fmp_client.get("/stable/historical-sector-performance", params=hist_params)
        if not raw_response:
            logger.warning(f"No data returned for {sector}.")
            continue

        for record in raw_response:
            all_records.append({
                "date": record.get("date"),
                "sector": record.get("sector"),
                "exchange": record.get("exchange"),
                "averageChange": float(record.get("averageChange", 0)),
                "_fetched_at": fetched_time,
                "_source": __name__,
                "_sanitized_url": sanitized_url
            })

    logger.info(f"Fetched historical sector records of {sectors}.")
    return all_records

def fetch_historic_industry_perf_us(industries: List[str] = None, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """Fetch historical industry performance in the US."""
    if not params or "from" not in params or "to" not in params:
        logger.error("Both 'from' and 'to' parameters are required!")
        return []

    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        logger.error("FMP_API_KEY is not set!")
        return []
    
    if industries is None or len(industries)==0:
        logger.warning("No industry(s) were provided.")
        return []

    fetched_time = datetime.now(timezone.utc).isoformat()
    hist_params = params.copy()
    hist_params["apikey"] = api_key
    all_records = []

    for industry in industries:
        hist_params["industry"] = industry
        raw_response, sanitized_url = fmp_client.get("/stable/historical-industry-performance", params=hist_params)
        if not raw_response:
            logger.warning(f"No data returned for {industry}.")
            return []

        for record in raw_response:
            all_records.append({
                "date": record.get("date"),
                "industry": record.get("industry"),
                "exchange": record.get("exchange"),
                "averageChange": float(record.get("averageChange", 0)),
                "_fetched_at": fetched_time,
                "_source": __name__,
                "_sanitized_url": sanitized_url
            })

    logger.info(f"Fetched historical industry records of {industries}.")
    return all_records

if __name__=="__main__":
    countries = ["United States", "India"]

    data = fetch_market_risk_premium(countries=countries)
    print(data)

    sectors = ["Basic Materials", "Utilities", "Consumer Cyclical"]
    params = {
        "date": "2025-09-01"
    }

    data = fetch_sector_perf_snapshot_us(sectors=sectors, params=params)
    print(data)

    industries = ["Consumer Electronics", "Auto - Parts", "Apparel - Retail", "Apparel - Footwear & Accessories"]
    params = {
        "date": "2025-09-01"
    }

    data = fetch_industry_perf_snapshot_us(industries=industries, params=params)
    print(data)

    sectors = ["Energy", "Utilities", "Consumer Cyclical"]
    params = {
        "from": "2025-07-01",
        "to": "2025-08-01",
        "exchange": "NASDAQ"
    }

    data = fetch_historic_sector_perf_us(sectors=sectors, params=params)
    print(data)

    industries = ["Consumer Electronics", "Auto - Parts", "Apparel - Retail", "Apparel - Footwear & Accessories"]
    params = {
        "from": "2025-07-01",
        "to": "2025-08-01",
        "exchange": "NASDAQ"
    }

    data = fetch_historic_industry_perf_us(industries=industries, params=params)
    print(data)
