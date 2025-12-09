from typing import List, Dict, Any, Generator
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
import logging

from src.utils.api_client import APIClient

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(name)s | %(asctime)s] "
           "- [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

alpha_vantage_client = APIClient(
    base_url="https://www.alphavantage.co",
    timeout=30,
    max_retries=3
)

def fetch_commodities_raw(functions: List[str], params: Dict[str, Any] = {}) -> Generator[Dict[str, Any], None, None]:
    """
    Fetches flattened RAW responses for commodities from Alpha Vantage.
    """
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        logging.error("ALPHA_VANTAGE_API_KEY is not set!")
        return

    all_records = []

    for function in functions:
        symbol_params = params.copy()
        symbol_params["function"] = function
        symbol_params["apikey"] = api_key

        logging.info(f"Fetching commodity data for function={function}")

        fetched_time = datetime.now(timezone.utc).isoformat()
        raw_response, sanitized_url = alpha_vantage_client.get("/query", params=symbol_params)

        if not raw_response:
            logging.warning(f"No data returned for commodity={function}, skipping.")
            continue
        
        name = raw_response.get("name")
        unit = raw_response.get("unit")
        data = raw_response.get("data")
        for record in data:
            all_records.append({
                "commodity": name,
                "symbol": function,
                "unit": unit,
                "date": record.get("date"),
                "value": record.get("value"),
                "_fetched_at": fetched_time,
                "_source": "Alpha Vantage",
                "_sanitized_url": sanitized_url,
            })

    return all_records
        

def fetch_company_stock_raw(function:str = "TIME_SERIES_DAILY", companies:List[str] = [], params: Dict[str, Any] = {}) -> Generator[Dict[str, Any], None, None]:
    """
    Fetches RAW responses for company stock data from Alpha Vantage API and yields them as-is for bronze layer storage.
    Each yielded item is a complete API response for one company.

    Args:
        function (str): The function to call (e.g., "TIME_SERIES_WEEKLY_ADJUSTED"), this determines the granularity or adjusted stock values of the companies
        companies (List[str]): List of company symbols like ['AAPL', 'MSFT']
        params (Dict[str, str]): Additional parameters for the API request

    Yields:
        Dict[str, Any]: Raw API response exactly as received, plus ingestion metadata
    """
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        print("ALPHA_VANTAGE_API_KEY is not set!")
        return
    
    if "ADJUSTED" in function:
        logger.warning("Please use fetch_company_stock_adjusted_raw to fetch adjusted values of stocks")
        return []

    all_records = []

    for company in companies:
        symbol_params = params.copy()
        symbol_params["function"] = function
        symbol_params["symbol"] = company
        symbol_params["apikey"] = api_key

        print(f"Fetching RAW data for company: {company}")
        fetched_time = datetime.now(timezone.utc).isoformat()
        raw_response, sanitized_url = alpha_vantage_client.get("/query", params=symbol_params)

        if not raw_response:
            print(f"No data returned for {company}, skipping.")
            continue

        time_series_key = next((k for k in raw_response.keys() if "Time Series" in k), None)
        if not time_series_key:
            logging.warning(f"No time series data in response for company={company}")
            continue
        
        data = raw_response.get(time_series_key)
        for date, metrics in data.items():
            all_records.append({
                "date": date,
                "company": company,
                "open": metrics.get("1. open"),
                "high": metrics.get("2. high"),
                "low": metrics.get("3. low"),
                "close": metrics.get("4. close"),
                "volume": metrics.get("5. volume"),
                "_source": "Alpha Vantage",
                "_ingestion_time": fetched_time,
                "_request_url": sanitized_url
            })
    logger.info(f"Fetched data for all the companies in {companies}")
    return all_records

def fetch_company_stock_adjusted_raw(function:str = "TIME_SERIES_DAILY", companies:List[str] = [], params: Dict[str, Any] = {}) -> Generator[Dict[str, Any], None, None]:
    """
    Fetches RAW responses for company stock data from Alpha Vantage API and yields them as-is for bronze layer storage.
    Each yielded item is a complete API response for one company.

    Args:
        function (str): The function to call (e.g., "TIME_SERIES_WEEKLY_ADJUSTED"), this determines the granularity or adjusted stock values of the companies
        companies (List[str]): List of company symbols like ['AAPL', 'MSFT']
        params (Dict[str, str]): Additional parameters for the API request

    Yields:
        Dict[str, Any]: Raw API response exactly as received, plus ingestion metadata
    """
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        print("ALPHA_VANTAGE_API_KEY is not set!")
        return
    
    if "ADJUSTED" not in function:
        logger.warning("Please use fetch_company_stock_raw for non-adjusted company stock values")
        return []

    all_records = []

    for company in companies:
        symbol_params = params.copy()
        symbol_params["function"] = function
        symbol_params["symbol"] = company
        symbol_params["apikey"] = api_key

        print(f"Fetching RAW data for company: {company}")
        fetched_time = datetime.now(timezone.utc).isoformat()
        raw_response, sanitized_url = alpha_vantage_client.get("/query", params=symbol_params)

        if not raw_response:
            print(f"No data returned for {company}, skipping.")
            continue

        time_series_key = next((k for k in raw_response.keys() if "Time Series" in k), None)
        if not time_series_key:
            logging.warning(f"No time series data in response for company={company}")
            continue
        
        data = raw_response.get(time_series_key)
        for date, metrics in data.items():
            all_records.append({
                "date": date,
                "company": company,
                "open": metrics.get("1. open"),
                "high": metrics.get("2. high"),
                "low": metrics.get("3. low"),
                "close": metrics.get("4. close"),
                "adjusted_close": metrics.get("5. adjusted close"),
                "volume": metrics.get("6. volume"),
                "adjusted_volume": metrics.get("7. dividend amount"),
                "_source": "Alpha Vantage",
                "_ingestion_time": fetched_time,
                "_request_url": sanitized_url
            })
    logger.info(f"Fetched data for all the companies in {companies}")
    return all_records
    

if __name__ == "__main__":
    # # Oil
    functions = ["WTI"]
    params = {
        "interval": "monthly"
    }

    data = fetch_commodities_raw(functions, params)
    print(data[0])

    # Company quotes
    companies = ["SIG", "WSM", "F", "TJX"] # Brands that are sourced and sell globally
    function = "TIME_SERIES_WEEKLY_ADJUSTED"
    params = {
        "outputsize": "compact",
        "datatype": "json"
    }

    data = fetch_company_stock_raw(function=function, companies=companies, params=params)
    print(data[0], "\n", data[0].get("volume"), data[0].get("date"))

    data = fetch_company_stock_adjusted_raw(function=function, companies=companies, params=params)
    print(data[0], "\n", data[0].get("volume"), data[0].get("date"))