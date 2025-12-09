import requests
import time
import os
from typing import Dict, Any
from urllib.parse import urljoin
from dotenv import load_dotenv
from requests.structures import CaseInsensitiveDict
from urllib.parse import urlencode
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(name)s | %(asctime)s] - [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

class APIClient:
    def __init__(self, 
                 base_url: str, 
                 headers: dict = None, 
                 timeout: int = 10, 
                 backoff: int = 2, 
                 max_retries: int = 3
        ):
        self.base_url = base_url
        self.headers = headers or {}
        self.timeout = timeout
        self.backoff = backoff
        self.max_retries = max_retries

    def get(self, endpoint: str, params: dict = None) -> dict:
        """
        Executes a GET request.

        Args:
            endpoint (str): The API endpoint to call.
            params (dict, optional): Dictionary of parameters to send.
            
        Returns:
            dict: The JSON response from the API.
        """
        url = urljoin(self.base_url, endpoint)
        sanitized_url = self.sanitize_url(url, params)
        attempt = 0

        while attempt < self.max_retries:
            try:
                logger.info(f"GET {url} (Attempt {attempt + 1}/{self.max_retries})")
                response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)
                data = response.json()
                if isinstance(data, dict) and "error" in data:
                    logger.error(f"API call successful, but error text received for {url} for params={params}, ERROR: {data['error']}")
                    return {}, sanitized_url
                logger.info(f"Successfully fetched from {endpoint}")
                return data, sanitized_url
            
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error occurred: {str(e)}")
                return {}, sanitized_url

            except requests.exceptions.Timeout:
                logger.error(f"Timeout while calling {url} with params={params}")
                return {}, sanitized_url
            
            except requests.exceptions.RequestException as e:
                attempt += 1
                if attempt >= self.max_retries:
                    logger.error(f"Failed to GET {url} after {self.max_retries} attempts, ERROR: {str(e)}")
                    return {}, sanitized_url
                sleep_time = self.backoff ** attempt  # Exponential backoff
                logger.warning(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            
            except Exception as e:
                logger.exception(f"Unexpected error in fetch_data: {str(e)}")
                return {}, sanitized_url

    @staticmethod
    def data_with_metadata(data: dict, source: str, params: dict, fetched_at: str, status_code: int) -> dict:
        """
        Wraps the API response with metadata.
        Args:
            data (dict): The API response data.
            source (str): The source of the data (e.g., API name).
            params (dict): The parameters used in the API request.
            status_code (int): The HTTP status code of the response.
        Returns:
            dict: The API response wrapped with metadata.
        """
        safe_params = {k: v for k, v in (params or {}).items() if "key" not in k.lower()}
        record = {
            **(data if isinstance(data, dict) else {"_raw": data}),
            "_fetched_at": fetched_at,
            "_status_code": status_code,
            "_source": source,
            "_params": str(safe_params)
        }
        return record
    
    @staticmethod
    def sanitize_url(
        url: str,
        params: Dict[str, Any],
        sensitive_params=["apikey", "APIKEY", "api_key", "api-key", 
                        "API_KEY", "API-KEY", "token", "key", 
                        "access_token", "password"]
    ) -> str:
        """Remove sensitive params and return a safe-to-log URL."""
        safe_params = {}
        for k, v in (params or {}).items():
            if k in sensitive_params:
                safe_params[k] = "REDACTED"
            else:
                safe_params[k] = v

        query = urlencode(safe_params, doseq=True)
        sanitized_url = f"{url}?{query}" if query else url
        return sanitized_url

if __name__ == "__main__":
    # US Gold Spot
    api_key = os.getenv("TWELVE_DATA_API_KEY")
    twelve_data_client = APIClient(base_url="https://api.twelvedata.com")
    params = {
        "apikey": f"{api_key}", # You need to get a real one
        "interval": "1h",
        "symbol": "XAU/USD",
        "country": "US",
        "exchange": "NASDAQ",
        "dp": 4,
        "timezone": "utc",
        "format": "JSON",
        "outputsize": 1
    }
    gold_data, sanitized_url = twelve_data_client.get("/time_series", params=params)
    print("Gold Data:", gold_data)
    print("Sanitized_url", sanitized_url)
    #######################################################################################
    
    # Exchange Rate
    params = {
        "apikey": f"{api_key}", # You need to get a real one
        "interval": "1h",
        "symbol": "INR/USD",
        "dp": 4,
        "timezone": "utc",
        "format": "JSON",
        "outputsize": 1
    }
    exchangerate_usd_inr = twelve_data_client.get("/exchange_rate", params=params)
    print("Exchange Rate Data:", exchangerate_usd_inr)
    #######################################################################################

    # US Metals
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    api_key = os.getenv('METALS_DEV_API_KEY')
    metalsdev_client = APIClient(base_url="https://api.metals.dev", headers=headers)
    params = {
        "api_key": f"{api_key}",
        "currency": "USD",
        "unit": "g"
    }
    metals_data = metalsdev_client.get("/v1/latest", params=params)
    print("Metals Data:", metals_data)