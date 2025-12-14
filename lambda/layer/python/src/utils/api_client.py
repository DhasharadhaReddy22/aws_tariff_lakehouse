import time
import random
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from urllib.parse import urljoin, urlencode

import requests
from requests.exceptions import Timeout, ConnectionError, HTTPError, RequestException
from requests.structures import CaseInsensitiveDict

from .logger import get_logger
from .config import config

logger = get_logger(__name__, caller_file_path=__file__)

class APIClient:
    def __init__(
            self, 
            base_url: str, 
            headers: Optional[Dict[str, str]] = None, 
            timeout: int = 10, 
            max_retries: int = 3,
            backoff_base: float = 1.0,
            backoff_cap: float = 8.0 
        ):
        self.base_url = base_url.rstrip('/')
        self.headers = headers or {}
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap

    @staticmethod
    def sanitize_url(
        url: str,
        params: Dict[str, Any],
        sensitive_keys=["apikey", "APIKEY", "api_key", "api-key", 
                        "API_KEY", "API-KEY", "token", "key", 
                        "access_token", "password"]
    ) -> str:
        """Remove sensitive params and return a safe-to-log URL."""
        if not params:
            # if params is empty
            return url
        
        safe_params = {}
        for k, v in params.items():
            if k.lower() in sensitive_keys:
                safe_params[k] = "REDACTED"
            else:
                safe_params[k] = v

        query = urlencode(safe_params, doseq=True)
        return f"{url}?{query}"
    
    @staticmethod
    def utc_now() -> str:
        """Returns current UTC time in ISO-8601 format YYYY-MM-DDTHH:MM:SS+00:00"""
        return datetime.now(timezone.utc).isoformat()
    
    def _compute_backoff(self, attempt: int) -> float:
        """
        Exponential backoff with jitter.

        backoff = min(cap, base * 2^(attempt-1)) + jitter
        jitter  = random(0, backoff/2)
        """
        delay = min(
            self.backoff_cap,
            self.backoff_base * (2 ** (attempt - 1)),
        )
        jitter = random.uniform(0, delay / 2) # divide by to so that the jitter will not lead to delay > backoff_cap
        return delay / 2 + jitter # divide by 2 so the delay will be in range [delay/2, delay] < backoff_cap
    
    @staticmethod
    def _default_response_validator(data: dict):
        """
        Detect soft API errors encoded in successful JSON responses.
        Inspects a 2xx response body for common error patterns.
        
        Example,
            "status_code": 200,
            "data": {
                "code": 401,
                "message": "...apikey incorrect...",
                "status": "error"
            }
        
        Returns tuple (has_error, error_message) indicating whether an error was detected.

        """
        if not isinstance(data, dict):
            return False, None

        # Common patterns across APIs with soft errors
        if data.get("status") == "error":
            return True, data.get("message", "API returned error status")

        if "error" in data and isinstance(data["error"], (str, dict)):
            return True, str(data["error"])

        if "code" in data and isinstance(data["code"], int) and data["code"] >= 400:
            return True, data.get("message", f"API error code {data['code']}")

        return False, None

    
    def get(self, endpoint: str, params: dict = None) -> dict:
        """
        Executes a GET request.

        Args:
            endpoint (str): The API endpoint to call.
            params (dict, optional): Dictionary of parameters to send.
            
        Returns:
            dict: The JSON response from the API.
        
        Further downstream processing can check the "ok" field to determine success.
        """
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        sanitized_url = self.sanitize_url(url, params)

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"GET {url} (Attempt {attempt}/{self.max_retries})")
                response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
                received_at = self.utc_now()
                status_code = response.status_code

                # Retryable HTTP statuses
                if status_code == 429 or status_code >= 500:
                    raise HTTPError(f"Retryable HTTP error {status_code}", response=response)

                response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)

                data = response.json()
                is_error, error_msg = self._default_response_validator(data)

                if is_error:
                    # retries on API-level errors will not be attempted
                    # retries on this level of errors won't help
                    logger.error(f"API-level error detected | {error_msg}")
                    return {
                        "ok": False,
                        "status_code": status_code,
                        "data": data, # raw payload passed for debugging
                        "error": error_msg,
                        "url": sanitized_url,
                        "received_at": received_at
                    }

                return {
                    "ok": True,
                    "status_code": status_code,
                    "data": data,
                    "error": None,
                    "url": sanitized_url,
                    "received_at": received_at
                }
            
            # Known exceptions, error_msg is passed if attempts exceeded
            except (Timeout, ConnectionError) as e:
                error_msg = f"Network error: {str(e)}"

            except HTTPError as e:
                status = getattr(e.response, "status_code", None)
                error_msg = f"HTTP error {status}: {str(e)}"

            except RequestException as e:
                error_msg = f"Request error: {str(e)}"

            # Unknown exception, error message provided by the api or requests module passed 
            except Exception as e:
                logger.error("Unexpected exception during API call")
                return {
                    "ok": False,
                    "status_code": None,
                    "data": None,
                    "error": str(e),
                    "url": sanitized_url,
                    "received_at": self.utc_now(),
                }
            
            # Handling known exceptions
            if attempt >= self.max_retries:
                logger.error(f"GET failed after {self.max_retries} attempts | {error_msg}")
                return {
                    "ok": False,
                    "status_code": None,
                    "data": None,
                    "error": error_msg,
                    "url": sanitized_url,
                    "received_at": self.utc_now(),
                }

            # Handling retries with exponential backoffs + jitter
            sleep_time = self._compute_backoff(attempt)
            logger.warning(f"{error_msg} | retrying in {sleep_time:.2f}s")
            time.sleep(sleep_time)


if __name__ == "__main__":
    # US Gold Spot
    api_key = config.get("TWELVE_DATA_API_KEY")
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
    gold_data = twelve_data_client.get("/time_series", params=params)
    print("Complete response: ", gold_data)
    print("Gold Data: ", gold_data["data"])
    print("Sanitized_url: ", gold_data["url"])
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
    api_key = config.get('METALS_DEV_API_KEY')
    metalsdev_client = APIClient(base_url="https://api.metals.dev", headers=headers)
    params = {
        "api_key": f"{api_key}",
        "currency": "USD",
        "unit": "g"
    }
    metals_data = metalsdev_client.get("/v1/latest", params=params)
    print("Metals Data:", metals_data)