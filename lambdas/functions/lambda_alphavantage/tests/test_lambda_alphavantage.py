from unittest.mock import patch
from lambdas.functions.lambda_alphavantage.lambda_function import lambda_handler


def test_lambda_success():
    event = {
        "source": "alphavantage",
        "domain": "american_markets",
        "dataset": "TIME_SERIES_DAILY",
        "api_params": {
            "dataset": "TIME_SERIES_DAILY",
            "params": {
                "symbols": ["AAPL", "MSFT", "IBM"],
                "outputsize": "compact"
            }
        },
        "triggered_at": "2026-01-20T18:44:04.886035+00:00"
    }

    fake_api_response = {
        "ok": True,
        "status_code": 200,
        "data": {
            "Time Series (Daily)": {
                "2024-01-01": {
                    "1. open": "100",
                    "2. high": "110",
                    "3. low": "95",
                    "4. close": "105",
                    "5. volume": "1000000"
                }
            }
        },
        "error": None,
        "url": "https://mocked-url",
        "received_at": "2026-01-20T18:44:04.886035+00:00"
    }

    with patch("src.ingestion.alphavantage.APIClient.get") as mock_api_get, \
         patch("src.ingestion.alphavantage.bucket_client.put_jsonl") as mock_s3:

        mock_api_get.return_value = fake_api_response
        mock_s3.return_value = None

        result = lambda_handler(event, None)

        assert isinstance(result, dict)
        assert result["status"] == "SUCCESS"