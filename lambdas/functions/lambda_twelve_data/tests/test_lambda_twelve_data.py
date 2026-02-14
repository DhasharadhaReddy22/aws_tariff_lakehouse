from lambdas.functions.lambda_twelve_data.lambda_function import lambda_handler

def test_lambda_success():
    event = {
        "source": "twelvedata",
        "domain": "commodities",
        "dataset": "exchange_rates",
        "api_params": {
            "symbols" : ["XAU/USD", "USD/INR"],
                "params" : {
                    "interval": "1day",
                    "dp": 4,
                    "timezone": "utc",
                    "exchange": "NASDAQ",
                    "format": "JSON",
                    "outputsize": 365
                }
            },
        "triggered_at": "2026-01-20T18:44:04.886035+00:00"
    }

    result = lambda_handler(event, None)

    assert isinstance(result, dict)
    assert result["status"] == "SUCCESS"