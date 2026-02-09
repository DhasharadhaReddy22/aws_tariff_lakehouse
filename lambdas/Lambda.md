# AWS Lambda Functions and Layer

This README covers the development and deployment of Lambda functions and layers for the AWS Tariff Lakehouse pipeline, including packaging, configuration, and IAM setup.

---

## Architecture Overview

The Lambda architecture follows a **shared layer pattern**:

```
lambdas/
├── functions/                      # Individual Lambda function handlers
│   ├── lambda_alphavantage/
│   ├── lambda_imf_datamapper/
│   ├── lambda_indian_fuel_api/
│   ├── lambda_indian_stocks_api/
│   ├── lambda_metals_dev/
│   ├── lambda_newsapi/
│   ├── lambda_twelve_data/
│   └── lambda_financial_modeling_prep/
└── layer/                          # Shared Lambda layer
    ├── python/                     # Layer content (Lambda-compatible structure)
    │   ├── src/                    # Shared ingestion & utility modules
    │   │   ├── ingestion/          # Data source-specific ingestion logic
    │   │   └── utils/              # API client, S3 client, config, logger
    │   └── lib/                    # Third-party dependencies (from requirements.txt)
    │       └── python3.12/
    │           └── site-packages/
    └── requirements.txt            # Layer dependencies
```

#### Design Benefits

- **Single layer for all functions** – Eliminates code duplication across Lambda functions
- **Centralized dependency management** – All functions share the same version of `requests`, `boto3`, etc.
- **Simplified updates** – Update layer once, all functions benefit
- **Reduced deployment size** – Each function package is ~6KB (handler only)

#### Prerequisites

Before deploying, ensure you have:

- AWS CLI configured with appropriate credentials
- Python 3.12 installed locally
- IAM permissions to create Lambda functions, layers, and roles with the configured aws cli profile
- S3 bucket for raw data storage (e.g., `tariff-lakehouse-bucket`)
- SSM Parameter Store paths configured for production secrets

---

## Step 1: Build the Lambda Layer

The Lambda layer contains shared code (`src/`) and dependencies (`lib/`).

#### 1.1 Install Dependencies

```bash
cd lambdas/layer

# Install dependencies to the Lambda-compatible directory structure
pip install -r requirements.txt \
  --target python/lib/python3.12/site-packages/ \
  --python-version 3.12
```

#### 1.2 Verify Layer Structure

After installation, confirm the directory structure:

```bash
tree -L 3 python/
```

Expected output:
```
python/
├── lib/
│   └── python3.12/
│       └── site-packages/
│           ├── requests/
│           ├── urllib3/
│           ├── certifi/
│           ├── charset_normalizer/
│           ├── idna/
│           └── dotenv/
└── src/
    ├── __init__.py
    ├── ingestion/
    │   ├── __init__.py
    │   ├── alphavantage.py
    │   ├── imf_datamapper.py
    │   ├── indian_stocks_api.py
    │   ├── indian_fuel_api.py
    │   ├── metals_dev.py
    │   ├── twelve_data.py
    │   ├── newsapi.py
    │   ├── financial_modeling_prep.py
    │   └── ingestion_utils.py
    └── utils/
        ├── __init__.py
        ├── api_client.py
        ├── bucket_client.py
        ├── config.py
        └── logger.py
```

#### 1.3 Package the Layer

```bash
cd lambdas/layer

# Create layer zip (must include 'python/' directory at root)
zip -r tariff_lambda_layer.zip python/ -x "*.pyc" -x "*__pycache__*"
```

**Critical:** The zip must contain `python/` at the root level, not nested.

#### 1.4 Upload the Layer to AWS

```bash
aws lambda publish-layer-version \
  --layer-name tariff-ingestion-layer \
  --description "Shared ingestion utilities and dependencies for tariff Lambda functions" \
  --zip-file fileb://path/to/tariff_lambda_layer.zip \
  --compatible-runtimes python3.12 \
  --region us-east-1
```

**Output:** Note the `LayerVersionArn` (e.g., `arn:aws:lambda:us-east-1:123456789012:layer:tariff-ingestion-layer:1`). You'll reference this when deploying functions.

---

## Step 2: Package and Deploy Lambda Functions

Each Lambda function is a lightweight handler that imports from the shared layer.

#### 2.1 Package a Function

Example for `lambda_alphavantage`:

```bash
cd lambdas/functions/lambda_alphavantage

# Create deployment package (handler only)
zip lambda_alphavantage.zip lambda_function.py
```

Repeat for all functions:
```bash
cd lambdas/functions

for func in lambda_*; do
  cd "$func"
  zip "${func}.zip" lambda_function.py
  cd ..
done
```

#### 2.2 Create IAM Execution Role

Each Lambda function requires an execution role with permissions for:
- CloudWatch Logs (logging)
- S3 (read/write raw data)
- SSM Parameter Store (read secrets in production)

**Create Role Policy Document**

Create `lambda_execution_role_policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::tariff-lakehouse-bucket",
        "arn:aws:s3:::tariff-lakehouse-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters"
      ],
      "Resource": "arn:aws:ssm:us-east-1:*:parameter/tariff/prod/*"
    }
  ]
}
```

**Create the Role**

```bash
# Create trust policy for Lambda
cat > lambda_trust_policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
aws iam create-role \
  --role-name tariff-lambda-execution-role \
  --assume-role-policy-document file://lambda_trust_policy.json

# Attach custom policy
aws iam put-role-policy \
  --role-name tariff-lambda-execution-role \
  --policy-name tariff-lambda-permissions \
  --policy-document file://lambda_execution_role_policy.json
```

**Output:** Note the `Role.Arn` (e.g., `arn:aws:iam::123456789012:role/tariff-lambda-execution-role`).

#### 2.3 Deploy Lambda Functions

Deploy each function with the shared layer and environment variables.

**Example**: Deploy `lambda_alphavantage`

```bash
cd lambdas/functions/lambda_alphavantage

aws lambda create-function \
  --function-name tariff_alphavantage_lambda_function \
  --runtime python3.12 \
  --role arn:aws:iam::123456789012:role/tariff-lambda-execution-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://lambda_alphavantage.zip \
  --timeout 120 \
  --memory-size 512 \
  --environment Variables="{STAGE=prod}" \
  --layers arn:aws:lambda:us-east-1:123456789012:layer:tariff-ingestion-layer:1 \
  --region us-east-1
```

**Key Configuration:**
- `--timeout 120` – Max runtime of 2 minutes (120 seconds)
- `--memory-size 512` – 512 MB memory (adjust based on data volume)
- `--environment Variables="{STAGE=prod}"` – Triggers SSM Parameter Store usage for secrets
- `--layers` – Attaches the shared layer

**Deploy All Functions (Script)**

```bash
#!/bin/bash

LAYER_ARN="arn:aws:lambda:us-east-1:123456789012:layer:tariff-ingestion-layer:1"
ROLE_ARN="arn:aws:iam::123456789012:role/tariff-lambda-execution-role"
REGION="us-east-1"

FUNCTIONS=(
  "lambda_alphavantage:tariff_alphavantage_lambda_function"
  "lambda_imf_datamapper:tariff_imf_datamapper_lambda_function"
  "lambda_indian_fuel_api:tariff_indian_fuel_api_lambda_function"
  "lambda_indian_stocks_api:tariff_indian_stock_api_lambda_function"
  "lambda_metals_dev:tariff_metals_dev_lambda_function"
  "lambda_twelve_data:tariff_twelvedata_lambda_function"
  "lambda_financial_modeling_prep:tariff_fmp_lambda_function"
  "lambda_newsapi:tariff_newsapi_lambda_function"
)

cd lambdas/functions

for entry in "${FUNCTIONS[@]}"; do
  IFS=':' read -r func_dir func_name <<< "$entry"
  
  echo "Deploying $func_name..."
  
  cd "$func_dir"
  
  aws lambda create-function \
    --function-name "$func_name" \
    --runtime python3.12 \
    --role "$ROLE_ARN" \
    --handler lambda_function.lambda_handler \
    --zip-file "fileb://${func_dir}.zip" \
    --timeout 120 \
    --memory-size 512 \
    --environment Variables="{STAGE=prod}" \
    --layers "$LAYER_ARN" \
    --region "$REGION"
  
  cd ..
done

echo "All functions deployed successfully."
```

---

## Step 3: Configure SSM Parameter Store (Production)

In production (`STAGE=prod`), the `Config` class reads secrets from SSM Parameter Store.

**Parameter Naming Convention**

Parameters follow the path structure: `/tariff/<stage>/<parameter_name>`

**Required Parameters**

Store the following in SSM Parameter Store:

```bash
# S3 bucket for raw data
aws ssm put-parameter \
  --name /tariff/prod/AWS_S3_LAKEHOUSE_BUCKET \
  --value "tariff-lakehouse-bucket" \
  --type String \
  --region us-east-1

# AWS region
aws ssm put-parameter \
  --name /tariff/prod/AWS_REGION_NAME \
  --value "us-east-1" \
  --type String \
  --region us-east-1

# AlphaVantage API key
aws ssm put-parameter \
  --name /tariff/prod/ALPHAVANTAGE_API_KEY \
  --value "YOUR_API_KEY_HERE" \
  --type SecureString \
  --region us-east-1

# Indian Stock API key
aws ssm put-parameter \
  --name /tariff/prod/INDIAN_STOCK_API_KEY \
  --value "YOUR_API_KEY_HERE" \
  --type SecureString \
  --region us-east-1

# Indian Fuel API key
aws ssm put-parameter \
  --name /tariff/prod/INDIAN_FUEL_API_KEY \
  --value "YOUR_API_KEY_HERE" \
  --type SecureString \
  --region us-east-1

# Metals.dev API key
aws ssm put-parameter \
  --name /tariff/prod/METALS_DEV_API_KEY \
  --value "YOUR_API_KEY_HERE" \
  --type SecureString \
  --region us-east-1

# TwelveData API key
aws ssm put-parameter \
  --name /tariff/prod/TWELVE_DATA_API_KEY \
  --value "YOUR_API_KEY_HERE" \
  --type SecureString \
  --region us-east-1

# Financial Modeling Prep API key
aws ssm put-parameter \
  --name /tariff/prod/FMP_API_KEY \
  --value "YOUR_API_KEY_HERE" \
  --type SecureString \
  --region us-east-1

# NewsAPI key
aws ssm put-parameter \
  --name /tariff/prod/NEWSAPI_KEY \
  --value "YOUR_API_KEY_HERE" \
  --type SecureString \
  --region us-east-1
```

**Note:** Use `SecureString` type for API keys to enable encryption at rest.

---

## Step 4: Update Existing Functions (Post-Initial Deployment)

#### 4.1 Update Function Code

```bash
cd lambdas/functions/lambda_alphavantage

# Re-zip handler
zip lambda_alphavantage.zip lambda_function.py

# Update function code
aws lambda update-function-code \
  --function-name tariff_alphavantage_lambda_function \
  --zip-file fileb://lambda_alphavantage.zip \
  --region us-east-1
```

#### 4.2 Update Layer

When you modify shared code or dependencies:

```bash
cd lambdas/layer

# Rebuild layer
pip install -r requirements.txt \
  --target python/lib/python3.12/site-packages/ \
  --platform manylinux2014_x86_64 \
  --only-binary=:all: \
  --python-version 3.12 \
  --upgrade

# Re-zip layer
zip -r tariff_lambda_layer.zip python/ -x "*.pyc" -x "*__pycache__*"

# Publish new layer version
aws lambda publish-layer-version \
  --layer-name tariff-ingestion-layer \
  --description "Updated shared ingestion utilities" \
  --zip-file fileb://tariff_lambda_layer.zip \
  --compatible-runtimes python3.12 \
  --region us-east-1
```

**Output:** Note the new `LayerVersionArn` (version will increment, e.g., `:2`, `:3`).

#### 4.3 Update Functions to Use New Layer Version

```bash
NEW_LAYER_ARN="arn:aws:lambda:us-east-1:123456789012:layer:tariff-ingestion-layer:2"

# Update all functions
for func_name in \
  tariff_alphavantage_lambda_function \
  tariff_imf_datamapper_lambda_function \
  tariff_indian_fuel_api_lambda_function \
  tariff_indian_stock_api_lambda_function \
  tariff_metals_dev_lambda_function \
  tariff_twelvedata_lambda_function \
  tariff_fmp_lambda_function \
  tariff_newsapi_lambda_function
do
  aws lambda update-function-configuration \
    --function-name "$func_name" \
    --layers "$NEW_LAYER_ARN" \
    --region us-east-1
done
```

---

## Step 5: Test Lambda Functions

#### 5.1 Test via AWS Console

1. Navigate to AWS Lambda Console
2. Select function (e.g., `tariff_alphavantage_lambda_function`)
3. Click **Test** tab
4. Create test event with sample payload:

```json
{
  "source": "alphavantage",
  "domain": "american_markets",
  "dataset": "time_series_daily",
  "api_params": {
    "dataset": "TIME_SERIES_DAILY",
    "params": {
      "symbols": ["AAPL", "MSFT"],
      "outputsize": "compact"
    }
  },
  "triggered_at": "2025-02-02T10:00:00.000Z"
}
```

5. Click **Test** and verify response:

```json
{
  "status": "SUCCESS",
  "lambda_exec_ts": "2025-02-02T10:00:15.123Z",
  "domain": "american_markets",
  "source": "alphavantage",
  "dataset": "time_series_daily",
  "keys": [
    "raw/american_markets/alphavantage/time_series_daily/2025-02-02/time_series_daily_AAPL_20250202T100015.jsonl"
  ],
  "record_count": 100,
  "ingested_at": "2025-02-02T10:00:15.123Z"
}
```

#### 5.2 Test via AWS CLI

```bash
aws lambda invoke \
  --function-name tariff_alphavantage_lambda_function \
  --payload '{"source":"alphavantage","domain":"american_markets","dataset":"time_series_daily","api_params":{"dataset":"TIME_SERIES_DAILY","params":{"symbols":["AAPL"],"outputsize":"compact"}},"triggered_at":"2025-02-02T10:00:00.000Z"}' \
  --region us-east-1 \
  response.json

# View response
cat response.json | jq
```

#### 5.3 Verify S3 Output

```bash
aws s3 ls s3://tariff-lakehouse-bucket/raw/american_markets/alphavantage/time_series_daily/2025-02-02/ --recursive

# Download and inspect
aws s3 cp s3://tariff-lakehouse-bucket/raw/american_markets/alphavantage/time_series_daily/2025-02-02/time_series_daily_AAPL_20250202T100015.jsonl - | head -5
```

---

## Lambda Configuration Summary

#### Environment Variables

| Variable | Value  | Purpose |
|----------|--------|---------|
| `STAGE`  | `prod` | Switches config loading from `.env` to SSM Parameter Store |

#### Runtime Configuration

| Setting | Value | Rationale |
|---------|-------|-----------|
| Runtime | `python3.12` | Latest stable Python version on Lambda |
| Timeout | `120` seconds | 2 minutes max runtime for API calls + S3 writes |
| Memory  | `512` MB | Sufficient for JSON parsing and S3 operations |
| Handler | `lambda_function.lambda_handler` | Entry point function in each handler file |

#### IAM Role Permissions

The execution role requires:

- **CloudWatch Logs** – `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`
- **S3** – `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` (scope: `tariff-lakehouse-bucket/*`)
- **SSM Parameter Store** – `ssm:GetParameter`, `ssm:GetParameters` (scope: `/tariff/prod/*`)

---

## Troubleshooting

#### Issue: `ModuleNotFoundError: No module named 'src'`

**Cause:** Layer not attached or layer structure incorrect.

**Solution:**
1. Verify layer ARN is correct: `aws lambda get-function --function-name <name> --query Configuration.Layers`
2. Verify layer zip contains `python/` at root: `unzip -l tariff_lambda_layer.zip | head`
3. Re-package layer with correct structure (see Step 1.3)

#### Issue: `ParameterNotFound` when reading SSM

**Cause:** `STAGE=prod` but parameter doesn't exist in SSM.

**Solution:**
1. Verify parameter exists: `aws ssm get-parameter --name /tariff/prod/ALPHAVANTAGE_API_KEY`
2. Create missing parameter (see Step 3)
3. Verify IAM role has `ssm:GetParameter` permission

#### Issue: `AccessDenied` when writing to S3

**Cause:** IAM role lacks S3 permissions.

**Solution:**
1. Verify role policy includes S3 permissions (see Step 2.2)
2. Update role policy:
```bash
aws iam put-role-policy \
  --role-name tariff-lambda-execution-role \
  --policy-name tariff-lambda-permissions \
  --policy-document file://lambda_execution_role_policy.json
```

#### Issue: Lambda logs not appearing in CloudWatch

**Cause:** IAM role lacks CloudWatch Logs permissions.

**Solution:** Attach the AWS-managed policy `AWSLambdaBasicExecutionRole`:
```bash
aws iam attach-role-policy \
  --role-name tariff-lambda-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

---

## Monitoring and Observability

#### CloudWatch Logs

Each function creates a log group: `/aws/lambdas/<function_name>`

View logs:
```bash
aws logs tail /aws/lambdas/tariff_alphavantage_lambda_function --follow
```

#### CloudWatch Metrics

Key metrics to monitor:
- **Invocations** – Total function executions
- **Errors** – Failed invocations (Lambda errors, not application errors)
- **Duration** – Execution time (should be <120 seconds)
- **Throttles** – Rate limit hits (increase reserved concurrency if needed)

Create CloudWatch alarms:
```bash
aws cloudwatch put-metric-alarm \
  --alarm-name tariff-lambda-error-rate \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 5 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=FunctionName,Value=tariff_alphavantage_lambda_function
```

---

## Related Documentation

- [Lambda Developer Guide](https://docs.aws.amazon.com/lambdas/latest/dg/welcome.html)
- [Lambda Layers Best Practices](https://docs.aws.amazon.com/lambdas/latest/dg/configuration-layers.html)
- [SSM Parameter Store](https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html)
- [Airflow DAG Configuration](../airflow/dags/tariff_dags_config.json)
- [Data Contracts](../DataContracts.md)