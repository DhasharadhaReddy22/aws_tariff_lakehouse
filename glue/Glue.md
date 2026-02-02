# Glue-Spark Jobs

This README covers the local development and deployment of **Glue-Spark** scripts for transformation jobs in the AWS Tariff Lakehouse pipeline.

---

## Architecture Overview

The Glue jobs implement a **medallion architecture** (Bronze → Silver → Gold) using Apache Iceberg for ACID-compliant transformations:

```
glue/
├── scripts/                                            # Production Glue job scripts
│   ├── tariff_alphavantage_american_markets_glue_job.py
│   ├── tariff_alphavantage_commodities_glue_job.py
│   ├── tariff_imf_datamapper_glue_job.py
│   ├── tariff_metals_dev_glue_job.py
│   ├── tariff_twelvedata_glue_job.py
│   └── test_glue_job.py                                # Testing sandbox
├── notebooks/                                          # Jupyter notebooks for interactive dev
│   └── glue_local_ex.ipynb
└── Dockerfile.livy_jupyter                             # Local Glue development environment
```

#### Transformation Layers

- **Bronze (Raw)** – Ingested JSONL files from Lambda (stored in S3)
- **Silver (Clean)** – Type-cast, deduplicated, SCD Type 2 tracking
- **Gold (Analytics)** – Business-ready fact tables (OBTs) and derived metrics

#### Prerequisites

Before proceeding, ensure you have:

- Docker installed (for local development and testing)
- AWS CLI configured with appropriate credentials
- S3 bucket containing Bronze layer data (e.g., `tariff-lakehouse-bucket`)
- IAM permissions to create Glue jobs, databases, and tables
- Access to the AWS Glue Data Catalog

---

## Part 1: Local Testing with Docker

Local testing uses AWS Glue Docker images to replicate the production environment.

#### 1.1 Build the Local Glue Development Environment

The provided `Dockerfile.livy_jupyter` sets up an Image for Glue environment with:
- AWS Glue libraries (version 5)
- Apache Spark 3.x
- Apache Iceberg support
- JupyterLab + Apache Livy (for interactive notebooks)

**Build the Docker Image**

```bash
cd glue/

docker build -t glue-local:latest -f Dockerfile.livy_jupyter .
```

**Build time:** ~5-10 minutes (downloads Glue base image, Livy, installs Python packages)

#### 1.2 Run the Container

Start the container with workspace volume and other mount as mentioned in the [docker-compose.yml](../docker-compose.yml) file.

**Port Mappings:**
- `8888` – JupyterLab (optional, for notebook-based development)
- `8998` – Apache Livy server (backend for Jupyter kernels)
- `4040` – Spark UI (for monitoring job execution)

**Volume Mount:**
- Maps local `glue/` directory to `/home/hadoop/workspace` in container
- Allows editing scripts locally while testing in container

**Environment Variables:**
- Provides AWS credentials for S3 and Glue Catalog access through AWS CLI profile
- The required datalake table format.
- Disable SSL connection.

#### 1.3 Access the Container Bash

In a new terminal, attach to the running container:

```bash
docker exec -it name_of_container bash
```

You should now be inside the container at `/home/hadoop`.

---

## Part 2: Local Testing with spark-submit

#### 2.1 Navigate to the Scripts Directory

```bash
cd /home/hadoop/workspace/scripts
```

#### 2.2 Prepare Test Data

Ensure your S3 bucket contains Bronze layer JSONL files. You can verify with:

```bash
aws s3 ls s3://tariff-lakehouse-bucket/bronze/american_markets/source=alphavantage/dataset=time_series_daily/ingestion_date=2026-01-27/ --recursive
```

**Expected output:**
```
2026-01-27 07:22:53      43107 bronze/american_markets/source=alphavantage/dataset=time_series_daily/ingestion_date=2026-01-27/time_series_daily_MSFT_20260127_072251.jsonl
2026-01-27 07:25:07      43107 bronze/american_markets/source=alphavantage/dataset=time_series_daily/ingestion_date=2026-01-27/time_series_daily_MSFT_20260127_072506.jsonl
```

#### 2.3 Run a Glue Job with spark-submit

Example: Testing `tariff_alphavantage_american_markets_glue_job.py`

```bash
spark-submit \
  /home/hadoop/workspace/scripts/test_glue_job-2.py \
  --DOMAIN energy \
  --SOURCE alphavantage \
  --DATASET time_series_daily \
  --KEYS '[
    "bronze/american_markets/source=alphavantage/dataset=time_series_daily/ingestion_date=2026-01-27/time_series_daily_MSFT_20260127_072251.jsonl",
    "bronze/american_markets/source=alphavantage/dataset=time_series_daily/ingestion_date=2026-01-27/time_series_daily_MSFT_20260127_072506.jsonl"
  ]' \
  --RECORD_COUNT 200 \
  --INGESTED_AT 2025-12-19T18:58:17.203499+00:00 \
  --DAG_ID dag_ig \
  --RUN_ID manual_run \
  > /home/hadoop/workspace/scripts/logs/alphavanatge_murican_markets_spark_log.log 2>&1
```

**Script Arguments (Passed by Airflow in Production):**

| Argument | Example Value | Description |
|----------|---------------|-------------|
| `--DOMAIN` | `american_markets` | Business domain from DAG config |
| `--SOURCE` | `alphavantage` | Data source identifier |
| `--DATASET` | `time_series_daily` | Dataset name |
| `--KEYS` | JSON array of S3 keys | Bronze files to process (from Lambda response) |
| `--RECORD_COUNT` | `200` | Expected record count (validation) |
| `--INGESTED_AT` | ISO 8601 timestamp | Original Lambda ingestion time |
| `--DAG_ID` | `alphavantage_american_markets_dag` | Airflow DAG identifier (lineage) |
| `--RUN_ID` | `manual__2025-02-02...` | Airflow run identifier (lineage) |

**Note:** `--datalake-formats iceberg` is passed by default in the Airflow DAG but not required for local testing (Iceberg is configured directly in Spark session).

#### 2.4 Monitor the Job

**Option A: View Logs in Real-Time**

```bash
tail -f /home/hadoop/workspace/scripts/logs/american_markets_spark_log.log
```

**Option B: Open Spark UI**

In your browser, navigate to: `http://localhost:4040`

The Spark UI shows:
- Job progress (stages, tasks)
- SQL queries executed (Iceberg MERGE operations)
- DAG visualization
- Executor metrics

#### 2.5 Verify Output in Glue Catalog

After successful execution, check that Iceberg tables were created:

```bash
# List databases
aws glue get-databases --region us-east-1

# List tables in Silver database
aws glue get-tables --database-name silver --region us-east-1

# List tables in Gold database
aws glue get-tables --database-name gold --region us-east-1
```

**Expected tables:**
- `silver.american_markets_alphavantage_time_series_daily_clean`
- `gold.american_markets_alphavantage_time_series_daily_fact`
- `gold.american_markets_alphavantage_time_series_daily_metrics`

#### 2.6 Query the Data via Athena

```sql
-- Query Silver (SCD Type 2)
SELECT symbol, trade_date, close, is_current, effective_from, effective_to
FROM silver.american_markets_alphavantage_time_series_daily_clean
WHERE symbol = 'AAPL'
ORDER BY trade_date DESC
LIMIT 10;

-- Query Gold Fact
SELECT symbol, trade_date, close, volume
FROM gold.american_markets_alphavantage_time_series_daily_fact
WHERE symbol = 'AAPL'
ORDER BY trade_date DESC
LIMIT 10;

-- Query Gold Metrics
SELECT symbol, trade_date, close, ma_5_close, ma_20_close, volatility_20
FROM gold.american_markets_alphavantage_time_series_daily_metrics
WHERE symbol = 'AAPL'
ORDER BY trade_date DESC
LIMIT 10;
```

---

## Part 3: Deploy Glue Jobs to AWS

Once local testing is complete, deploy the scripts to AWS Glue.

#### 3.1 Upload Scripts to S3

Glue jobs require scripts to be stored in S3.

```bash
# Create S3 prefix for Glue scripts
aws s3 mb s3://tariff-lakehouse-bucket/glue-scripts/

# Upload all job scripts
cd glue/scripts/

for script in tariff_*.py; do
  aws s3 cp "$script" s3://tariff-lakehouse-bucket/glue-scripts/ --region us-east-1
done
```

**Verify upload:**
```bash
aws s3 ls s3://tariff-lakehouse-bucket/glue-scripts/
```

#### 3.2 Create IAM Role for Glue

Glue jobs require an execution role with permissions for:
- S3 (read Bronze, write Silver/Gold)
- Glue Data Catalog (create/update databases and tables)
- CloudWatch Logs (write job logs)

**Create Role Policy Document**

Create `glue_execution_role_policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
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
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition",
        "glue:BatchUpdatePartition"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:*:catalog",
        "arn:aws:glue:us-east-1:*:database/silver",
        "arn:aws:glue:us-east-1:*:database/gold",
        "arn:aws:glue:us-east-1:*:table/silver/*",
        "arn:aws:glue:us-east-1:*:table/gold/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:/aws-glue/*"
    }
  ]
}
```

**Create the Role**

```bash
# Create trust policy for Glue
cat > glue_trust_policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
aws iam create-role \
  --role-name tariff-glue-execution-role \
  --assume-role-policy-document file://glue_trust_policy.json

# Attach custom policy
aws iam put-role-policy \
  --role-name tariff-glue-execution-role \
  --policy-name tariff-glue-permissions \
  --policy-document file://glue_execution_role_policy.json
```

**Output:** Note the `Role.Arn` (e.g., `arn:aws:iam::123456789012:role/tariff-glue-execution-role`).

#### 3.3 Create Glue Databases

Create `silver` and `gold` databases in the Glue Data Catalog:

```bash
# Create Silver database
aws glue create-database \
  --database-input '{
    "Name": "silver",
    "Description": "Clean, type-cast, and deduplicated data with SCD Type 2 tracking"
  }' \
  --region us-east-1

# Create Gold database
aws glue create-database \
  --database-input '{
    "Name": "gold",
    "Description": "Business-ready fact tables and derived analytical metrics"
  }' \
  --region us-east-1
```

**Verify:**
```bash
aws glue get-databases --region us-east-1
```

#### 3.4 Create Glue Jobs

Create a Glue job for each transformation script.

**Example: Create `tariff_alphavantage_american_markets_glue_job`**

```bash
aws glue create-job \
  --name tariff_alphavantage_american_markets_glue_job \
  --role arn:aws:iam::123456789012:role/tariff-glue-execution-role \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://tariff-lakehouse-bucket/glue-scripts/tariff_alphavantage_american_markets_glue_job.py",
    "PythonVersion": "3"
  }' \
  --glue-version "4.0" \
  --max-retries 0 \
  --timeout 5 \
  --default-arguments '{
    "--job-language": "python",
    "--enable-metrics": "true",
    "--enable-spark-ui": "true",
    "--spark-event-logs-path": "s3://tariff-lakehouse-bucket/glue-spark-logs/",
    "--enable-job-insights": "true",
    "--enable-glue-datacatalog": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--datalake-formats": "iceberg"
  }' \
  --region us-east-1
```

**Key Configuration:**

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `--role` | IAM role ARN | Execution permissions |
| `--command.ScriptLocation` | S3 URI | Path to Python script |
| `--glue-version` | `4.0` | Glue version (includes Spark 3.3, Iceberg support) |
| `--timeout` | `5` | Max runtime of 5 minutes (300 seconds) |
| `--max-retries` | `0` | No automatic retries (controlled by Airflow) |
| `--datalake-formats` | `iceberg` | Enables Apache Iceberg support |

**Default Arguments (Metadata & Logging):**
- `--enable-metrics` – Publishes CloudWatch metrics
- `--enable-spark-ui` – Enables Spark UI (stored in S3)
- `--spark-event-logs-path` – S3 path for Spark history
- `--enable-glue-datacatalog` – Uses Glue Catalog as Hive metastore
- `--enable-continuous-cloudwatch-log` – Real-time log streaming

**Create All Glue Jobs (Batch Script)**

```bash
#!/bin/bash

ROLE_ARN="arn:aws:iam::123456789012:role/tariff-glue-execution-role"
SCRIPT_LOCATION_PREFIX="s3://tariff-lakehouse-bucket/glue-scripts"
REGION="us-east-1"

JOBS=(
  "tariff_alphavantage_american_markets_glue_job"
  "tariff_alphavantage_commodities_glue_job"
  "tariff_imf_datamapper_glue_job"
  "tariff_metals_dev_glue_job"
  "tariff_twelvedata_glue_job"
)

for job_name in "${JOBS[@]}"; do
  echo "Creating Glue job: $job_name..."
  
  aws glue create-job \
    --name "$job_name" \
    --role "$ROLE_ARN" \
    --command "{
      \"Name\": \"glueetl\",
      \"ScriptLocation\": \"${SCRIPT_LOCATION_PREFIX}/${job_name}.py\",
      \"PythonVersion\": \"3\"
    }" \
    --glue-version "4.0" \
    --max-retries 0 \
    --timeout 5 \
    --default-arguments '{
      "--job-language": "python",
      "--enable-metrics": "true",
      "--enable-spark-ui": "true",
      "--spark-event-logs-path": "s3://tariff-lakehouse-bucket/glue-spark-logs/",
      "--enable-job-insights": "true",
      "--enable-glue-datacatalog": "true",
      "--enable-continuous-cloudwatch-log": "true",
      "--datalake-formats": "iceberg"
    }' \
    --region "$REGION"
done

echo "All Glue jobs created successfully."
```

#### 3.5 Test a Glue Job Manually

Before integrating with Airflow, test a job manually:

```bash
aws glue start-job-run \
  --job-name tariff_alphavantage_american_markets_glue_job \
  --arguments '{
    "--DOMAIN": "american_markets",
    "--SOURCE": "alphavantage",
    "--DATASET": "time_series_daily",
    "--KEYS": "[\"raw/american_markets/alphavantage/time_series_daily/2025-02-02/time_series_daily_AAPL_20250202T100015.jsonl\",\"raw/american_markets/alphavantage/time_series_daily/2025-02-02/time_series_daily_MSFT_20250202T100015.jsonl\"]",
    "--RECORD_COUNT": "200",
    "--INGESTED_AT": "2025-02-02T10:00:15.123Z",
    "--DAG_ID": "manual_test",
    "--RUN_ID": "manual_run_001"
  }' \
  --region us-east-1
```

**Output:** Returns `JobRunId` (e.g., `jr_abc123`).

**Monitor the Job Run**

```bash
# Get job run status
aws glue get-job-run \
  --job-name tariff_alphavantage_american_markets_glue_job \
  --run-id jr_abc123 \
  --region us-east-1

# View CloudWatch logs
aws logs tail /aws-glue/jobs/output \
  --follow \
  --log-stream-name tariff_alphavantage_american_markets_glue_job
```

---

## Part 4: Update Glue Jobs

#### 4.1 Update Script

After modifying a script locally and testing with `spark-submit`:

```bash
# Upload updated script
aws s3 cp tariff_alphavantage_american_markets_glue_job.py \
  s3://tariff-lakehouse-bucket/glue-scripts/ \
  --region us-east-1
```

**Note:** Glue jobs automatically use the latest script version on each run (no job update required).

#### 4.2 Update Job Configuration

To change timeout, role, or other settings:

```bash
aws glue update-job \
  --job-name tariff_alphavantage_american_markets_glue_job \
  --job-update '{
    "Role": "arn:aws:iam::123456789012:role/tariff-glue-execution-role",
    "Command": {
      "Name": "glueetl",
      "ScriptLocation": "s3://tariff-lakehouse-bucket/glue-scripts/tariff_alphavantage_american_markets_glue_job.py",
      "PythonVersion": "3"
    },
    "Timeout": 5,
    "GlueVersion": "5.0"
  }' \
  --region us-east-1
```

---

## Glue Job Configuration Summary

#### Runtime Configuration

| Setting | Value | Rationale |
|---------|-------|-----------|
| Glue Version | `5.0` | Latest stable version with Spark 3.3 + Iceberg |
| Timeout | `5` minutes | Max runtime for transformation jobs |
| Max Retries | `0` | Retries managed by Airflow (not Glue) |
| Python Version | `3` | Python 3.9+ |

#### Required Arguments (Passed by Airflow)

| Argument | Source | Description |
|----------|--------|-------------|
| `--DOMAIN` | DAG config | Business domain |
| `--SOURCE` | DAG config | Data source identifier |
| `--DATASET` | DAG config | Dataset name |
| `--KEYS` | Lambda XCom | JSON array of Bronze S3 keys |
| `--RECORD_COUNT` | Lambda XCom | Expected record count |
| `--INGESTED_AT` | Lambda XCom | Lambda execution timestamp |
| `--DAG_ID` | Airflow metadata | DAG identifier (lineage) |
| `--RUN_ID` | Airflow metadata | Run identifier (lineage) |
| `--datalake-formats` | Airflow default | `iceberg` (ACID compliance) |

#### IAM Role Permissions

The execution role requires:

- **S3** – Read/write on `tariff-lakehouse-bucket/*`
- **Glue Catalog** – Create/update databases and tables (`silver`, `gold`)
- **CloudWatch Logs** – Write job logs to `/aws-glue/jobs/*`

---

## Monitoring and Observability

#### CloudWatch Logs

Each Glue job creates log streams in:
- `/aws-glue/jobs/output` – Standard output logs
- `/aws-glue/jobs/error` – Error logs

**View logs:**
```bash
aws logs tail /aws-glue/jobs/output --follow
```

#### CloudWatch Metrics

Key metrics to monitor:
- **glue.driver.aggregate.numCompletedStages** – Completed Spark stages
- **glue.driver.aggregate.numFailedTasks** – Failed Spark tasks
- **glue.ALL.jvm.heap.usage** – Memory utilization
- **glue.driver.ExecutorAllocationManager.executors.numberAllExecutors** – Active executors

**Create CloudWatch alarm:**
```bash
aws cloudwatch put-metric-alarm \
  --alarm-name tariff-glue-job-failures \
  --metric-name glue.driver.aggregate.numFailedTasks \
  --namespace Glue \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=JobName,Value=tariff_alphavantage_american_markets_glue_job
```

#### Spark UI (Post-Job Analysis)

After job completion, Spark UI is available in S3:

```bash
aws s3 ls s3://tariff-lakehouse-bucket/glue-spark-logs/ --recursive
```

Download and view locally using Spark History Server.

---

## Best Practices

#### 1. Test Locally Before Deploying

Always run `spark-submit` in the Docker container before deploying to Glue.

#### 2. Use Incremental Processing

Design jobs to process only new data:
- Bronze: Lambda provides exact keys
- Silver: MERGE handles updates efficiently
- Gold: Recompute only affected partitions

#### 3. Validate Data Quality

Each layer includes validation checks:
- Bronze: Schema presence, required columns
- Silver: Deduplication, SCD2 invariants
- Gold: Business rule validation

#### 4. Monitor Resource Usage

Use Spark UI to optimize:
- Shuffle partitions (`spark.sql.shuffle.partitions`)
- Executor memory (`spark.executor.memory`)
- Parallelism (`spark.default.parallelism`)

#### 5. Version Control Scripts

Store scripts in Git, use S3 as deployment target:
```bash
git commit -m "Update alphavantage glue job"
aws s3 cp script.py s3://tariff-lakehouse-bucket/glue-scripts/
```

---

## Related Documentation

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Apache Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-ddl/)
- [Glue Docker Images](https://github.com/awslabs/aws-glue-libs)
- [Data Contracts](../DataContracts.md.md)
- [Airflow DAG Configuration](../airflow/dags/tariff_dags_config.json)