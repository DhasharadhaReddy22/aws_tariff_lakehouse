# Data Contract & Integration Flow

This document defines the **data contracts** and **integration patterns** used in the AWS Tariff Lakehouse pipeline, covering the event flow from **Airflow → Lambda → Glue**.

---

## Overview

The pipeline follows a **configuration-driven ELT architecture** where:

* **Airflow** orchestrates ingestion schedules and manages workflow state
* **Lambda** performs raw data extraction from external APIs and writes to S3
* **Glue** transforms raw data into Bronze/Silver/Gold layers using Apache Iceberg

Each component communicates through **well-defined contracts** that ensure loose coupling, observability, and idempotency.

---

## Contract 1: Airflow → Lambda (Ingestion Request)

### Purpose
Airflow triggers Lambda functions with context-specific parameters for data ingestion.

### Contract Structure

```json
{
  "source": "string",              // Data source identifier (e.g., "alphavantage", "imf")
  "domain": "string",              // Business domain (e.g., "commodities", "macroeconomics")
  "dataset": "string",             // Specific dataset name (e.g., "time_series_daily")
  "api_params": {                  // Source-specific API parameters
    "dataset": "string",           // Optional: API endpoint/function identifier
    "params": {                    // Nested parameters for the API call
      "symbols": ["string"],       // Example: stock symbols
      "interval": "string",        // Example: time interval
      // ... other source-specific params
    }
  },
  "triggered_at": "ISO8601"        // UTC timestamp when DAG triggered
}
```

### Contract Guarantees

* `source`, `domain`, `dataset` are **always present** and non-empty
* `api_params` structure varies by source but maintains consistent nesting
* `triggered_at` provides audit trail and idempotency key basis
* Lambda function name is resolved from configuration, not passed in event

### Example Event (AlphaVantage Daily Stock)

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
  "triggered_at": "2025-02-01T10:30:00.000Z"
}
```

### Example Event (IMF Macroeconomics)

```json
{
  "source": "imf",
  "domain": "macroeconomics",
  "dataset": "indicators",
  "api_params": {
    "params": {
      "indicator_codes": ["BCA_NGDPD", "PCPIPCH", "NGDP_RPCH", "LUR"]
    }
  },
  "triggered_at": "2025-02-01T00:00:00.000Z"
}
```

---

## Contract 2: Lambda → Airflow (Ingestion Result via XCom)

### Purpose
Lambda returns ingestion metadata to Airflow, which stores it in **XCom** for downstream Glue job consumption.

### Contract Structure (Success Response)

```json
{
  "status": "SUCCESS",
  "lambda_exec_ts": "ISO8601",     // Lambda execution timestamp
  "domain": "string",               // Echoed from request
  "source": "string",               // Echoed from request
  "dataset": "string",              // Echoed from request
  "keys": ["string"],               // S3 object keys written (array)
  "record_count": "integer",        // Total records ingested
  "ingested_at": "ISO8601"          // Timestamp when data was written to S3
}
```

### Contract Structure (Failure Response)

```json
{
  "status": "FAILED",
  "lambda_exec_ts": "ISO8601",
  "error": "string",                // Human-readable error message
  "error_type": "string",           // Exception class name
  "retryable": "boolean"            // Whether Airflow should retry
}
```

### Contract Guarantees

* `status` is **always present** and must be either `"SUCCESS"` or `"FAILED"`
* Success responses **must include** `keys`, `record_count`, and `ingested_at`
* `keys` is always an array (even for single-file writes)
* All timestamps are in **ISO 8601 format with UTC timezone**
* `record_count = 0` is treated as an error condition by Airflow

### Example Success Response (Multi-File Write)

```json
{
  "status": "SUCCESS",
  "lambda_exec_ts": "2025-02-01T10:30:45.123Z",
  "domain": "american_markets",
  "source": "alphavantage",
  "dataset": "time_series_daily",
  "keys": [
    "raw/american_markets/alphavantage/time_series_daily/2025-02-01/time_series_daily_AAPL_20250201T103045.jsonl",
    "raw/american_markets/alphavantage/time_series_daily/2025-02-01/time_series_daily_MSFT_20250201T103045.jsonl"
  ],
  "record_count": 256,
  "ingested_at": "2025-02-01T10:30:45.123Z"
}
```

### Example Failure Response

```json
{
  "status": "FAILED",
  "lambda_exec_ts": "2025-02-01T10:30:45.123Z",
  "error": "API rate limit exceeded: 429 Too Many Requests",
  "error_type": "RateLimitError",
  "retryable": true
}
```

---

## Contract 3: Airflow → Glue (Transformation Job Arguments)

### Purpose
Airflow passes normalized ingestion metadata to Glue as job arguments for transformation processing.

### Contract Structure (Glue Script Arguments)

```bash
--DOMAIN "string"                 # Business domain
--SOURCE "string"                 # Data source identifier
--DATASET "string"                # Dataset name
--KEYS '["string"]'               # JSON array of S3 keys to process
--RECORD_COUNT "integer"          # Expected record count
--INGESTED_AT "ISO8601"           # Original ingestion timestamp
--DAG_ID "string"                 # Airflow DAG identifier
--RUN_ID "string"                 # Airflow run identifier
--datalake-formats "iceberg"      # Lakehouse format (fixed)
```

### Contract Guarantees

* All arguments are **passed as strings** (even integers and JSON)
* `KEYS` is serialized as JSON string and must be parsed by Glue
* `DOMAIN`, `SOURCE`, `DATASET` form the logical partition key
* `DAG_ID` and `RUN_ID` provide full lineage back to orchestration
* `datalake-formats` is always `"iceberg"` for ACID compliance

### Example Glue Job Arguments

```python
{
  "--DOMAIN": "american_markets",
  "--SOURCE": "alphavantage",
  "--DATASET": "time_series_daily",
  "--KEYS": '["raw/american_markets/alphavantage/time_series_daily/2025-02-01/time_series_daily_AAPL_20250201T103045.jsonl", "raw/american_markets/alphavantage/time_series_daily/2025-02-01/time_series_daily_MSFT_20250201T103045.jsonl"]',
  "--RECORD_COUNT": "256",
  "--INGESTED_AT": "2025-02-01T10:30:45.123Z",
  "--DAG_ID": "alphavantage_american_markets_dag",
  "--RUN_ID": "manual__2025-02-01T10:30:00+00:00",
  "--datalake-formats": "iceberg"
}
```

---

## Data Flow Architecture

The complete data flow follows this sequence:

```
┌─────────────────────────────────────────────────────────────────────┐
│                          AIRFLOW (Orchestrator)                      │
│                                                                      │
│  1. Load DAG config from tariff_dags_config.json                    │
│  2. Build Lambda event payload (Contract 1)                         │
│  3. Invoke Lambda via LambdaInvokeFunctionOperator                  │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         LAMBDA (Ingestion)                           │
│                                                                      │
│  1. Receive event (source, domain, dataset, api_params)             │
│  2. Call external API with parameters                               │
│  3. Transform API response → JSONL records                          │
│  4. Write JSONL files to S3 (raw/ prefix)                           │
│  5. Return metadata response (Contract 2)                           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    AIRFLOW (XCom Processing)                         │
│                                                                      │
│  1. Capture Lambda response in XCom                                 │
│  2. Validate response status and record_count                       │
│  3. Extract keys, timestamps, metadata                              │
│  4. Build Glue job arguments (Contract 3)                           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          GLUE (Transformation)                       │
│                                                                      │
│  1. Parse script arguments (KEYS, DOMAIN, etc.)                     │
│  2. Read JSONL files from S3 using KEYS                             │
│  3. Apply schema validation and transformations                     │
│  4. Write to Bronze/Silver/Gold Iceberg tables                      │
│  5. Register table metadata in Glue Catalog                         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Configuration-Driven Design

All DAGs are **dynamically generated** from `tariff_dags_config.json`:

### Configuration Schema

```json
{
  "<pipeline_key>": {
    "dag_id": "string",                      // Unique DAG identifier
    "schedule": "string",                    // Cron expression or preset
    "source": "string",                      // Maps to Lambda routing
    "domain": "string",                      // Business domain
    "dataset": "string",                     // Dataset identifier
    "lambda_function_name": "string",        // AWS Lambda function name
    "glue_job": "string",                    // AWS Glue job name
    "tags": ["string"],                      // Searchability tags
    "api_params": {                          // Source-specific parameters
      "dataset": "string",                   // Optional
      "params": {                            // Nested params
        // ... varies by source
      }
    }
  }
}
```

### Design Benefits

* **Single source of truth** for pipeline definitions
* **DRY principle** – one DAG factory function, N pipelines
* **Easy to add new sources** – just add config entry
* **Type safety** via JSON schema validation (future enhancement)

---

## Idempotency & Deduplication Strategy

### Ingestion Idempotency (Lambda)

* S3 object keys include **ingestion timestamp** in the filename
* Multiple runs of the same ingestion create distinct files
* Downstream Glue jobs handle deduplication based on business keys

### Processing Idempotency (Glue)

* Glue jobs receive **explicit S3 keys** to process (not date ranges)
* Each Glue run processes only the files from its triggering Lambda execution
* Apache Iceberg provides **ACID transactions** for upserts/merges

### Deduplication Strategy

* Raw layer (Bronze) preserves all ingested records
* Silver layer applies business logic deduplication:
  * For time-series data: dedupe on `(symbol, date)`
  * For snapshot data: dedupe on natural key + latest `ingested_at`
* Gold layer aggregates deduplicated Silver data

---

## Error Handling & Observability

### Lambda Error Handling

* API failures return `{"status": "FAILED", "retryable": true}`
* Airflow retries based on `retryable` flag (max 1 retry, 5min delay)
* All errors logged with full context (event, traceback, response)

### Airflow Validation

* `extract_lambda_result` task validates:
  * `status == "SUCCESS"`
  * `record_count > 0`
  * Required fields present (`keys`, `ingested_at`, etc.)
* Invalid responses raise `ValueError` or `RuntimeError` to fail the DAG

### Glue Observability

* Job arguments provide full lineage (`DAG_ID`, `RUN_ID`, `KEYS`)
* Glue logs record processing metrics per file
* Failures do not corrupt existing Iceberg tables (transaction rollback)

---

## Raw Data Storage Convention

### S3 Key Structure

```
s3://<bucket>/raw/<domain>/<source>/<dataset>/<date>/<filename>.jsonl
```

### Components

* `<domain>` – Business domain (e.g., `commodities`, `american_markets`)
* `<source>` – Data source identifier (e.g., `alphavantage`, `imf`)
* `<dataset>` – Dataset name (e.g., `time_series_daily`, `indicators`)
* `<date>` – Ingestion date in `YYYY-MM-DD` format
* `<filename>` – Constructed as `{dataset}_{entity}_{timestamp}.jsonl`

### Example Keys

```
raw/american_markets/alphavantage/time_series_daily/2025-02-01/time_series_daily_AAPL_20250201T103045.jsonl
raw/commodities/alphavantage/commodity_prices/2025-02-01/commodity_prices_WTI_20250201T103045.jsonl
raw/macroeconomics/imf/indicators/2025-02-01/indicators_20250201T103045.jsonl
```

### Design Benefits

* **Partitioning** by domain/source/dataset enables targeted reads
* **Date-based paths** support incremental processing patterns
* **Entity fanout** (e.g., per-symbol files) reduces Glue job parallelism contention

---

## Metadata Fields in Raw Data

Every ingested record includes standardized metadata:

```json
{
  // Business fields
  "symbol": "AAPL",
  "trade_date": "2025-01-31",
  "open": 150.25,
  // ... other business fields

  // Ingestion metadata
  "source": "alphavantage",
  "dataset": "time_series_daily",
  "ingested_at": "2025-02-01T10:30:45.123Z",

  // Transport metadata
  "request_url": "https://www.alphavantage.co/query?function=...",
  "received_at": "2025-02-01T10:30:42.456Z"
}
```

### Metadata Guarantees

* `ingested_at` – Timestamp when Lambda wrote to S3 (ISO 8601 UTC)
* `received_at` – Timestamp when Lambda received API response (ISO 8601 UTC)
* `source` and `dataset` – Echo of Airflow event for data lineage
* `request_url` – Full API URL for audit and debugging

---

## Contract Evolution & Versioning

### Current Assumptions

* Lambda response structure is **stable** for all sources
* Glue script argument names are **fixed**
* S3 key structure is **immutable** once written

### Future Considerations

* **Contract versioning** – Add `contract_version` field to events
* **Schema registry** – Validate API responses against registered schemas
* **Backward compatibility** – Support multiple contract versions simultaneously

### Trade-offs

* Increased complexity in version handling
* Migration tooling for legacy data
* Stronger guarantees against breaking changes

---

## Contract Validation Checklist

Before deploying a new pipeline, verify:

- [ ] `tariff_dags_config.json` entry is valid and complete
- [ ] Lambda function accepts required event fields (`source`, `domain`, `dataset`, `api_params`)
- [ ] Lambda returns all required success fields (`status`, `keys`, `record_count`, `ingested_at`)
- [ ] Airflow `extract_lambda_result` successfully parses Lambda response
- [ ] Glue job accepts all required arguments (`DOMAIN`, `SOURCE`, `DATASET`, `KEYS`, etc.)
- [ ] S3 keys follow `raw/<domain>/<source>/<dataset>/<date>/<filename>.jsonl` pattern
- [ ] Raw data includes standardized metadata fields
- [ ] End-to-end lineage is traceable (`triggered_at` → `lambda_exec_ts` → `ingested_at` → Glue logs)

---

## Related Documentation

* [Airflow DAG Configuration](tariff_dags_config.json)
* [Dynamic DAG Factory](tariff_dynamic_dag.py)
* [Lambda Handler Implementation](lambda_function.py)
* [AlphaVantage Ingestion Module](alphavantage.py)
* [Raw Data Storage Conventions](#raw-data-storage-convention)
* [Glue Transformation Jobs](../glue/) (reference external documentation)