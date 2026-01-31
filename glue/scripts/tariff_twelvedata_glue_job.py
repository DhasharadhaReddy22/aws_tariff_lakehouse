"""
To test the glue job locally, refer to the following spark submit command in terminal (making appropriate changes),
spark-submit \
  /home/hadoop/workspace/scripts/tariff_alphavantage_commodities_glue_job.py \
  --DOMAIN commodities \
  --SOURCE twelvedata \
  --DATASET exchange_rates \
  --KEYS '[
    "bronze/.../twelvedata_timeseries_20260126_043955.jsonl"
  ]' \
  --RECORD_COUNT 200 \
  --INGESTED_AT 2026-01-08T06:07:17.483178+00:00 \
  --DAG_ID dag_ig \
  --RUN_ID manual_run \
  > /home/hadoop/workspace/scripts/logs/alphavanatge_commodities_spark_log.log 2>&1

and view the live spark job logs in the alphavanatge_murican_markets_spark_log.log file or,
have the spark UI enabled in the /usr/lib/spark/conf/spark-defaults.conf
"""

import sys
import json
import logging
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, sha2, date_format,
    concat_ws, current_timestamp, row_number
)
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("twelvedata-fx-gold-job")

args = getResolvedOptions(
    sys.argv,
    [
        "DOMAIN",
        "SOURCE",
        "DATASET",
        "KEYS",
        "DAG_ID",
        "RUN_ID",
    ],
)

DOMAIN = args["DOMAIN"]
SOURCE = args["SOURCE"]
DATASET = args["DATASET"]
KEYS = json.loads(args["KEYS"])
DAG_ID = args["DAG_ID"]
RUN_ID = args["RUN_ID"]

# -----------------------------------------------------------------------------
# Spark / Iceberg setup

LAKEHOUSE_BUCKET = "s3://dummy-lakehouse"
GLUE_CATALOG = "glue_catalog"

spark = (
    SparkSession.builder
    .config(f"spark.sql.catalog.{GLUE_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.warehouse", LAKEHOUSE_BUCKET)
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .getOrCreate()
)

logger.info(f"Spark session started with catalog={GLUE_CATALOG}")

# -----------------------------------------------------------------------------
# Catalog objects

SILVER_DB = "silver"
GOLD_DB = "gold"

SILVER_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_clean"
GOLD_FACT_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_fact"

SILVER_FQN = f"{GLUE_CATALOG}.{SILVER_DB}.{SILVER_TABLE}"
GOLD_FACT_FQN = f"{GLUE_CATALOG}.{GOLD_DB}.{GOLD_FACT_TABLE}"

BRONZE_PATHS = [f"{LAKEHOUSE_BUCKET}/{k}" for k in KEYS]

# -----------------------------------------------------------------------------
# Bronze Layer

bronze_df = spark.read.option("mode", "FAILFAST").json(BRONZE_PATHS)

logger.info("Running Bronze validations...")

if bronze_df.rdd.isEmpty():
    raise RuntimeError("Bronze validation failed: no records found")

required_cols = {
    "symbol",
    "interval",
    "currency_base",
    "currency_quote",
    "market_time",
    "open",
    "high",
    "low",
    "close",
    "source",
    "dataset",
    "ingested_at"
}

missing = required_cols - set(bronze_df.columns)
if missing:
    raise RuntimeError(f"Bronze validation failed: missing columns {missing}")

logger.info("Bronze validations passed")

# -----------------------------------------------------------------------------
# Silver Layer (Append-only with conditional full-table dedup)

logger.info("Casting FX fields")

silver_incoming = (
    bronze_df
    .withColumn("symbol", col("symbol"))
    .withColumn("interval", col("interval"))
    .withColumn("currency_base", col("currency_base"))
    .withColumn("currency_quote", col("currency_quote"))
    .withColumn("market_time", to_timestamp(col("market_time")))
    .withColumn("year_month", date_format(col("market_time"), "yyyy-MM"))

    .withColumn("open", col("open").cast("double"))
    .withColumn("high", col("high").cast("double"))
    .withColumn("low", col("low").cast("double"))
    .withColumn("close", col("close").cast("double"))

    .withColumn("source", col("source"))
    .withColumn("dataset", col("dataset"))
    .withColumn("request_url", col("request_url"))

    .withColumn("received_at", to_timestamp(col("received_at")))
    .withColumn("ingested_at", to_timestamp(col("ingested_at")))

    .withColumn("dag_id", lit(DAG_ID))
    .withColumn("run_id", lit(RUN_ID))
    .withColumn("processed_at", current_timestamp())
)

logger.info("Creating Silver FX table if not exists")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_FQN} (
  symbol STRING,
  interval STRING,
  currency_base STRING,
  currency_quote STRING,
  market_time TIMESTAMP,
  year_month STRING,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  source STRING,
  dataset STRING,
  request_url STRING,
  received_at TIMESTAMP,
  ingested_at TIMESTAMP,
  dag_id STRING,
  run_id STRING,
  processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (year_month)
""")

logger.info("Appending incoming records into Silver FX table")

(
    silver_incoming
    .write
    .format("iceberg")
    .mode("append")
    .saveAsTable(SILVER_FQN)
)

logger.info("Silver FX append completed")

# -----------------------------------------------------------------------------
# Conditional Silver Deduplication (full-table)

logger.info("Checking for duplicate business keys in Silver FX table")

dup_keys_df = spark.sql(f"""
SELECT symbol, market_time, COUNT(*) AS cnt
FROM {SILVER_FQN}
GROUP BY symbol, market_time
HAVING COUNT(*) > 1
""")

dup_key_count = dup_keys_df.count()

if dup_key_count > 0:
    logger.info(
        f"Detected {dup_key_count} duplicate business keys in Silver FX table. "
        f"Running full-table deduplication."
    )

    silver_full = spark.table(SILVER_FQN)

    w_fix = Window.partitionBy("symbol", "market_time").orderBy(col("ingested_at").desc())

    silver_dedup_full = (
        silver_full
        .withColumn("rn", row_number().over(w_fix))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    logger.info("Overwriting Silver FX table with deduplicated snapshot")

    (
        silver_dedup_full
        .write
        .format("iceberg")
        .mode("overwrite")
        .option("overwrite-mode", "dynamic")
        .saveAsTable(SILVER_FQN)
    )

    logger.info(
        f"Silver FX deduplication completed. "
        f"Removed duplicates for {dup_key_count} business keys."
    )
else:
    logger.info("No duplicate business keys found in Silver FX table. Skipping deduplication.")

# -----------------------------------------------------------------------------
# Gold Layer (Append-only with dedup)

logger.info("Creating Gold FX fact table if not exists")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FACT_FQN} (
  symbol STRING,
  interval STRING,
  currency_base STRING,
  currency_quote STRING,
  market_time TIMESTAMP,
  year_month STRING,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (year_month)
""")

logger.info("Preparing deduplicated snapshot for Gold FX table")

silver_for_gold = spark.table(SILVER_FQN)

w_gold = Window.partitionBy("symbol", "market_time").orderBy(col("ingested_at").desc())

gold_dedup = (
    silver_for_gold
    .withColumn("rn", row_number().over(w_gold))
    .filter(col("rn") == 1)
    .drop("rn")
)

logger.info("Appending deduplicated records into Gold FX table")

(
    gold_dedup
    .select(
        "symbol",
        "interval",
        "currency_base",
        "currency_quote",
        "market_time",
        "year_month",
        "open",
        "high",
        "low",
        "close",
        "processed_at",
    )
    .write
    .format("iceberg")
    .mode("append")
    .saveAsTable(GOLD_FACT_FQN)
)

logger.info("Gold FX append completed")
logger.info("FX Glue job finished successfully")