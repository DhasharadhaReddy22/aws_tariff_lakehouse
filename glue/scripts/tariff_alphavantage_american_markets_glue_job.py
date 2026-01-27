"""
To test the glue job locally, refer to the following spark submit command in terminal (making appropriate changes),
spark-submit \
  /home/hadoop/workspace/scripts/test_glue_job-2.py \
  --DOMAIN energy \
  --SOURCE alphavantage \
  --DATASET time_series_daily \
  --KEYS '[
    "bronze/.../time_series_daily_AAPL_20260108_060133.jsonl",
    "bronze/.../time_series_daily_MSFT_20260108_060133.jsonl"
  ]' \
  --RECORD_COUNT 224 \
  --INGESTED_AT 2025-12-19T18:58:17.203499+00:00 \
  --DAG_ID dag_ig \
  --RUN_ID manual_run \
  > /home/hadoop/workspace/scripts/logs/alphavanatge_murican_markets_spark_log.log 2>&1

and view the live spark job logs in the alphavanatge_murican_markets_spark_log.log file or,
have the spark UI enabled in the /usr/lib/spark/conf/spark-defaults.conf
"""

import sys
import json
import logging
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date, to_timestamp, expr, sha2,
    concat_ws, current_timestamp, lag, avg, stddev,
    min as spark_min, year, month, row_number
)
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("alphavantage-gold-job")

args = getResolvedOptions(
    sys.argv,
    [
        "DOMAIN",
        "SOURCE",
        "DATASET",
        "KEYS",
        "RECORD_COUNT",
        "INGESTED_AT",
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

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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

logger.info(f"Spark session started, configured to catalog={GLUE_CATALOG} and lakehouse_bucket={spark.conf.get(f'spark.sql.catalog.{GLUE_CATALOG}.warehouse')}")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Catalog objects

SILVER_DB = "silver"
GOLD_DB = "gold"

SILVER_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_clean"
GOLD_FACT_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_fact"
GOLD_METRICS_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_metrics"

SILVER_FQN = f"{GLUE_CATALOG}.{SILVER_DB}.{SILVER_TABLE}"
GOLD_FACT_FQN = f"{GLUE_CATALOG}.{GOLD_DB}.{GOLD_FACT_TABLE}"
GOLD_METRICS_FQN = f"{GLUE_CATALOG}.{GOLD_DB}.{GOLD_METRICS_TABLE}"
BRONZE_PATHS = [f"{LAKEHOUSE_BUCKET}/{k}" for k in KEYS]

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Bronze Layer

bronze_df = spark.read.option("mode", "FAILFAST").json(BRONZE_PATHS)

logger.info("Running Bronze validations...")

if bronze_df.rdd.isEmpty():
    raise RuntimeError("Bronze validation failed: no records found")

required_cols = {
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "ingested_at"
}

missing = required_cols - set(bronze_df.columns)
if missing:
    raise RuntimeError(f"Bronze validation failed: missing columns {missing}")

logger.info("Bronze validations passed")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Silver Layer

logger.info("Tye casting and creation of the hash column that will be used as a check for SCD2 task")
silver_incoming = (
    bronze_df
    
    # Business keys
    .withColumn("symbol", col("symbol"))
    .withColumn("trade_date", to_date(col("trade_date")))

    # OHLCV metrics
    .withColumn("open", col("open").cast("double"))
    .withColumn("high", col("high").cast("double"))
    .withColumn("low", col("low").cast("double"))
    .withColumn("close", col("close").cast("double"))
    .withColumn("volume", col("volume").cast("bigint"))

    # Source metadata
    .withColumn("source", col("source"))
    .withColumn("dataset", col("dataset"))
    .withColumn("request_url", col("request_url"))

    # Timestamps
    .withColumn("received_at", to_timestamp(col("received_at")))
    .withColumn("ingested_at", to_timestamp(col("ingested_at")))

    # SCD2 record hash
    .withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                col("symbol"),
                col("trade_date").cast("string"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("volume")
            ),
            256
        )
    )

    # SCD2 control columns
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))

    # Lineage & orchestration
    .withColumn("dag_id", lit(DAG_ID))
    .withColumn("run_id", lit(RUN_ID))
    .withColumn("processed_at", current_timestamp())
)

silver_incoming.createOrReplaceTempView("incoming")

logger.info("Created the temporary view of the incoming batch of data")

w = Window.partitionBy("symbol", "trade_date").orderBy(col("ingested_at").desc())
logger.info("Deduping the incoming batch of data") 
silver_dedup = (
  silver_incoming
  .withColumn("rn", row_number().over(w))
  .filter(col("rn") == 1)
  .drop("rn")
)

silver_dedup.createOrReplaceTempView("incoming_dedup")
logger.info("Finished deduping the batch")

logger.info("Running incoming batch validations...")
dup_cnt = spark.sql("""
SELECT COUNT(*) AS cnt
FROM (
  SELECT symbol, trade_date
  FROM incoming_dedup
  GROUP BY symbol, trade_date
  HAVING COUNT(*) > 1
)
""").collect()[0]["cnt"]

if dup_cnt > 0:
    raise RuntimeError(f"Incoming validation failed: {dup_cnt} duplicate business keys after deduplication")

logger.info("Incoming validations passed")

logger.info(f"Creating table {SILVER_FQN} if not exists...")
spark.sql(f''' 
    CREATE TABLE IF NOT EXISTS {SILVER_FQN} (
      symbol STRING,
      trade_date DATE,
      open DOUBLE,
      high DOUBLE,
      low DOUBLE,
      close DOUBLE,
      volume BIGINT,
      source STRING,
      dataset STRING,
      request_url STRING,
      received_at TIMESTAMP,
      ingested_at TIMESTAMP,
      record_hash STRING,
      effective_from TIMESTAMP,
      effective_to TIMESTAMP,
      is_current BOOLEAN,
      dag_id STRING,
      run_id STRING,
      processed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (trade_date);
''')

logger.info(f"Running the Merge task between the incoming batch and the {SILVER_FQN} table")
spark.sql(f"""
MERGE INTO {SILVER_FQN} t
USING incoming_dedup s
ON t.symbol = s.symbol AND t.trade_date = s.trade_date AND t.is_current = true
WHEN MATCHED AND t.record_hash <> s.record_hash THEN
  UPDATE SET t.effective_to = s.effective_from, t.is_current = false
WHEN NOT MATCHED THEN 
  INSERT *
""")

logger.info("Running Silver SCD2 invariant checks...")
multiple_currents = spark.sql(f"""
SELECT symbol, trade_date
FROM {SILVER_FQN}
WHERE is_current = true
GROUP BY symbol, trade_date
HAVING COUNT(*) > 1
""").count()

if multiple_currents > 0:
    raise ValueError(f"Found current records ({multiple_currents} records) for single business keys (symbol, trade_date) indicating FAULTY MERGE")

missing_currents = spark.sql(f"""
SELECT symbol, trade_date
FROM {SILVER_FQN}
GROUP BY symbol, trade_date
HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) = 0
""").count()

if missing_currents > 0:
    raise ValueError(f"Found records that are missing ({missing_currents} records) for single business keys (symbol, trade_date) indicating DATA LOSS and FAULTY MERGE")
    
logger.info("Silver validations passed")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Gold Layer

logger.info(f"Creating table {GOLD_FACT_FQN} if not exists...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_FACT_FQN} (
      symbol STRING,
      trade_date DATE,
      open DOUBLE,
      high DOUBLE,
      low DOUBLE,
      close DOUBLE,
      volume BIGINT,
      processed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (trade_date)
""")

logger.info("Merging latest Silver snapshot into Gold FACT")
spark.sql(f"""
    MERGE INTO {GOLD_FACT_FQN} AS tgt
    USING (
        SELECT
            symbol,
            trade_date,
            open,
            high,
            low,
            close,
            volume
        FROM {SILVER_FQN}
        WHERE is_current = true
    ) src
    ON tgt.symbol = src.symbol
    AND tgt.trade_date = src.trade_date

    WHEN MATCHED THEN UPDATE SET
        tgt.open = src.open,
        tgt.high = src.high,
        tgt.low  = src.low,
        tgt.close = src.close,
        tgt.volume = src.volume,
        tgt.processed_at = current_timestamp()

    WHEN NOT MATCHED THEN INSERT (
        symbol,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        processed_at
    ) VALUES (
        src.symbol,
        src.trade_date,
        src.open,
        src.high,
        src.low,
        src.close,
        src.volume,
        current_timestamp()
    )
""")

logger.info(f"Gold fact table {GOLD_FACT_FQN} has been successfully merged")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Incremental METRICS recomputation

logger.info("Determining incremental recomputation window")

min_affected_date = silver_dedup.select(spark_min("trade_date")).collect()[0][0]

LOOKBACK_DAYS = 25
recompute_from_date = expr(f"date_sub('{min_affected_date}', {LOOKBACK_DAYS})")

logger.info(f"Recomputing metrics from trade_date >= ({min_affected_date} - {LOOKBACK_DAYS} days)")

gold_fact_df = (
    spark.table(GOLD_FACT_FQN)
    .filter(col("trade_date") >= recompute_from_date)
)

logger.info("Creating the appropriate trading windows")

daily_window = Window.partitionBy("symbol").orderBy(col("trade_date").asc())
rolling_5 = daily_window.rowsBetween(-4, 0)
rolling_20 = daily_window.rowsBetween(-19, 0)

logger.info("Computing analytical metrics")

metrics_df = (
    gold_fact_df
    .withColumn(
        "daily_return",
        (col("close") - lag("close").over(daily_window)) /
        lag("close").over(daily_window)
    )
    .withColumn("ma_5_close", avg("close").over(rolling_5))
    .withColumn("ma_20_close", avg("close").over(rolling_20))
    .withColumn("volatility_20", stddev("daily_return").over(rolling_20))
    .withColumn("processed_at", current_timestamp())
)

logger.info("Writing Gold METRICS table incrementally, only partitions present in metrics_df are overwritten, all other partitions remain untouched.")

(
    metrics_df
    .write
    .mode("overwrite")
    .format("iceberg")
    .option("overwrite-mode", "dynamic") # incremental write not through append but through overwriting only the latest partitions, i.e., append the first time and overwrites on further runs
    .saveAsTable(GOLD_METRICS_FQN)
)

logger.info("Gold METRICS recomputation completed successfully")
logger.info("Job finished")