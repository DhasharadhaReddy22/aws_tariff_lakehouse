"""
To test the glue job locally, refer to the following spark submit command in terminal (making appropriate changes),
spark-submit \
  /home/hadoop/workspace/scripts/tariff_alphavantage_glue_job.py \
  --DOMAIN energy \
  --SOURCE alphavantage \
  --DATASET time_series_daily \
  --KEYS '[
    "bronze/.../time_series_daily_AAPL_20260121_055235.jsonl",
    "bronze/.../time_series_daily_MSFT_20260121_055235.jsonl"
  ]' \
  --RECORD_COUNT 224 \
  --INGESTED_AT 2025-12-19T18:58:17.203499+00:00 \
  --DAG_ID dag_ig \
  --RUN_ID manual_run \
  > /home/hadoop/workspace/scripts/spark_log.log 2>&1

and view the live spark job logs in the spark_log.log file or,
have the spark UI enabled in the /usr/lib/spark/conf/spark-defaults.conf
"""

import sys, json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

args = getResolvedOptions(
    sys.argv,
    ["DOMAIN", "SOURCE", "DATASET", "KEYS", "RECORD_COUNT", "INGESTED_AT", "DAG_ID", "RUN_ID"]
)

print(args)

DOMAIN = args["DOMAIN"]
SOURCE = args["SOURCE"]
DATASET = args["DATASET"]
KEYS = json.loads(args["KEYS"]) # list of relative paths/keys to ingested data files
RECORD_COUNT = int(args["RECORD_COUNT"])
INGESTED_AT = args["INGESTED_AT"]
DAG_ID = args["DAG_ID"]
RUN_ID = args["RUN_ID"]

# Set paths and catalog
LAKEHOUSE_BUCKET = 's3://dummy-lakehouse'
GLUE_CATALOG = 'glue_catalog'

# Initialize Spark session with Iceberg configurations
spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{GLUE_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.warehouse", LAKEHOUSE_BUCKET) \
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .getOrCreate()

print(f"Running glue job for DAG: {DAG_ID}, run_id: {RUN_ID}, ingested_at: {INGESTED_AT}")
print(f"\nThe job is being run on {LAKEHOUSE_BUCKET} lakehouse bucket")

if not KEYS:
    raise ValueError("No bronze files provided to Silver job")

SILVER_DB = "silver"
GOLD_DB = "gold"

SILVER_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_silver"
GOLD_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_gold"

SILVER_FQN = f"{GLUE_CATALOG}.{SILVER_DB}.{SILVER_TABLE}"
GOLD_FQN = f"{GLUE_CATALOG}.{GOLD_DB}.{GOLD_TABLE}"

BRONZE_PATHS = [f"{LAKEHOUSE_BUCKET}/{k}" for k in KEYS]

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Bronze Layer
bronze_df = spark.read.option("mode", "FAILFAST").json(BRONZE_PATHS)

print("Running Bronze validations...")

if bronze_df.count() == 0:
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

print("Bronze validations passed")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Silver Layer
silver_incoming = (
    bronze_df
    # ----------------------------
    # Business keys
    # ----------------------------
    .withColumn("symbol", col("symbol"))
    .withColumn("trade_date", to_date(col("trade_date")))

    # ----------------------------
    # OHLCV metrics
    # ----------------------------
    .withColumn("open", col("open").cast("double"))
    .withColumn("high", col("high").cast("double"))
    .withColumn("low", col("low").cast("double"))
    .withColumn("close", col("close").cast("double"))
    .withColumn("volume", col("volume").cast("bigint"))

    # ----------------------------
    # Source metadata
    # ----------------------------
    .withColumn("source", col("source"))
    .withColumn("dataset", col("dataset"))
    .withColumn("request_url", col("request_url"))

    # ----------------------------
    # Timestamps
    # ----------------------------
    .withColumn("received_at", to_timestamp(col("received_at")))
    .withColumn("ingested_at", to_timestamp(col("ingested_at")))

    # ----------------------------
    # SCD2 record hash
    # ----------------------------
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

    # ----------------------------
    # SCD2 control columns
    # ----------------------------
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))

    # ----------------------------
    # Lineage & orchestration
    # ----------------------------
    .withColumn("dag_id", lit(DAG_ID))
    .withColumn("run_id", lit(RUN_ID))
    .withColumn("processed_at", current_timestamp())
)

silver_incoming.createOrReplaceTempView("incoming")

w = Window.partitionBy("symbol", "trade_date").orderBy(col("ingested_at").desc())
silver_dedup = (
  silver_incoming
  .withColumn("rn", row_number().over(w))
  .filter(col("rn") == 1)
  .drop("rn")
)

silver_dedup.createOrReplaceTempView("incoming_dedup")

print("Running incoming batch validations...")

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

print("Incoming validations passed")

print(f"Running the Merge task between the incoming batch and the {SILVER_FQN} table")

spark.sql(f"""
MERGE INTO {SILVER_FQN} t
USING incoming_dedup s
ON t.symbol = s.symbol AND t.trade_date = s.trade_date AND t.is_current = true
WHEN MATCHED AND t.record_hash <> s.record_hash THEN
  UPDATE SET t.effective_to = s.effective_from, t.is_current = false
WHEN NOT MATCHED THEN INSERT *
""")

print("Running Silver SCD2 invariant checks...")

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
    
print("Silver validations passed")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Gold Layer

spark.sql(f"""
INSERT OVERWRITE {GOLD_FQN}
SELECT
    symbol,
    trade_date,

    open,
    high,
    low,
    close,
    volume,

    current_timestamp() AS processed_at
FROM {SILVER_FQN}
WHERE is_current = true
""")

print(f"Gold table {GOLD_FQN} has been successfully rewritten")