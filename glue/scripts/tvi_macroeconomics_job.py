"""
To test the glue job locally, refer to the following spark submit command in terminal (making appropriate changes),
spark-submit \
  /home/hadoop/workspace/scripts/tvi_macroeconomics_job.py \
  --DOMAIN energy \
  --SOURCE alphavantage \
  --DATASET time_series_daily \
  --KEYS '[
    "bronze/.../imf_indicators_20260110_173426.jsonl"
  ]' \
  --RECORD_COUNT 224 \
  --INGESTED_AT 2025-12-19T18:58:17.203499+00:00 \
  --DAG_ID dag_ig \
  --RUN_ID manual_run \
  > /home/hadoop/workspace/scripts/logs/tvi_macroeconomics_spark_log.log 2>&1

and view the live spark job logs in the tvi_macroeconomics_spark_log.log file or,
have the spark UI enabled in the /usr/lib/spark/conf/spark-defaults.conf
"""

import sys
import json
import logging
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, current_timestamp,
    avg, min as spark_min, max as spark_max
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("tvi-gold-job")

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

if not KEYS:
    raise ValueError("No bronze files provided")

LAKEHOUSE_BUCKET = "s3://dummy-lakehouse"
GLUE_CATALOG = "glue_catalog"

spark = (
    SparkSession.builder
    .config(f"spark.sql.catalog.{GLUE_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.warehouse", LAKEHOUSE_BUCKET)
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

BRONZE_PATHS = [f"{LAKEHOUSE_BUCKET}/{k}" for k in KEYS]

bronze_df = spark.read.json(BRONZE_PATHS)

required_cols = {"indicator", "country", "year", "value", "ingested_at"}
missing = required_cols - set(bronze_df.columns)
if missing:
    raise RuntimeError(f"Missing columns {missing}")

silver_df = (
    bronze_df
    .withColumn("year", col("year").cast("int"))
    .withColumn("value", col("value").cast("double"))
    .withColumn("ingested_at", to_timestamp(col("ingested_at")))
    .withColumn("processed_at", current_timestamp())
)

SILVER_FQN = f"{GLUE_CATALOG}.silver.imf_indicators_clean"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_FQN} (
  indicator STRING,
  country STRING,
  year INT,
  value DOUBLE,
  ingested_at TIMESTAMP,
  processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (year)
""")

silver_df.createOrReplaceTempView("incoming")

spark.sql(f"""
MERGE INTO {SILVER_FQN} t
USING incoming s
ON t.indicator = s.indicator AND t.country = s.country AND t.year = s.year
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

logger.info("Silver merge complete")

df = spark.table(SILVER_FQN)

pivot_df = (
    df.groupBy("country", "year")
    .pivot("indicator")
    .agg(avg("value"))
)

for c in pivot_df.columns:
    if c not in {"country", "year"}:
        stats = pivot_df.select(
            spark_min(c).alias("min"),
            spark_max(c).alias("max")
        ).collect()[0]
        if stats["max"] != stats["min"]:
            pivot_df = pivot_df.withColumn(
                f"{c}_norm",
                (col(c) - lit(stats["min"])) / (lit(stats["max"]) - lit(stats["min"])) * 100
            )

tvi_df = (
    pivot_df
    .withColumn(
        "tvi_score",
        (
            0.35 * col("BX_GDP_norm") +
            0.25 * col("NGDP_RPCH_norm") +
            0.20 * col("PCPIPCH_norm") +
            0.20 * col("DirectIn_norm")
        )
    )
    .withColumn("processed_at", current_timestamp())
)

GOLD_FQN = f"{GLUE_CATALOG}.gold.tvi_country_year"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FQN} (
  country STRING,
  year INT,
  tvi_score DOUBLE,
  processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (year)
""")

(
    tvi_df
    .select("country", "year", "tvi_score", "processed_at")
    .write
    .mode("overwrite")
    .format("iceberg")
    .option("overwrite-mode", "dynamic")
    .saveAsTable(GOLD_FQN)
)

logger.info("TVI Gold computation complete")