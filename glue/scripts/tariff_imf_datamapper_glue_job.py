"""
To test the glue job locally, refer to the following spark submit command in terminal (making appropriate changes),
spark-submit \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.sql.adaptive.enabled=true \
  /home/hadoop/workspace/scripts/tariff_imf_datamapper_glue_job.py \
  --DOMAIN macroeconomics \
  --SOURCE imf \
  --DATASET indicators \
  --KEYS '[
    "bronze/macroeconomics/source=imf/dataset=indicators/ingestion_date=2026-01-29/imf_indicators_20260129_051258.jsonl"
  ]' \
  --RECORD_COUNT 8 \
  --INGESTED_AT 2026-01-28T07:13:18.177910+00:00 \
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
    sha2, concat_ws, row_number, when, coalesce,
    avg, min as spark_min, max as spark_max
)
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("tariff_imf_datamapper_glue_job")

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

if not KEYS:
    raise ValueError("No bronze files provided")

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
    .getOrCreate()
)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Catalog objects

SILVER_DB = "silver"
GOLD_DB = "gold"

SILVER_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_clean"
GOLD_FACT_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_fact"
GOLD_SCORES_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_scores"
GOLD_TVI_TABLE = f"{DOMAIN}_{SOURCE}_{DATASET}_tvi"

SILVER_FQN = f"{GLUE_CATALOG}.{SILVER_DB}.{SILVER_TABLE}"
GOLD_FACT_FQN = f"{GLUE_CATALOG}.{GOLD_DB}.{GOLD_FACT_TABLE}"
GOLD_SCORES_FQN = f"{GLUE_CATALOG}.{GOLD_DB}.{GOLD_SCORES_TABLE}"
GOLD_TVI_FQN = f"{GLUE_CATALOG}.{GOLD_DB}.{GOLD_TVI_TABLE}"
BRONZE_PATHS = [f"{LAKEHOUSE_BUCKET}/{k}" for k in KEYS]

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Bronze Layer

bronze_df = spark.read.json(BRONZE_PATHS)

logger.info("Running Bronze validations...")

if bronze_df.rdd.isEmpty():
    raise RuntimeError("Bronze validation failed: no records found")
   
required_cols = {"indicator", "country", "year", "value", "ingested_at"}
missing = required_cols - set(bronze_df.columns)
if missing:
    raise RuntimeError(f"Missing columns {missing}")
   
logger.info("Bronze validations passed")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Silver Layer

logger.info("Type casting and creation of the hash column that will be used as a check for SCD2 task")

silver_incoming = (
    bronze_df
    # Business keys
    .withColumn("indicator", col("indicator"))
    .withColumn("country", col("country"))
    .withColumn("year", col("year").cast("int"))
   
    # Metrics
    .withColumn("value", col("value").cast("double"))
   
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
                col("indicator"),
                col("country"),
                col("year").cast("string"),
                col("value").cast("string")
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

w = Window.partitionBy("indicator", "country", "year").orderBy(col("year").desc())
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
      SELECT indicator, country, year
      FROM incoming_dedup
      GROUP BY indicator, country, year
      HAVING COUNT(*) > 1
    )
""").collect()[0]["cnt"]

if dup_cnt > 0:
    raise RuntimeError(f"Incoming validation failed: {dup_cnt} duplicate business keys after deduplication")

logger.info("Incoming validations passed")

logger.info(f"Creating table {SILVER_FQN} if not exists...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_FQN} (
      indicator STRING,
      country STRING,
      year INT,
      value DOUBLE,
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
    PARTITIONED BY (indicator);
""")

logger.info(f"Running the Merge task between the incoming batch and the {SILVER_FQN} table")
spark.sql(f"""
    MERGE INTO {SILVER_FQN} t
    USING incoming_dedup s
    ON t.indicator = s.indicator AND t.country = s.country AND t.year = s.year AND t.is_current = true
    WHEN MATCHED AND t.record_hash <> s.record_hash THEN
        UPDATE SET t.effective_to = s.effective_from, t.is_current = false
    WHEN NOT MATCHED THEN
        INSERT *
""")

logger.info("Running Silver SCD2 invariant checks...")
multiple_currents = spark.sql(f"""
    SELECT indicator, country, year
    FROM {SILVER_FQN}
    WHERE is_current = true
    GROUP BY indicator, country, year
    HAVING COUNT(*) > 1
""").count()

if multiple_currents > 0:
    raise ValueError(f"Found current records ({multiple_currents} records) for single business keys (indicator, country, year) indicating FAULTY MERGE")

missing_currents = spark.sql(f"""
    SELECT indicator, country, year
    FROM {SILVER_FQN}
    GROUP BY indicator, country, year
    HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) = 0
""").count()

if missing_currents > 0:
    raise ValueError(f"Found records that are missing ({missing_currents} records) for single business keys (indicator, country, year) indicating DATA LOSS and FAULTY MERGE")

logger.info("Silver validations passed")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Gold Layer

logger.info(f"Creation of the table {GOLD_FACT_FQN}, {GOLD_SCORES_FQN}, and {GOLD_TVI_FQN} if not exists...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_FACT_FQN} (
        indicator STRING,
        country STRING,
        year INT,
        value DOUBLE,
        processed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (indicator)
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_SCORES_FQN} (
        country STRING,
        year INT,
        trade_exposure_score DOUBLE,
        competitiveness_score DOUBLE,
        macro_resilience_score DOUBLE,
        financial_sensitivity_score DOUBLE,
        processed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (year)
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TVI_FQN} (
        country STRING,
        year INT,
        tvi_score DOUBLE,
        processed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (year)
""")

logger.info(f"Writing/over-writing into the {GOLD_FACT_FQN} table")
spark.sql(f"""
    INSERT OVERWRITE {GOLD_FACT_FQN}
    SELECT indicator, country, year, value, processed_at
    FROM {SILVER_FQN}
    WHERE is_current = true
""")

logger.info(f"Finished writing/over-writing into the {GOLD_FACT_FQN} table")

logger.info(f"Proceeding to computing the scores for the {GOLD_SCORES_FQN} and TVI for the {GOLD_TVI_FQN} tables")
logger.info("Preparing normalized indicator datasets")

base_df = (
    spark.table(SILVER_FQN)
    .filter(col("is_current") == True)
    .select("indicator", "country", "year", "value")
)

base_df = base_df.persist()

def normalize_indicator(df, indicator_name):
    """
    Normalize one indicator across countries per year.
    Returns: country, year, <indicator>_norm
    """
    logger.info(f"Normalizing indicator: {indicator_name}")

    w = Window.partitionBy("year")

    return (
        df.filter(col("indicator") == indicator_name)
          .withColumn("min_v", spark_min("value").over(w))
          .withColumn("max_v", spark_max("value").over(w))
          .withColumn(
              f"{indicator_name}_norm",
              when(col("max_v") == col("min_v"), lit(50.0))
              .otherwise((col("value") - col("min_v")) / (col("max_v") - col("min_v")) * 100)
          )
          .select("country", "year", f"{indicator_name}_norm")
    )

bca_norm   = normalize_indicator(base_df, "BCA_NGDPD")
pcpi_norm  = normalize_indicator(base_df, "PCPIPCH")
gdp_norm   = normalize_indicator(base_df, "NGDP_RPCH")
lur_norm   = normalize_indicator(base_df, "LUR")

logger.info("Finished indicator normalization")
logger.info("Computing channel scores")

scores_df = (
    bca_norm
    .join(pcpi_norm, ["country", "year"], "outer")
    .join(gdp_norm, ["country", "year"], "outer")
    .join(lur_norm, ["country", "year"], "outer")

    # Channel 1: Trade Exposure
    .withColumn("trade_exposure_score", coalesce(col("BCA_NGDPD_norm"), lit(50.0)))

    # Channel 2: Competitiveness
    .withColumn("competitiveness_score", coalesce(col("PCPIPCH_norm"), lit(50.0)))

    # Channel 3: Macro Resilience
    .withColumn("macro_resilience_score", 0.6 * coalesce(col("NGDP_RPCH_norm"), lit(50.0)) + 0.4 * coalesce(col("LUR_norm"), lit(50.0)))

    # Channel 4: Financial Sensitivity (not observable)
    .withColumn("financial_sensitivity_score", lit(50.0))
    
    .withColumn("coverage_ratio", (
        col("BCA_NGDPD_norm").isNotNull().cast("int") +
        col("PCPIPCH_norm").isNotNull().cast("int") +
        col("NGDP_RPCH_norm").isNotNull().cast("int") +
        col("LUR_norm").isNotNull().cast("int")
        ) / lit(4.0))

    .withColumn("processed_at", current_timestamp())
)

logger.info("Channel score computation complete")

logger.info(f"Finished computing the TVI scores, and proceeding to validate and then write to the {GOLD_SCORES_FQN} table...")

score_cols = [c for c in scores_df.columns if c.endswith("_score")]
for c in score_cols:
    invalid_cnt = (
        scores_df
        .filter(~col(c).between(0, 100))
        .count()
    )
    if invalid_cnt > 0:
        raise ValueError(f"Found {invalid_cnt} rows where {c} is outside [0,100]")

logger.info(f"Writing channel scores into {GOLD_SCORES_FQN}")

(
    scores_df
    .select(
        "country",
        "year",
        "trade_exposure_score",
        "competitiveness_score",
        "macro_resilience_score",
        "financial_sensitivity_score",
        "processed_at"
    )
    .write
    .mode("overwrite")
    .format("iceberg")
    .option("overwrite-mode", "dynamic")
    .saveAsTable(GOLD_SCORES_FQN)
)

logger.info("Gold Scores write complete")

logger.info("Computing final TVI score")

tvi_df = (
    scores_df
    .withColumn(
        "tvi_score",
        0.35 * col("trade_exposure_score") +
        0.20 * col("competitiveness_score") +
        0.25 * col("macro_resilience_score") +
        0.20 * col("financial_sensitivity_score")
    )
    .select("country", "year", "tvi_score", "processed_at")
)

logger.info(f"Proceeding to write the new calculations to {GOLD_TVI_FQN}")

logger.info(f"Writing TVI scores into {GOLD_TVI_FQN}")

(
    tvi_df
    .write
    .mode("overwrite")
    .format("iceberg")
    .option("overwrite-mode", "dynamic")
    .saveAsTable(GOLD_TVI_FQN)
)

logger.info("TVI Gold computation complete")