# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC **Purpose:** Load raw Parquet files from HDFS upload into Delta Bronze table.
# MAGIC
# MAGIC **Input:** Uploaded Parquet files (from local HDFS → DBFS)
# MAGIC **Output:** Delta table at `/bronze/transactions`

# COMMAND ----------

from pyspark.sql import functions as F

# Path configuration
# Upload your HDFS Parquet files to DBFS first:
#   databricks fs cp -r ./hdfs_export/ dbfs:/bronze/raw_parquet/
PARQUET_INPUT = "/bronze/raw_parquet/"
BRONZE_OUTPUT = "/bronze/transactions"

# COMMAND ----------

# Read raw Parquet data
raw_df = spark.read.parquet(PARQUET_INPUT)

print(f"Total rows loaded: {raw_df.count():,}")
raw_df.printSchema()

# COMMAND ----------

# Add ingestion metadata
bronze_df = (
    raw_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source", F.lit("paysim_hdfs"))
)

# COMMAND ----------

# Write to Delta Bronze
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(BRONZE_OUTPUT)
)

print(f"✅ Bronze layer written to {BRONZE_OUTPUT}")
print(f"   Rows: {bronze_df.count():,}")

# COMMAND ----------

# Verify
display(spark.read.format("delta").load(BRONZE_OUTPUT).limit(10))
