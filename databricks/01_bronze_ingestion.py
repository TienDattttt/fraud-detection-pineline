# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC **Purpose:** Load raw Parquet files uploaded to DBFS into Delta Bronze table.
# MAGIC
# MAGIC **Input:** Parquet files at `dbfs:/FileStore/fraud-pipeline/parquet/`
# MAGIC **Output:** Delta table at `dbfs:/delta/bronze/transactions`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📤 Hướng dẫn Upload Parquet lên DBFS (Community Edition)
# MAGIC
# MAGIC Databricks Community Edition **không hỗ trợ CLI/API upload**.
# MAGIC Thực hiện upload thủ công qua giao diện web:
# MAGIC
# MAGIC 1. Vào Databricks workspace → Click **"Data"** ở sidebar trái
# MAGIC 2. Click **"DBFS"** → Navigate đến `/FileStore/`
# MAGIC 3. Tạo folder `fraud-pipeline/parquet/` nếu chưa có
# MAGIC 4. Click **"Upload"** → Kéo thả các file `.parquet` từ thư mục
# MAGIC    `hdfs_export/` hoặc output của Spark Streaming (thư mục `datalake/transactions/`)
# MAGIC 5. Chờ upload hoàn tất → Chạy notebook này
# MAGIC
# MAGIC **Lưu ý:** Nếu output từ Spark Streaming có cấu trúc partition `dt=.../hour=...`,
# MAGIC hãy upload **toàn bộ thư mục gốc** (bao gồm subdirectories).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
)

# Path configuration — chuẩn cho Databricks Community Edition
PARQUET_INPUT = "dbfs:/FileStore/fraud-pipeline/parquet/"
BRONZE_OUTPUT = "dbfs:/delta/bronze/transactions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Raw Parquet

# COMMAND ----------

# Expected schema from Spark Streaming output
EXPECTED_SCHEMA = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
    StructField("prediction", DoubleType(), True),
    StructField("fraud_probability", DoubleType(), True),
])

# Read Parquet — use mergeSchema in case partitions have slight differences
raw_df = (
    spark.read  # noqa: F821
    .option("mergeSchema", "true")
    .parquet(PARQUET_INPUT)
)

total_rows = raw_df.count()
print(f"📂 Total rows loaded: {total_rows:,}")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Cleaning

# COMMAND ----------

# Drop rows with null values in critical columns
critical_cols = ["amount", "type", "nameOrig", "nameDest"]
clean_df = raw_df.na.drop(subset=critical_cols)

dropped = total_rows - clean_df.count()
if dropped > 0:
    print(f"⚠️ Dropped {dropped:,} rows with null values in {critical_cols}")
else:
    print("✅ No null values found in critical columns")

# Ensure correct data types
clean_df = (
    clean_df
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("step", F.col("step").cast("integer"))
    .withColumn("isFraud", F.col("isFraud").cast("integer"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add Ingestion Metadata & Write Bronze

# COMMAND ----------

# Add lineage/audit columns
bronze_df = (
    clean_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source", F.lit("streaming_parquet_upload"))
    .withColumn("ingestion_date", F.current_date())
)

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

# MAGIC %md
# MAGIC ## 4. Verify Bronze Table

# COMMAND ----------

# Quick verification
verified_df = spark.read.format("delta").load(BRONZE_OUTPUT)  # noqa: F821
print(f"📊 Bronze table row count: {verified_df.count():,}")
print(f"   Columns: {len(verified_df.columns)}")
print("   Schema:")
verified_df.printSchema()

# Show sample data
display(verified_df.limit(10))  # noqa: F821
