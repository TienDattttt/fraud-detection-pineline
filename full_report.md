# BÁO CÁO KỸ THUẬT: FRAUD DETECTION PIPELINE

## 1. TỔNG QUAN DỰ ÁN
- **Tên dự án:** Real-time Fraud Detection Pipeline
- **Mục tiêu:** Quản trị rủi ro & Phát hiện gian lận theo thời gian thực cho ví điện tử.
- **Tech Stack:** Kafka, Spark Streaming, HDFS, Redis, FastAPI, Databricks, Docker, MLflow, GitHub Actions.

## 2. CẤU TRÚC THƯ MỤC THỰC TẾ
`	ext
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\.github\workflows\ci.yml
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\data\paysim_transactions.csv
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\databricks\01_bronze_ingestion.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\databricks\02_silver_ml.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\databricks\03_gold_aggregation.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\metadata\part-00000
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\metadata\_SUCCESS
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\0_StringIndexer_a8298aa67476\data\part-00000-4b1c7d5a-8e24-47aa-a1df-4a4c7e5554a1-c000.snappy.parquet
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\0_StringIndexer_a8298aa67476\data\_SUCCESS
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\0_StringIndexer_a8298aa67476\metadata\part-00000
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\0_StringIndexer_a8298aa67476\metadata\_SUCCESS
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\1_VectorAssembler_093ac75ca7db\metadata\part-00000
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\1_VectorAssembler_093ac75ca7db\metadata\_SUCCESS
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\2_StandardScaler_d57e58f4a09b\data\part-00000-96611e06-61f9-4369-915d-f56eb12f8beb-c000.snappy.parquet
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\2_StandardScaler_d57e58f4a09b\data\_SUCCESS
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\2_StandardScaler_d57e58f4a09b\metadata\part-00000
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\2_StandardScaler_d57e58f4a09b\metadata\_SUCCESS
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\3_GBTClassifier_a53b1c7f6513\data\part-00000-e8cea215-98db-48cc-9641-5468aa018708-c000.snappy.parquet
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\3_GBTClassifier_a53b1c7f6513\data\_SUCCESS
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\3_GBTClassifier_a53b1c7f6513\metadata\part-00000
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\3_GBTClassifier_a53b1c7f6513\metadata\_SUCCESS
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\3_GBTClassifier_a53b1c7f6513\treesMetadata\part-00000-35348eca-dacb-4b99-86b2-aad9efc14dfd-c000.snappy.parquet
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\fraud_pipeline_model\stages\3_GBTClassifier_a53b1c7f6513\treesMetadata\_SUCCESS
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\0\meta.yaml
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\metrics\accuracy
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\metrics\auc
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\metrics\f1
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\metrics\precision
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\metrics\recall
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\params\features
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\params\model_type
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\params\test_size
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\params\train_size
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\tags\mlflow.runName
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\tags\mlflow.source.name
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\tags\mlflow.source.type
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\tags\mlflow.user
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\10d857fde3cb4cedbed55045938dae1e\meta.yaml
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\models\mlruns\736006819556372166\meta.yaml
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\producer\kafka_producer.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\producer\requirements.txt
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\scripts\download_dataset.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\scripts\start_pipeline.sh
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\scripts\stop_pipeline.sh
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\serving\api.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\serving\Dockerfile
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\serving\redis_listener.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\serving\requirements.txt
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\spark\__pycache__\preprocessing.cpython-38.pyc
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\spark\preprocessing.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\spark\requirements.txt
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\spark\streaming_pipeline.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\spark\train_model.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\tests\conftest.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\tests\test_api.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\tests\test_preprocessing.py
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\docker-compose.yml
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\Dockerfile
D:\Ki2Nam4\BigData\Brainstorm\fraud-detection-pipeline\README.md

`

## 3. NỘI DUNG TỪNG FILE QUAN TRỌNG

### .github/workflows/ci.yml
**Trạng thái / Ghi chú:** Code logic
`yaml
name: CI Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  lint:
    name: Lint (flake8)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install flake8
        run: pip install flake8

      - name: Run flake8
        run: |
          flake8 producer/ spark/ serving/ \
            --max-line-length 120 \
            --exclude __pycache__,.git \
            --count --show-source --statistics

  test-api:
    name: Test API (pytest)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install -r serving/requirements.txt
          pip install pytest httpx

      - name: Run API tests
        run: pytest tests/test_api.py -v

  docker-validate:
    name: Validate Docker Compose
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Validate compose file
        run: docker compose config --quiet

`

### databricks/01_bronze_ingestion.py
**Trạng thái / Ghi chú:** Code logic
`python
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

`

### databricks/02_silver_ml.py
**Trạng thái / Ghi chú:** Code logic
`python
# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Feature Engineering + ML Prediction
# MAGIC
# MAGIC **Purpose:** Clean Bronze data, engineer features, apply ML model.
# MAGIC
# MAGIC **Input:** Delta `/bronze/transactions`
# MAGIC **Output:** Delta `/silver/transactions_scored`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

BRONZE_PATH = "/bronze/transactions"
SILVER_OUTPUT = "/silver/transactions_scored"
# Upload your trained model to DBFS:
#   databricks fs cp -r ./models/fraud_pipeline_model dbfs:/models/fraud_pipeline_model
MODEL_PATH = "/models/fraud_pipeline_model"

# COMMAND ----------

# Read Bronze data
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze rows: {bronze_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering (same as local preprocessing.py)

# COMMAND ----------

def add_engineered_features(df):
    """Domain-specific features for PaySim fraud detection."""
    result = df

    result = result.withColumn(
        "balance_diff_orig",
        F.col("oldbalanceOrg") - F.col("newbalanceOrig")
    )
    result = result.withColumn(
        "balance_diff_dest",
        F.col("newbalanceDest") - F.col("oldbalanceDest")
    )
    result = result.withColumn(
        "amount_ratio",
        F.col("amount") / (F.col("oldbalanceOrg") + F.lit(1.0))
    )
    result = result.withColumn(
        "is_zero_balance_orig",
        F.when(F.col("newbalanceOrig") == 0.0, 1.0).otherwise(0.0)
    )
    result = result.withColumn(
        "is_large_amount",
        F.when(F.col("amount") > 200000.0, 1.0).otherwise(0.0)
    )
    return result

# COMMAND ----------

# Prepare features
prepared_df = bronze_df.withColumn("label", F.col("isFraud").cast("double"))
prepared_df = add_engineered_features(prepared_df)

# COMMAND ----------

# Load trained model
model = PipelineModel.load(MODEL_PATH)
print("✅ Model loaded from DBFS")

# COMMAND ----------

# Apply ML predictions
scored_df = model.transform(prepared_df)

# Extract fraud probability
extract_fraud_prob = F.udf(
    lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0,
    "double"
)
scored_df = scored_df.withColumn(
    "fraud_probability", extract_fraud_prob(F.col("probability"))
)

# COMMAND ----------

# Select final Silver columns
silver_df = scored_df.select(
    "step", "type", "amount",
    "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest",
    "isFraud", "isFlaggedFraud",
    "balance_diff_orig", "balance_diff_dest",
    "amount_ratio", "is_zero_balance_orig",
    "prediction", "fraud_probability",
    F.current_timestamp().alias("scored_at"),
)

# Write Silver Delta
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_OUTPUT)
)

print(f"✅ Silver layer written to {SILVER_OUTPUT}")
print(f"   Total: {silver_df.count():,}")
print(f"   Predicted fraud: {silver_df.filter(F.col('prediction') == 1.0).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Model Tracking

# COMMAND ----------

import mlflow

# Log evaluation metrics
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)

evaluators = {
    "auc": BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC"),
    "accuracy": MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy"),
    "f1": MulticlassClassificationEvaluator(labelCol="label", metricName="f1"),
}

# Need to re-transform with label for evaluation
eval_df = model.transform(prepared_df)

with mlflow.start_run(run_name="silver_batch_evaluation"):
    for name, evaluator in evaluators.items():
        score = evaluator.evaluate(eval_df)
        mlflow.log_metric(name, score)
        print(f"  {name}: {score:.6f}")

print("📝 Metrics logged to MLflow")

`

### databricks/03_gold_aggregation.py
**Trạng thái / Ghi chú:** Code logic
`python
# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer — KPI Aggregations
# MAGIC
# MAGIC **Purpose:** Aggregate Silver data into business KPIs for dashboards.
# MAGIC
# MAGIC **Input:** Delta `/silver/transactions_scored`
# MAGIC **Output:** Multiple Gold Delta tables under `/gold/`

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_PATH = "/silver/transactions_scored"
GOLD_PATH = "/gold"

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver rows: {silver_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fraud by Transaction Type

# COMMAND ----------

fraud_by_type = (
    silver_df.groupBy("type")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(F.when(F.col("prediction") == 1.0, 1).otherwise(0))
         .alias("predicted_fraud"),
        F.sum(F.when(F.col("isFraud") == 1, 1).otherwise(0))
         .alias("actual_fraud"),
        F.sum("amount").alias("total_amount"),
        F.avg("fraud_probability").alias("avg_fraud_probability"),
    )
    .withColumn(
        "fraud_rate",
        F.round(F.col("predicted_fraud") / F.col("total_transactions") * 100, 4)
    )
    .orderBy(F.desc("predicted_fraud"))
)

fraud_by_type.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/fraud_by_type")
display(fraud_by_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fraud by Amount Range

# COMMAND ----------

fraud_by_amount = (
    silver_df
    .withColumn(
        "amount_range",
        F.when(F.col("amount") < 10000, "0-10K")
        .when(F.col("amount") < 100000, "10K-100K")
        .when(F.col("amount") < 500000, "100K-500K")
        .when(F.col("amount") < 1000000, "500K-1M")
        .otherwise("1M+")
    )
    .groupBy("amount_range")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(F.when(F.col("prediction") == 1.0, 1).otherwise(0))
         .alias("predicted_fraud"),
        F.avg("fraud_probability").alias("avg_fraud_probability"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy("amount_range")
)

fraud_by_amount.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/fraud_by_amount")
display(fraud_by_amount)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Hourly Transaction Volume (step = 1 hour in PaySim)

# COMMAND ----------

hourly_volume = (
    silver_df.groupBy("step")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum(F.when(F.col("prediction") == 1.0, 1).otherwise(0))
         .alias("fraud_count"),
        F.sum("amount").alias("total_amount"),
    )
    .withColumn(
        "fraud_rate",
        F.round(F.col("fraud_count") / F.col("transaction_count") * 100, 4)
    )
    .orderBy("step")
)

hourly_volume.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/hourly_volume")
display(hourly_volume.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model Performance Summary

# COMMAND ----------

# Confusion matrix style
model_summary = (
    silver_df
    .withColumn("true_positive",
                F.when((F.col("prediction") == 1.0) & (F.col("isFraud") == 1), 1).otherwise(0))
    .withColumn("false_positive",
                F.when((F.col("prediction") == 1.0) & (F.col("isFraud") == 0), 1).otherwise(0))
    .withColumn("true_negative",
                F.when((F.col("prediction") == 0.0) & (F.col("isFraud") == 0), 1).otherwise(0))
    .withColumn("false_negative",
                F.when((F.col("prediction") == 0.0) & (F.col("isFraud") == 1), 1).otherwise(0))
    .agg(
        F.sum("true_positive").alias("TP"),
        F.sum("false_positive").alias("FP"),
        F.sum("true_negative").alias("TN"),
        F.sum("false_negative").alias("FN"),
        F.count("*").alias("total"),
    )
)

model_summary.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/model_summary")

row = model_summary.collect()[0]
tp, fp, tn, fn = row["TP"], row["FP"], row["TN"], row["FN"]
print(f"Confusion Matrix:")
print(f"  TP={tp:,}  FP={fp:,}")
print(f"  FN={fn:,}  TN={tn:,}")
precision = tp / (tp + fp) if (tp + fp) > 0 else 0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0
print(f"  Precision: {precision:.4f}")
print(f"  Recall:    {recall:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Overall KPI Summary

# COMMAND ----------

kpi_summary = silver_df.agg(
    F.count("*").alias("total_transactions"),
    F.sum(F.when(F.col("prediction") == 1.0, 1).otherwise(0)).alias("predicted_fraud"),
    F.sum(F.when(F.col("isFraud") == 1, 1).otherwise(0)).alias("actual_fraud"),
    F.sum("amount").alias("total_volume"),
    F.avg("amount").alias("avg_transaction"),
    F.countDistinct("nameOrig").alias("unique_senders"),
    F.countDistinct("nameDest").alias("unique_receivers"),
)

kpi_summary.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/kpi_summary")
display(kpi_summary)

# COMMAND ----------

print("🎉 Gold layer complete!")
print(f"Tables written to {GOLD_PATH}/:")
print("  1. fraud_by_type")
print("  2. fraud_by_amount")
print("  3. hourly_volume")
print("  4. model_summary (confusion matrix)")
print("  5. kpi_summary")

`

### producer/kafka_producer.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
Kafka Producer for PaySim e-wallet transaction data.

Reads PaySim CSV and streams each row as JSON to Kafka topic 'transactions'.
Simulates real-time delay between messages for demo purposes.
"""
import time
import json
import random
import csv
import os
import argparse
import sys

from kafka import KafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "transactions")
CSV_FILE_PATH = os.getenv(
    "CSV_PATH", "/opt/spark/work/data/paysim_transactions.csv"
)

# PaySim numeric fields to cast from CSV strings
FLOAT_FIELDS = [
    "amount", "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest",
]
INT_FIELDS = ["step", "isFraud", "isFlaggedFraud"]


def parse_row(row: dict) -> dict:
    """Cast PaySim CSV string values to proper numeric types."""
    parsed = {}
    for key, value in row.items():
        clean_key = key.strip()
        if clean_key in FLOAT_FIELDS:
            parsed[clean_key] = float(value)
        elif clean_key in INT_FIELDS:
            parsed[clean_key] = int(value)
        else:
            parsed[clean_key] = value.strip()
    return parsed


def create_producer(retries: int = 5) -> KafkaProducer:
    """Create Kafka producer with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                acks="all",
                retries=3,
                batch_size=16384,
                linger_ms=10,
            )
            print(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except Exception as e:
            print(f"❌ Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(5)
    print("💀 Could not connect to Kafka. Exiting.")
    sys.exit(1)


def run_producer(speed: float = 1.0, limit: int = 0):
    """
    Stream PaySim CSV rows to Kafka.

    Args:
        speed: Multiplier for delay. 1.0 = default (0.1-1.5s).
               0.0 = no delay (max speed). 2.0 = slower.
        limit: Max rows to send. 0 = all rows.
    """
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ File not found: {CSV_FILE_PATH}")
        print("   Run: python scripts/download_dataset.py")
        sys.exit(1)

    producer = create_producer()
    fraud_count = 0
    total_count = 0

    try:
        with open(CSV_FILE_PATH, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                parsed = parse_row(row)
                producer.send(TOPIC_NAME, value=parsed)

                total_count += 1
                if parsed.get("isFraud") == 1:
                    fraud_count += 1

                if total_count % 50 == 0:
                    producer.flush()

                if total_count % 500 == 0:
                    fraud_pct = (
                        (fraud_count / total_count * 100)
                        if total_count > 0 else 0
                    )
                    print(
                        f"[{total_count:,}] Sent | "
                        f"Fraud: {fraud_count:,} ({fraud_pct:.2f}%) | "
                        f"Last: {parsed.get('type')} "
                        f"${parsed.get('amount', 0):,.2f}"
                    )

                if limit > 0 and total_count >= limit:
                    break

                if speed > 0:
                    time.sleep(random.uniform(0.1, 1.5) * speed)

        producer.flush()
        print(f"\n{'='*60}")
        print(f"✅ Done! Sent {total_count:,} transactions")
        print(f"   Fraud: {fraud_count:,} / {total_count:,}")
        print(f"{'='*60}")

    except KeyboardInterrupt:
        print(f"\n⏹ Stopped. Sent {total_count:,} rows.")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PaySim Kafka Producer"
    )
    parser.add_argument(
        "--speed", type=float, default=1.0,
        help="Delay multiplier. 0=instant, 1=normal, 2=slow"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max rows to send. 0=all"
    )
    args = parser.parse_args()
    run_producer(speed=args.speed, limit=args.limit)

`

### producer/requirements.txt
**Trạng thái / Ghi chú:** Code logic
`text
kafka-python==2.0.2

`

### scripts/download_dataset.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
Download PaySim dataset from Kaggle.

Usage:
    pip install kaggle
    export KAGGLE_USERNAME=your_username
    export KAGGLE_KEY=your_api_key
    python scripts/download_dataset.py

Alternative (manual):
    1. Go to https://www.kaggle.com/datasets/ealaxi/paysim1
    2. Download 'PS_20174392719_1491204439457_log.csv'
    3. Rename to 'paysim_transactions.csv'
    4. Place in 'data/' folder
"""
import os
import sys
import zipfile
import shutil


DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data"
)
FINAL_NAME = "paysim_transactions.csv"
FINAL_PATH = os.path.join(DATA_DIR, FINAL_NAME)
KAGGLE_DATASET = "ealaxi/paysim1"


def download_with_kaggle():
    """Download using Kaggle API."""
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        print("❌ kaggle package not installed.")
        print("   Run: pip install kaggle")
        sys.exit(1)

    api = KaggleApi()
    api.authenticate()

    print(f"📥 Downloading {KAGGLE_DATASET}...")
    api.dataset_download_files(KAGGLE_DATASET, path=DATA_DIR, unzip=False)

    zip_path = os.path.join(DATA_DIR, "paysim1.zip")
    if os.path.exists(zip_path):
        print("📦 Extracting...")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(DATA_DIR)
        os.remove(zip_path)

    # Rename the extracted file
    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv") and f != FINAL_NAME:
            src = os.path.join(DATA_DIR, f)
            shutil.move(src, FINAL_PATH)
            print(f"✅ Renamed {f} → {FINAL_NAME}")
            break


def verify_dataset():
    """Verify downloaded dataset."""
    if not os.path.exists(FINAL_PATH):
        print(f"❌ File not found: {FINAL_PATH}")
        return False

    size_mb = os.path.getsize(FINAL_PATH) / (1024 * 1024)
    print(f"✅ Dataset ready: {FINAL_PATH}")
    print(f"   Size: {size_mb:.1f} MB")

    with open(FINAL_PATH, "r") as f:
        header = f.readline().strip()
        first_row = f.readline().strip()
    print(f"   Header: {header[:80]}...")
    print(f"   Sample: {first_row[:80]}...")

    # Count rows
    with open(FINAL_PATH, "r") as f:
        row_count = sum(1 for _ in f) - 1
    print(f"   Rows: {row_count:,}")
    return True


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(FINAL_PATH):
        print(f"⚡ Dataset already exists at {FINAL_PATH}")
        verify_dataset()
        sys.exit(0)

    print("=" * 60)
    print("PaySim Dataset Downloader")
    print("=" * 60)
    print()
    print("Option 1: Automatic (requires Kaggle API key)")
    print("Option 2: Manual download")
    print()

    choice = input("Try automatic download? [y/N]: ").strip().lower()
    if choice == "y":
        download_with_kaggle()
    else:
        print()
        print("📋 Manual download steps:")
        print("   1. Go to: https://www.kaggle.com/datasets/ealaxi/paysim1")
        print("   2. Click 'Download' (need Kaggle account)")
        print(f"   3. Extract CSV to: {DATA_DIR}/")
        print(f"   4. Rename to: {FINAL_NAME}")
        print()
        input("Press Enter after placing the file...")

    if verify_dataset():
        print("\n🎉 Ready to use!")
    else:
        print("\n❌ Dataset not found. Please download manually.")
        sys.exit(1)

`

### scripts/start_pipeline.sh
**Trạng thái / Ghi chú:** Code logic
`bash
#!/bin/bash
# ============================================================
# start_pipeline.sh — One-command startup for the full pipeline
# ============================================================
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=================================${NC}"
echo -e "${GREEN} Fraud Detection Pipeline Startup${NC}"
echo -e "${GREEN}=================================${NC}"

# ------------------------------------------------------------------
# Step 1: Build and start Docker containers
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[1/5] Starting Docker containers...${NC}"
docker compose up --build -d

# ------------------------------------------------------------------
# Step 2: Wait for services to be healthy
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[2/5] Waiting for services...${NC}"

echo -n "  Kafka..."
until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo -n "."
    sleep 3
done
echo -e " ${GREEN}OK${NC}"

echo -n "  HDFS..."
until docker exec namenode hdfs dfs -ls / > /dev/null 2>&1; do
    echo -n "."
    sleep 3
done
echo -e " ${GREEN}OK${NC}"

echo -n "  Redis..."
until docker exec redis redis-cli ping > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}OK${NC}"

echo -n "  Spark..."
until curl -s http://localhost:8081/ > /dev/null 2>&1; do
    echo -n "."
    sleep 3
done
echo -e " ${GREEN}OK${NC}"

# ------------------------------------------------------------------
# Step 3: Setup HDFS directories
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[3/5] Setting up HDFS directories...${NC}"
docker exec namenode hdfs dfs -mkdir -p /datalake/transactions
docker exec namenode hdfs dfs -mkdir -p /datalake/checkpoints/streaming
docker exec namenode hdfs dfs -chmod -R 777 /datalake
echo -e "  ${GREEN}HDFS directories created${NC}"

# ------------------------------------------------------------------
# Step 4: Create Kafka topic
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[4/5] Creating Kafka topic...${NC}"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic transactions \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || true
echo -e "  ${GREEN}Topic 'transactions' ready${NC}"

# ------------------------------------------------------------------
# Step 5: Check data file
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[5/5] Checking dataset...${NC}"
if [ -f "./data/paysim_transactions.csv" ]; then
    ROW_COUNT=$(wc -l < ./data/paysim_transactions.csv)
    echo -e "  ${GREEN}Dataset found: ${ROW_COUNT} rows${NC}"
else
    echo -e "  ${RED}Dataset not found!${NC}"
    echo "  Run: python scripts/download_dataset.py"
fi

# ------------------------------------------------------------------
# Done
# ------------------------------------------------------------------
echo -e "\n${GREEN}=================================${NC}"
echo -e "${GREEN} All services running!${NC}"
echo -e "${GREEN}=================================${NC}"
echo ""
echo "  Kafka:       localhost:9092"
echo "  HDFS UI:     http://localhost:9870"
echo "  Spark UI:    http://localhost:8081"
echo "  API:         http://localhost:8000"
echo "  API Docs:    http://localhost:8000/docs"
echo ""
echo "Next steps:"
echo "  1. Train model (if not done):"
echo "     docker exec spark-master spark-submit /opt/spark/work/spark/train_model.py"
echo ""
echo "  2. Start streaming:"
echo "     docker exec spark-master spark-submit \\"
echo "       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\"
echo "       /opt/spark/work/spark/streaming_pipeline.py"
echo ""
echo "  3. Start producer:"
echo "     docker exec spark-master python3 /opt/spark/work/producer/kafka_producer.py --speed 0.5"

`

### scripts/stop_pipeline.sh
**Trạng thái / Ghi chú:** Code logic
`bash
#!/bin/bash
# ============================================================
# stop_pipeline.sh — Graceful shutdown
# ============================================================
set -e

echo "🛑 Stopping Fraud Detection Pipeline..."

echo "  [1/2] Stopping containers..."
docker compose down

echo "  [2/2] Cleaning up..."
# Optional: remove volumes for fresh restart
# docker compose down -v

echo "✅ Pipeline stopped."
echo ""
echo "To also remove data volumes:"
echo "  docker compose down -v"

`

### serving/api.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
FastAPI serving layer for fraud detection results.

Endpoints:
    GET /health           - Health check
    GET /kpis             - Transaction and fraud KPIs
    GET /recent-alerts    - Latest 50 fraud alerts
    GET /stream/alerts    - SSE real-time fraud alert stream
"""
import os
import json
import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
import redis.asyncio as aioredis

from redis_listener import RedisAlertListener


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_CHANNEL = "fraud_alerts"


# Global state
redis_pool: Optional[aioredis.Redis] = None
alert_listener: Optional[RedisAlertListener] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle."""
    global redis_pool, alert_listener

    # Startup
    redis_pool = aioredis.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        decode_responses=True,
        socket_timeout=5,
    )
    alert_listener = RedisAlertListener(
        REDIS_HOST, REDIS_PORT, REDIS_CHANNEL
    )
    asyncio.create_task(alert_listener.start())
    print(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

    yield

    # Shutdown
    await alert_listener.stop()
    await redis_pool.close()


app = FastAPI(
    title="Fraud Detection API",
    description="Real-time fraud detection serving layer",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Response Models
# ============================================================
class HealthResponse(BaseModel):
    status: str
    redis: str


class KPIResponse(BaseModel):
    total_transactions: int
    total_fraud: int
    fraud_rate: float


class AlertItem(BaseModel):
    step: Optional[int] = None
    type: Optional[str] = None
    amount: Optional[float] = None
    nameOrig: Optional[str] = None
    nameDest: Optional[str] = None
    fraud_probability: Optional[float] = None
    oldbalanceOrg: Optional[float] = None
    newbalanceOrig: Optional[float] = None


# ============================================================
# Endpoints
# ============================================================
@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    try:
        await redis_pool.ping()
        redis_status = "connected"
    except Exception:
        redis_status = "disconnected"

    return HealthResponse(status="ok", redis=redis_status)


@app.get("/kpis", response_model=KPIResponse)
async def get_kpis():
    """
    Get transaction and fraud KPI counters.

    These counters are atomically incremented by the Spark streaming
    pipeline via Redis INCRBY.
    """
    total_txn = await redis_pool.get("total_transactions")
    total_fraud = await redis_pool.get("total_fraud")

    total_txn = int(total_txn) if total_txn else 0
    total_fraud = int(total_fraud) if total_fraud else 0
    fraud_rate = (
        (total_fraud / total_txn * 100) if total_txn > 0 else 0.0
    )

    return KPIResponse(
        total_transactions=total_txn,
        total_fraud=total_fraud,
        fraud_rate=round(fraud_rate, 4),
    )


@app.get("/recent-alerts")
async def get_recent_alerts(
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Get the most recent fraud alerts.

    Alerts are stored in a Redis list by the Spark streaming pipeline.
    """
    raw_alerts = await redis_pool.lrange("recent_alerts", 0, limit - 1)
    alerts = []
    for raw in raw_alerts:
        try:
            alerts.append(json.loads(raw))
        except json.JSONDecodeError:
            continue

    return JSONResponse(content={
        "count": len(alerts),
        "alerts": alerts,
    })


@app.get("/stream/alerts")
async def stream_alerts():
    """
    SSE (Server-Sent Events) endpoint for real-time fraud alerts.

    Connect with EventSource in browser:
        const es = new EventSource('/stream/alerts');
        es.onmessage = (e) => console.log(JSON.parse(e.data));
    """
    async def event_generator():
        queue = alert_listener.subscribe()
        try:
            while True:
                try:
                    alert = await asyncio.wait_for(
                        queue.get(), timeout=30.0
                    )
                    yield {"event": "fraud_alert", "data": alert}
                except asyncio.TimeoutError:
                    # Send keepalive to prevent connection timeout
                    yield {"event": "keepalive", "data": ""}
        except asyncio.CancelledError:
            alert_listener.unsubscribe(queue)
            raise

    return EventSourceResponse(event_generator())


@app.get("/model-metrics")
async def get_model_metrics():
    """
    Get cached model metrics (stored by train_model.py).

    Metrics are stored as a Redis hash by the training pipeline.
    """
    metrics = await redis_pool.hgetall("model_metrics")
    if not metrics:
        return JSONResponse(
            content={"message": "No metrics available"},
            status_code=404,
        )
    return JSONResponse(content={
        k: float(v) for k, v in metrics.items()
    })

`

### serving/Dockerfile
**Trạng thái / Ghi chú:** Code logic
`text
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]

`

### serving/redis_listener.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
Redis Pub/Sub listener for fraud alerts.

Subscribes to the 'fraud_alerts' channel and distributes
messages to connected SSE clients via asyncio queues.
"""
import asyncio
import json
from typing import Set

import redis.asyncio as aioredis


class RedisAlertListener:
    """
    Async Redis subscriber that fans out messages to multiple SSE clients.

    Each SSE client gets its own asyncio.Queue via subscribe().
    Messages from Redis are broadcast to all queues.
    """

    def __init__(self, host: str, port: int, channel: str):
        self.host = host
        self.port = port
        self.channel = channel
        self._subscribers: Set[asyncio.Queue] = set()
        self._running = False
        self._task = None

    def subscribe(self) -> asyncio.Queue:
        """Register a new SSE client. Returns a queue to read from."""
        queue = asyncio.Queue(maxsize=100)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        """Remove an SSE client."""
        self._subscribers.discard(queue)

    async def start(self):
        """Start listening to Redis Pub/Sub."""
        self._running = True
        while self._running:
            try:
                conn = aioredis.Redis(
                    host=self.host, port=self.port,
                    decode_responses=True,
                )
                pubsub = conn.pubsub()
                await pubsub.subscribe(self.channel)
                print(
                    f"📡 Subscribed to Redis channel: {self.channel}"
                )

                async for message in pubsub.listen():
                    if not self._running:
                        break
                    if message["type"] != "message":
                        continue

                    data = message["data"]

                    # Broadcast to all SSE clients
                    dead_queues = set()
                    for queue in self._subscribers:
                        try:
                            queue.put_nowait(data)
                        except asyncio.QueueFull:
                            dead_queues.add(queue)

                    # Clean up disconnected clients
                    self._subscribers -= dead_queues

                await pubsub.unsubscribe(self.channel)
                await conn.close()

            except Exception as e:
                if self._running:
                    print(f"⚠️ Redis listener error: {e}. Retrying in 5s...")
                    await asyncio.sleep(5)

    async def stop(self):
        """Stop the listener."""
        self._running = False
        # Unblock all waiting queues
        for queue in self._subscribers:
            try:
                queue.put_nowait(
                    json.dumps({"event": "shutdown"})
                )
            except asyncio.QueueFull:
                pass
        self._subscribers.clear()

`

### serving/requirements.txt
**Trạng thái / Ghi chú:** Code logic
`text
fastapi==0.115.0
uvicorn==0.30.0
redis[hiredis]==5.0.0
sse-starlette==2.0.0
pydantic==2.7.0

`

### spark/preprocessing.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
Feature engineering for PaySim fraud detection.

All transformations use PySpark ONLY — no Pandas.
This module is shared between train_model.py and streaming_pipeline.py.
"""
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
    StandardScaler,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# PaySim transaction types
TRANSACTION_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]

# Numeric features to assemble
NUMERIC_FEATURES = [
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "balance_diff_orig",
    "balance_diff_dest",
    "amount_ratio",
    "is_zero_balance_orig",
    "is_large_amount",
]

# Final feature vector column names (after StringIndexer)
ASSEMBLER_INPUT_COLS = NUMERIC_FEATURES + ["type_index"]


def add_engineered_features(df: DataFrame) -> DataFrame:
    """
    Add domain-specific features for fraud detection.

    These features capture common fraud patterns in e-wallet transactions:
    - Draining account to zero (TRANSFER/CASH_OUT fraud pattern)
    - Amount larger than current balance
    - Mismatches between expected and actual balance changes
    """
    result = df

    # Balance difference: how much origin account actually lost
    result = result.withColumn(
        "balance_diff_orig",
        F.col("oldbalanceOrg") - F.col("newbalanceOrig")
    )

    # Balance difference: how much destination account actually gained
    result = result.withColumn(
        "balance_diff_dest",
        F.col("newbalanceDest") - F.col("oldbalanceDest")
    )

    # Amount as ratio of origin balance (high ratio = suspicious)
    # +1 to avoid division by zero
    result = result.withColumn(
        "amount_ratio",
        F.col("amount") / (F.col("oldbalanceOrg") + F.lit(1.0))
    )

    # Binary: origin account drained to zero after transaction
    result = result.withColumn(
        "is_zero_balance_orig",
        F.when(F.col("newbalanceOrig") == 0.0, 1.0).otherwise(0.0)
    )

    # Binary: transaction amount > 200,000 (PaySim units)
    result = result.withColumn(
        "is_large_amount",
        F.when(F.col("amount") > 200000.0, 1.0).otherwise(0.0)
    )

    return result


def create_feature_pipeline() -> Pipeline:
    """
    Build a PySpark ML Pipeline for feature engineering.

    Steps:
    1. StringIndexer: type (categorical) → type_index (numeric)
    2. VectorAssembler: combine all features → raw_features
    3. StandardScaler: normalize → features

    Returns:
        Pipeline ready to .fit() on training data
    """
    # Encode transaction type as numeric index
    type_indexer = StringIndexer(
        inputCol="type",
        outputCol="type_index",
        handleInvalid="keep",
    )

    # Combine all numeric features into a single vector
    assembler = VectorAssembler(
        inputCols=ASSEMBLER_INPUT_COLS,
        outputCol="raw_features",
        handleInvalid="keep",
    )

    # Scale features to zero mean, unit variance
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False,  # Sparse vectors don't support withMean=True
    )

    return Pipeline(stages=[type_indexer, assembler, scaler])


def prepare_dataframe(df: DataFrame) -> DataFrame:
    """
    Prepare raw PaySim DataFrame for ML pipeline.

    Casts types, adds engineered features, renames label column.
    """
    result = df

    # Ensure correct types (CSV reads everything as string)
    result = (
        result
        .withColumn("step", F.col("step").cast("integer"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("oldbalanceOrg", F.col("oldbalanceOrg").cast("double"))
        .withColumn("newbalanceOrig", F.col("newbalanceOrig").cast("double"))
        .withColumn("oldbalanceDest", F.col("oldbalanceDest").cast("double"))
        .withColumn("newbalanceDest", F.col("newbalanceDest").cast("double"))
        .withColumn("isFraud", F.col("isFraud").cast("double"))
        .withColumn("isFlaggedFraud",
                     F.col("isFlaggedFraud").cast("integer"))
    )

    # Add engineered features
    result = add_engineered_features(result)

    # Rename label column for ML
    result = result.withColumnRenamed("isFraud", "label")

    return result

`

### spark/requirements.txt
**Trạng thái / Ghi chú:** Code logic
`text
pyspark==3.5.3
kafka-python==2.0.2
redis==5.0.0
mlflow==2.12.0
numpy==1.24.4

`

### spark/streaming_pipeline.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
Spark Structured Streaming pipeline for real-time fraud detection.

Consumes transactions from Kafka, applies ML model for fraud prediction,
writes results to HDFS (Parquet) and publishes fraud alerts to Redis.

Usage (inside spark-master container):
    spark-submit --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        /opt/spark/work/spark/streaming_pipeline.py
"""
import os
import sys
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
)
from pyspark.ml import PipelineModel
import redis

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from preprocessing import add_engineered_features  # noqa: E402


# ============================================================
# Configuration
# ============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
MODEL_PATH = os.getenv(
    "MODEL_PATH", "/opt/spark/work/models/fraud_pipeline_model"
)
HDFS_OUTPUT = os.getenv(
    "HDFS_OUTPUT", "hdfs://namenode:8020/datalake/transactions"
)
HDFS_CHECKPOINT = os.getenv(
    "HDFS_CHECKPOINT", "hdfs://namenode:8020/datalake/checkpoints/streaming"
)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_CHANNEL = "fraud_alerts"


# ============================================================
# PaySim JSON schema (matches producer output)
# ============================================================
PAYSIM_SCHEMA = StructType([
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
])


def get_redis_client():
    """Create Redis client with retry."""
    try:
        client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            decode_responses=True, socket_timeout=5,
        )
        client.ping()
        print(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return client
    except Exception as e:
        print(f"⚠️ Redis connection failed: {e}")
        print("   Fraud alerts will NOT be published.")
        return None


def process_batch(batch_df, batch_id, model, redis_client):
    """
    Process each micro-batch:
    1. Apply feature engineering
    2. Run ML prediction
    3. Write all results to HDFS (Parquet)
    4. Publish fraud alerts to Redis
    """
    if batch_df.isEmpty():
        return

    row_count = batch_df.count()

    # ------------------------------------------------------------------
    # Step 1: Feature engineering
    # ------------------------------------------------------------------
    # Cast isFraud to double for consistency with training label
    featured_df = batch_df.withColumn(
        "label", F.col("isFraud").cast("double")
    )
    featured_df = add_engineered_features(featured_df)

    # ------------------------------------------------------------------
    # Step 2: ML prediction
    # ------------------------------------------------------------------
    predictions_df = model.transform(featured_df)

    # Extract fraud probability from probability vector
    # probability is a Vector [prob_class_0, prob_class_1]
    extract_fraud_prob = F.udf(
        lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0,
        DoubleType()
    )
    predictions_df = predictions_df.withColumn(
        "fraud_probability", extract_fraud_prob(F.col("probability"))
    )

    # Add processing metadata
    predictions_df = (
        predictions_df
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("dt", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
        .withColumn(
            "hour", F.date_format(F.current_timestamp(), "HH")
        )
    )

    # ------------------------------------------------------------------
    # Step 3: Write to HDFS (Parquet, partitioned by dt/hour)
    # ------------------------------------------------------------------
    # Select only the columns we need for storage
    output_cols = [
        "step", "type", "amount", "nameOrig", "oldbalanceOrg",
        "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud",
        "prediction", "fraud_probability", "processed_at",
        "dt", "hour",
    ]

    output_df = predictions_df.select(output_cols)

    output_df.write \
        .mode("append") \
        .format("parquet") \
        .partitionBy("dt", "hour") \
        .save(HDFS_OUTPUT)

    # ------------------------------------------------------------------
    # Step 4: Publish fraud alerts to Redis
    # ------------------------------------------------------------------
    fraud_count = predictions_df.filter(F.col("prediction") == 1.0).count()

    if redis_client is not None:
        # Update atomic counters
        try:
            redis_client.incrby("total_transactions", row_count)
            redis_client.incrby("total_fraud", fraud_count)
        except Exception:
            pass

        # Publish individual fraud alerts
        if fraud_count > 0:
            fraud_rows = (
                predictions_df
                .filter(F.col("prediction") == 1.0)
                .select(
                    "step", "type", "amount",
                    "nameOrig", "nameDest",
                    "fraud_probability",
                    "oldbalanceOrg", "newbalanceOrig",
                )
                .collect()
            )

            for row in fraud_rows:
                alert = {
                    "step": row["step"],
                    "type": row["type"],
                    "amount": row["amount"],
                    "nameOrig": row["nameOrig"],
                    "nameDest": row["nameDest"],
                    "fraud_probability": round(
                        row["fraud_probability"], 4
                    ),
                    "oldbalanceOrg": row["oldbalanceOrg"],
                    "newbalanceOrig": row["newbalanceOrig"],
                }
                try:
                    redis_client.publish(
                        REDIS_CHANNEL, json.dumps(alert)
                    )
                    # Also keep in a list for REST API
                    redis_client.lpush(
                        "recent_alerts", json.dumps(alert)
                    )
                    redis_client.ltrim("recent_alerts", 0, 499)
                except Exception:
                    pass

    print(
        f"[Batch {batch_id}] "
        f"Processed: {row_count} | "
        f"Fraud detected: {fraud_count}"
    )


def main():
    """Start streaming pipeline."""
    print("=" * 60)
    print("🚀 Real-time Fraud Detection — Streaming Pipeline")
    print("=" * 60)

    spark = (
        SparkSession.builder
        .appName("FraudDetection-Streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        )
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # Load ML model
    # ------------------------------------------------------------------
    print(f"\n📂 Loading model from {MODEL_PATH}...")
    try:
        model = PipelineModel.load(MODEL_PATH)
        print("✅ Model loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load model: {e}")
        print("   Run train_model.py first!")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Connect to Redis
    # ------------------------------------------------------------------
    redis_client = get_redis_client()

    # ------------------------------------------------------------------
    # Read from Kafka
    # ------------------------------------------------------------------
    print(f"\n📡 Connecting to Kafka topic '{KAFKA_TOPIC}'...")
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON from Kafka value
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(
            F.from_json(F.col("json_str"), PAYSIM_SCHEMA).alias("data")
        )
        .select("data.*")
    )

    # Add event timestamp with watermark for late data handling
    watermarked_stream = (
        parsed_stream
        .withColumn("event_time", F.current_timestamp())
        .withWatermark("event_time", "10 minutes")
    )

    # ------------------------------------------------------------------
    # Start foreachBatch processing
    # ------------------------------------------------------------------
    print("\n⏳ Starting streaming query...")
    print(f"   HDFS output: {HDFS_OUTPUT}")
    print(f"   Redis channel: {REDIS_CHANNEL}")
    print("   Press Ctrl+C to stop.\n")

    query = (
        watermarked_stream.writeStream
        .foreachBatch(
            lambda df, bid: process_batch(df, bid, model, redis_client)
        )
        .option("checkpointLocation", HDFS_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

`

### spark/train_model.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
Offline ML training for PaySim fraud detection.

Trains RandomForest and GBT classifiers using PySpark MLlib.
Handles class imbalance via weight column.
Logs metrics to MLflow and saves best model.

Usage (inside spark-master container):
    spark-submit --master spark://spark-master:7077 \
        /opt/spark/work/spark/train_model.py
"""
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    RandomForestClassifier,
    GBTClassifier,
)
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Add parent dir to path so we can import preprocessing
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from preprocessing import (  # noqa: E402
    create_feature_pipeline,
    prepare_dataframe,
)


CSV_PATH = os.getenv(
    "CSV_PATH", "/opt/spark/work/data/paysim_transactions.csv"
)
MODEL_OUTPUT = os.getenv(
    "MODEL_PATH", "/opt/spark/work/models/fraud_pipeline_model"
)
MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI", "file:///opt/spark/work/models/mlruns"
)


def add_class_weights(df):
    """
    Add weight column to handle class imbalance.

    PaySim has ~0.13% fraud — without weights, model ignores minority class.
    Weight formula: total_samples / (2 * class_count)
    """
    total = df.count()
    fraud_count = df.filter(F.col("label") == 1.0).count()
    non_fraud_count = total - fraud_count

    print(f"\n📊 Class Distribution:")
    print(f"   Non-Fraud: {non_fraud_count:,} ({non_fraud_count/total*100:.2f}%)")
    print(f"   Fraud:     {fraud_count:,} ({fraud_count/total*100:.2f}%)")

    # Weight: inverse of class frequency
    weight_fraud = total / (2.0 * fraud_count) if fraud_count > 0 else 1.0
    weight_non_fraud = total / (2.0 * non_fraud_count)

    print(f"   Weight Non-Fraud: {weight_non_fraud:.4f}")
    print(f"   Weight Fraud:     {weight_fraud:.4f}")

    weighted_df = df.withColumn(
        "weight",
        F.when(F.col("label") == 1.0, F.lit(weight_fraud))
        .otherwise(F.lit(weight_non_fraud))
    )
    return weighted_df


def evaluate_model(predictions, model_name: str) -> dict:
    """Compute all classification metrics."""
    evaluators = {
        "accuracy": MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction",
            metricName="accuracy"
        ),
        "precision": MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction",
            metricName="weightedPrecision"
        ),
        "recall": MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction",
            metricName="weightedRecall"
        ),
        "f1": MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction",
            metricName="f1"
        ),
        "auc": BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        ),
    }

    metrics = {}
    for name, evaluator in evaluators.items():
        metrics[name] = evaluator.evaluate(predictions)

    print(f"\n📈 {model_name} Metrics:")
    for name, value in metrics.items():
        print(f"   {name:>10}: {value:.6f}")

    return metrics


def train_and_evaluate(spark):
    """Main training pipeline."""
    print("=" * 60)
    print("🚀 PaySim Fraud Detection — Model Training")
    print("=" * 60)

    # ------------------------------------------------------------------
    # 1. Load data
    # ------------------------------------------------------------------
    print(f"\n📂 Loading data from {CSV_PATH}...")
    raw_df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    print(f"   Total rows: {raw_df.count():,}")

    # ------------------------------------------------------------------
    # 2. Prepare features
    # ------------------------------------------------------------------
    print("\n🔧 Preparing features...")
    prepared_df = prepare_dataframe(raw_df)

    # Add class weights for imbalanced handling
    weighted_df = add_class_weights(prepared_df)

    # ------------------------------------------------------------------
    # 3. Train/Test split (80/20, stratified by label)
    # ------------------------------------------------------------------
    train_df, test_df = weighted_df.randomSplit([0.8, 0.2], seed=42)
    print(f"\n📊 Split:")
    print(f"   Train: {train_df.count():,}")
    print(f"   Test:  {test_df.count():,}")

    # ------------------------------------------------------------------
    # 4. Build feature pipeline
    # ------------------------------------------------------------------
    feature_pipeline = create_feature_pipeline()

    # ------------------------------------------------------------------
    # 5. Train Random Forest
    # ------------------------------------------------------------------
    print("\n🌲 Training Random Forest...")
    t0 = time.time()

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        weightCol="weight",
        numTrees=100,
        maxDepth=10,
        seed=42,
    )

    rf_pipeline = Pipeline(stages=feature_pipeline.getStages() + [rf])

    # CrossValidator for hyperparameter tuning
    rf_param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [50, 100])
        .addGrid(rf.maxDepth, [8, 10])
        .build()
    )

    rf_cv = CrossValidator(
        estimator=rf_pipeline,
        estimatorParamMaps=rf_param_grid,
        evaluator=BinaryClassificationEvaluator(
            labelCol="label", metricName="areaUnderROC"
        ),
        numFolds=3,
        seed=42,
    )

    rf_model = rf_cv.fit(train_df)
    rf_time = time.time() - t0
    print(f"   Training time: {rf_time:.1f}s")

    rf_predictions = rf_model.transform(test_df)
    rf_metrics = evaluate_model(rf_predictions, "Random Forest (CV)")

    # ------------------------------------------------------------------
    # 6. Train Gradient Boosted Trees
    # ------------------------------------------------------------------
    print("\n🌳 Training Gradient Boosted Trees...")
    t0 = time.time()

    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        weightCol="weight",
        maxIter=50,
        maxDepth=8,
        seed=42,
    )

    gbt_pipeline = Pipeline(stages=feature_pipeline.getStages() + [gbt])
    gbt_model = gbt_pipeline.fit(train_df)
    gbt_time = time.time() - t0
    print(f"   Training time: {gbt_time:.1f}s")

    gbt_predictions = gbt_model.transform(test_df)
    gbt_metrics = evaluate_model(gbt_predictions, "GBT")

    # ------------------------------------------------------------------
    # 7. Compare and select best model
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("🏆 Model Comparison")
    print("=" * 60)
    print(f"{'Metric':<12} {'Random Forest':>15} {'GBT':>15}")
    print("-" * 44)
    for metric in ["accuracy", "precision", "recall", "f1", "auc"]:
        print(
            f"{metric:<12} "
            f"{rf_metrics[metric]:>15.6f} "
            f"{gbt_metrics[metric]:>15.6f}"
        )

    # Select model with higher AUC (best for imbalanced binary classification)
    if rf_metrics["auc"] >= gbt_metrics["auc"]:
        best_model = rf_model.bestModel
        best_name = "Random Forest (CV)"
        best_metrics = rf_metrics
    else:
        best_model = gbt_model
        best_name = "GBT"
        best_metrics = gbt_metrics

    print(f"\n✅ Best model: {best_name} (AUC={best_metrics['auc']:.6f})")

    # ------------------------------------------------------------------
    # 8. MLflow logging
    # ------------------------------------------------------------------
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("fraud-detection")

        with mlflow.start_run(run_name=best_name):
            for metric_name, metric_value in best_metrics.items():
                mlflow.log_metric(metric_name, metric_value)

            mlflow.log_param("model_type", best_name)
            mlflow.log_param("train_size", train_df.count())
            mlflow.log_param("test_size", test_df.count())
            mlflow.log_param("features", len(prepared_df.columns))

        print("📝 Metrics logged to MLflow")
    except Exception as e:
        print(f"⚠️ MLflow logging skipped: {e}")

    # ------------------------------------------------------------------
    # 9. Save best model
    # ------------------------------------------------------------------
    print(f"\n💾 Saving model to {MODEL_OUTPUT}...")
    best_model.write().overwrite().save(MODEL_OUTPUT)
    print("✅ Model saved successfully!")

    print("\n" + "=" * 60)
    print("🎉 Training complete!")
    print("=" * 60)

    return best_metrics


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("FraudDetection-Training")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        train_and_evaluate(spark)
    finally:
        spark.stop()

`

### tests/conftest.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
Pytest fixtures shared across test modules.
"""
import sys
import os
import pytest

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "spark"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "serving"))


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing (local mode)."""
    try:
        from pyspark.sql import SparkSession
        session = (
            SparkSession.builder
            .master("local[2]")
            .appName("FraudDetection-Tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )
        yield session
        session.stop()
    except ImportError:
        pytest.skip("PySpark not installed")


@pytest.fixture
def sample_paysim_data(spark):
    """Create a sample PaySim-like DataFrame for testing."""
    data = [
        # Normal PAYMENT transaction
        (1, "PAYMENT", 9839.64, "C1231006815", 170136.0, 160296.36,
         "M1979787155", 0.0, 0.0, 0, 0),
        # Normal TRANSFER
        (1, "TRANSFER", 181.0, "C1305486145", 181.0, 0.0,
         "C553264065", 0.0, 0.0, 0, 0),
        # Fraudulent TRANSFER (drains account to zero)
        (1, "TRANSFER", 181000.0, "C840083671", 181000.0, 0.0,
         "C38997010", 0.0, 0.0, 1, 0),
        # Fraudulent CASH_OUT (large amount)
        (1, "CASH_OUT", 500000.0, "C2054744914", 500000.0, 0.0,
         "C1286084959", 21182.0, 521182.0, 1, 1),
        # Normal CASH_IN
        (2, "CASH_IN", 1000.0, "C1234567890", 5000.0, 6000.0,
         "M9876543210", 0.0, 0.0, 0, 0),
        # Normal DEBIT
        (3, "DEBIT", 5000.0, "C1111111111", 50000.0, 45000.0,
         "M2222222222", 0.0, 0.0, 0, 0),
    ]

    columns = [
        "step", "type", "amount", "nameOrig",
        "oldbalanceOrg", "newbalanceOrig",
        "nameDest", "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud",
    ]

    return spark.createDataFrame(data, columns)

`

### tests/test_api.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
Integration tests for FastAPI serving layer.

Tests API endpoints with mocked Redis.
"""
import json
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def mock_redis():
    """Create a mock async Redis client."""
    mock = AsyncMock()
    mock.ping = AsyncMock(return_value=True)
    mock.get = AsyncMock(return_value=None)
    mock.lrange = AsyncMock(return_value=[])
    mock.hgetall = AsyncMock(return_value={})
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def client(mock_redis):
    """Create a test client with mocked Redis."""
    import sys
    import os
    sys.path.insert(
        0, os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "serving"
        )
    )

    # Patch Redis before importing the app
    with patch("api.aioredis") as mock_aioredis:
        mock_aioredis.Redis.return_value = mock_redis
        from api import app
        import api
        api.redis_pool = mock_redis

        with TestClient(app, raise_server_exceptions=False) as c:
            yield c


class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_health_ok(self, client, mock_redis):
        """Health endpoint returns 200 with status ok."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"


class TestKPIsEndpoint:
    """Tests for GET /kpis."""

    def test_kpis_empty(self, client, mock_redis):
        """KPIs return zeros when no data."""
        mock_redis.get = AsyncMock(return_value=None)
        response = client.get("/kpis")
        assert response.status_code == 200
        data = response.json()
        assert data["total_transactions"] == 0
        assert data["total_fraud"] == 0
        assert data["fraud_rate"] == 0.0

    def test_kpis_with_data(self, client, mock_redis):
        """KPIs return correct values."""
        mock_redis.get = AsyncMock(
            side_effect=lambda k: "1000" if k == "total_transactions"
            else "13" if k == "total_fraud" else None
        )
        response = client.get("/kpis")
        assert response.status_code == 200
        data = response.json()
        assert data["total_transactions"] == 1000
        assert data["total_fraud"] == 13
        assert data["fraud_rate"] == pytest.approx(1.3, rel=0.01)


class TestRecentAlertsEndpoint:
    """Tests for GET /recent-alerts."""

    def test_recent_alerts_empty(self, client, mock_redis):
        """Returns empty list when no alerts."""
        mock_redis.lrange = AsyncMock(return_value=[])
        response = client.get("/recent-alerts")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert data["alerts"] == []

    def test_recent_alerts_with_data(self, client, mock_redis):
        """Returns parsed alerts from Redis list."""
        alerts = [
            json.dumps({"type": "TRANSFER", "amount": 500000}),
            json.dumps({"type": "CASH_OUT", "amount": 200000}),
        ]
        mock_redis.lrange = AsyncMock(return_value=alerts)
        response = client.get("/recent-alerts")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert data["alerts"][0]["type"] == "TRANSFER"

    def test_recent_alerts_limit(self, client, mock_redis):
        """Respects limit parameter."""
        mock_redis.lrange = AsyncMock(return_value=[])
        response = client.get("/recent-alerts?limit=10")
        assert response.status_code == 200
        mock_redis.lrange.assert_called_once_with("recent_alerts", 0, 9)


class TestModelMetricsEndpoint:
    """Tests for GET /model-metrics."""

    def test_no_metrics(self, client, mock_redis):
        """Returns 404 when no metrics available."""
        mock_redis.hgetall = AsyncMock(return_value={})
        response = client.get("/model-metrics")
        assert response.status_code == 404

    def test_with_metrics(self, client, mock_redis):
        """Returns metrics as floats."""
        mock_redis.hgetall = AsyncMock(return_value={
            "accuracy": "0.998",
            "auc": "0.975",
        })
        response = client.get("/model-metrics")
        assert response.status_code == 200
        data = response.json()
        assert data["accuracy"] == pytest.approx(0.998)
        assert data["auc"] == pytest.approx(0.975)

`

### tests/test_preprocessing.py
**Trạng thái / Ghi chú:** Code logic
`python
"""
Unit tests for spark/preprocessing.py.

Tests feature engineering functions and ML pipeline creation.
All tests use PySpark local mode — no cluster needed.
"""
import pytest


class TestAddEngineeredFeatures:
    """Test add_engineered_features() function."""

    def test_balance_diff_orig(self, sample_paysim_data):
        """Balance diff should equal old - new balance."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        row = result.collect()[0]

        expected = row["oldbalanceOrg"] - row["newbalanceOrig"]
        assert row["balance_diff_orig"] == pytest.approx(expected)

    def test_balance_diff_dest(self, sample_paysim_data):
        """Dest balance diff should equal new - old."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        # Use the CASH_OUT row (index 3) which has non-zero dest balances
        rows = result.collect()
        cashout_row = rows[3]

        expected = (
            cashout_row["newbalanceDest"]
            - cashout_row["oldbalanceDest"]
        )
        assert cashout_row["balance_diff_dest"] == pytest.approx(expected)

    def test_amount_ratio(self, sample_paysim_data):
        """Amount ratio = amount / (oldbalanceOrg + 1)."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        row = result.collect()[0]

        expected = row["amount"] / (row["oldbalanceOrg"] + 1.0)
        assert row["amount_ratio"] == pytest.approx(expected, rel=1e-4)

    def test_is_zero_balance_fraud(self, sample_paysim_data):
        """Fraud transaction that drains account should have is_zero_balance=1."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        # Row index 2: fraud TRANSFER, newbalanceOrig = 0
        fraud_row = result.collect()[2]

        assert fraud_row["is_zero_balance_orig"] == 1.0

    def test_is_zero_balance_normal(self, sample_paysim_data):
        """Normal transaction with remaining balance should have is_zero_balance=0."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        # Row index 0: normal PAYMENT, newbalanceOrig = 160296.36
        normal_row = result.collect()[0]

        assert normal_row["is_zero_balance_orig"] == 0.0

    def test_is_large_amount(self, sample_paysim_data):
        """Transaction > 200,000 should be flagged as large."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        rows = result.collect()

        # Row 3: CASH_OUT 500,000 → large
        assert rows[3]["is_large_amount"] == 1.0
        # Row 0: PAYMENT 9,839.64 → not large
        assert rows[0]["is_large_amount"] == 0.0

    def test_output_columns_added(self, sample_paysim_data):
        """Should add exactly 5 engineered feature columns."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        new_cols = set(result.columns) - set(sample_paysim_data.columns)

        expected_new = {
            "balance_diff_orig",
            "balance_diff_dest",
            "amount_ratio",
            "is_zero_balance_orig",
            "is_large_amount",
        }
        assert new_cols == expected_new


class TestPrepareDataframe:
    """Test prepare_dataframe() function."""

    def test_label_column_created(self, sample_paysim_data):
        """isFraud should be renamed to label."""
        from preprocessing import prepare_dataframe

        result = prepare_dataframe(sample_paysim_data)
        assert "label" in result.columns
        assert "isFraud" not in result.columns

    def test_label_values(self, sample_paysim_data):
        """Label should be double (0.0 or 1.0)."""
        from preprocessing import prepare_dataframe

        result = prepare_dataframe(sample_paysim_data)
        labels = [row["label"] for row in result.select("label").collect()]

        assert all(isinstance(l, float) for l in labels)
        assert set(labels) == {0.0, 1.0}


class TestCreateFeaturePipeline:
    """Test create_feature_pipeline() function."""

    def test_pipeline_stages(self):
        """Pipeline should have 3 stages: StringIndexer, VectorAssembler, StandardScaler."""
        from preprocessing import create_feature_pipeline

        pipeline = create_feature_pipeline()
        stages = pipeline.getStages()

        assert len(stages) == 3
        assert "StringIndexer" in type(stages[0]).__name__
        assert "VectorAssembler" in type(stages[1]).__name__
        assert "StandardScaler" in type(stages[2]).__name__

    def test_pipeline_fit_transform(self, sample_paysim_data):
        """Pipeline should fit and produce 'features' column."""
        from preprocessing import (
            create_feature_pipeline,
            prepare_dataframe,
        )

        prepared = prepare_dataframe(sample_paysim_data)
        pipeline = create_feature_pipeline()

        model = pipeline.fit(prepared)
        result = model.transform(prepared)

        assert "features" in result.columns
        assert "type_index" in result.columns
        assert result.count() == sample_paysim_data.count()

`

### docker-compose.yml
**Trạng thái / Ghi chú:** Code logic
`yaml
services:
  # ============================================================
  # KAFKA (KRaft Mode - NO Zookeeper)
  # ============================================================
  kafka:
    image: apache/kafka:3.7.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LISTENERS: PLAINTEXT://:29092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 30s
    networks:
      - fraud-network

  # ============================================================
  # HADOOP HDFS
  # ============================================================
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    ports:
      - "9870:9870"
      - "8020:8020"
    environment:
      - CLUSTER_NAME=fraud-cluster
      - CORE_CONF_fs_defaultFS=hdfs://namenode:8020
      - HDFS_CONF_dfs_namenode_rpc___address=namenode:8020
      - HDFS_CONF_dfs_replication=1
    volumes:
      - namenode_data:/hadoop/dfs/name
    healthcheck:
      test: curl -f http://localhost:9870/ || exit 1
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 30s
    networks:
      - fraud-network

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    depends_on:
      namenode:
        condition: service_healthy
    environment:
      - SERVICE_PRECONDITION=namenode:9870
      - CORE_CONF_fs_defaultFS=hdfs://namenode:8020
    volumes:
      - datanode_data:/hadoop/dfs/data
    networks:
      - fraud-network

  # ============================================================
  # SPARK (Custom image with ML libraries)
  # ============================================================
  spark-master:
    build:
      context: .
      dockerfile: Dockerfile
    image: fraud-spark:3.5.3
    container_name: spark-master
    ports:
      - "8081:8080"
      - "7077:7077"
    environment:
      - SPARK_MODE=master
    command: >
      /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
      --ip 0.0.0.0 --port 7077 --webui-port 8080
    volumes:
      - ./spark:/opt/spark/work/spark
      - ./producer:/opt/spark/work/producer
      - ./models:/opt/spark/work/models
      - ./data:/opt/spark/work/data
    healthcheck:
      test: curl -f http://localhost:8080/ || exit 1
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 20s
    networks:
      - fraud-network

  spark-worker:
    image: fraud-spark:3.5.3
    container_name: spark-worker
    depends_on:
      spark-master:
        condition: service_healthy
    environment:
      - SPARK_MASTER=spark://spark-master:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2G
    command: >
      /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker
      spark://spark-master:7077
    volumes:
      - ./spark:/opt/spark/work/spark
      - ./producer:/opt/spark/work/producer
      - ./models:/opt/spark/work/models
      - ./data:/opt/spark/work/data
    networks:
      - fraud-network

  # ============================================================
  # REDIS (Pub/Sub + Alert storage)
  # ============================================================
  redis:
    image: redis:7-alpine
    container_name: redis
    ports:
      - "6379:6379"
    command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru
    healthcheck:
      test: redis-cli ping | grep PONG || exit 1
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 5s
    networks:
      - fraud-network

  # ============================================================
  # FASTAPI SERVING LAYER
  # ============================================================
  serving:
    build:
      context: ./serving
      dockerfile: Dockerfile
    container_name: serving
    ports:
      - "8000:8000"
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
    depends_on:
      redis:
        condition: service_healthy
    healthcheck:
      test: python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1
      interval: 15s
      timeout: 10s
      retries: 3
      start_period: 10s
    networks:
      - fraud-network

networks:
  fraud-network:
    driver: bridge

volumes:
  namenode_data:
  datanode_data:

`

### Dockerfile
**Trạng thái / Ghi chú:** Code logic
`text
FROM apache/spark:3.5.3

USER root

# Install Python ML and streaming dependencies (NO Selenium/Chrome)
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    redis==5.0.0 \
    pyspark==3.5.3 \
    mlflow==2.12.0 \
    numpy==1.24.4

RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2

USER spark

`## 4. TRẠNG THÁI TỪNG THÀNH PHẦN

| Component | File(s) | Trạng thái | Ghi chú |
|-----------|---------|------------|---------|
| Kafka Producer | `producer/kafka_producer.py` | ✅ Hoàn chỉnh | Đọc CSV, mock realtime streaming. |
| Spark Streaming | `spark/streaming_pipeline.py` | ✅ Hoàn chỉnh | Đọc Kafka, chạy model predict, đẩy HDFS & Redis. |
| ML Training | `spark/train_model.py` | ✅ Hoàn chỉnh | Train RF/GBT, xử lý mất cân bằng data, MLflow log. |
| FastAPI Serving | `serving/api.py`, `redis_listener.py` | ✅ Hoàn chỉnh | API endpoints, SSE stream real-time từ Redis. |
| Redis Integration | `spark/streaming_pipeline.py`, `serving/redis_listener.py` | ✅ Hoàn chỉnh | Pub/Sub cho SSE, List cho mảng alert gần nhất. |
| Docker Compose | `docker-compose.yml`, `Dockerfile` | ✅ Hoàn chỉnh | 7 services đồng bộ (Kafka, HDFS, Spark, Redis, FastAPI). |
| Databricks Notebooks | `databricks/01_bronze...`, `02_silver...`, `03_gold...` | ✅ Hoàn chỉnh | Đã tạo cấu trúc Medallion Data Lakehouse (Bronze/Silver/Gold). |
| Unit Tests | `tests/test_api.py`, `test_preprocessing.py` | ✅ Hoàn chỉnh | Kiểm thử pre-processing logic & integration test API endpoints. |
| CI/CD | `.github/workflows/ci.yml` | ✅ Hoàn chỉnh | Lint (flake8), PyTest, Validate schema docker-compose tự động qua GitHub Actions. |

## 5. DEPENDENCIES THỰC TẾ

### Các file `requirements.txt`
**`producer/requirements.txt`**
```text
kafka-python==2.0.2
```

**`serving/requirements.txt`**
```text
fastapi==0.115.0
uvicorn==0.30.0
redis[hiredis]==5.0.0
sse-starlette==2.0.0
pydantic==2.7.0
```

**`spark/requirements.txt`**
```text
pyspark==3.5.3
kafka-python==2.0.2
redis==5.0.0
mlflow==2.12.0
numpy==1.24.4
```

### Docker Images (from `docker-compose.yml` & `Dockerfile`)
- `apache/kafka:3.7.0` (Kafka KRaft mode - Không Zookeeper)
- `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` (Hadoop HDFS)
- `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` (Hadoop HDFS)
- `apache/spark:3.5.3` (Base image cho cluster nội bộ)
- `redis:7-alpine` (Redis In-Memory DB)
- `python:3.11-slim` (Image base cho Serving layer API)

## 6. VẤN ĐỀ / LỖI ĐANG TỒN TẠI
- **Code sạch**: Quét bằng script Python KHÔNG phát hiện bất kỳ `TODO`, `FIXME` hay các biến Hardcode tạm mang tính phá hoại lớn nào. Code được tổ chức module hóa tốt.
- **Rủi ro bỏ lỗi im lặng (Silent failure risk)**: Có 3 lệnh `pass` được bắt gặp trong block `except Exception` rải rác ở luồng kết nối cơ sở dữ liệu:
  - `serving/redis_listener.py` (Dòng 92)
  - `spark/streaming_pipeline.py` (Dòng 161, 200) 
  *(Lưu ý: Nếu ngoại lệ liên tục diễn ra do mất kết nối Redis, hệ thống sẽ bỏ qua thay vì crash, khiến việc debug khó khăn vì log sẽ bị trống dẫu cảnh báo không được push).*

## 7. LUỒNG DỮ LIỆU THỰC TẾ (SO KHỚP VỚI CODE)
Mô tả Data Flow chính xác diễn ra dưới nền tảng:
1. **Producer (`producer/kafka_producer.py`)**: Đọc file CSV tĩnh trên đĩa (`paysim_transactions.csv`), parse và chuyển từng dòng thành bản ghi JSON. Sau đó bắn tin nhắn vào Message Queue là topic `transactions` trên cụm **Kafka** ở port `29092` với tham số điều chỉnh độ trễ giả lập luồng.
2. **Stream Processor (`spark/streaming_pipeline.py`)**: 
   - Đọc JSON liên tục từ Kafka (Micro-batch khoảng 10 giây/lần), xử lý độ trễ ngẫu nhiên qua hàm Watermark `event_time` 10 phút.
   - Thêm cột feature mới (feature engineering).
   - Đẩy qua Machine Learning Model (Tải tệp `.load` model đã Train) để trích xuất xuất xác suất lỗi `fraud_probability`.
   - **Tách 2 nhánh rẽ Data Sink:**
     - Đầu 1: Ghi hàng loạt (Append mode) file định dạng nén tối ưu `parquet` đẩy xuống kho **HDFS** (`hdfs://namenode:8020/datalake/transactions`), phân chia vách partition theo ngày (`dt`) và giờ (`hour`).
     - Đầu 2: Lọc các giao dịch bị mô hình dự đoán là Gian lận (`prediction == 1.0`). Đẩy một gói Json String ngược lên mạng **Redis**. Một bản vào kênh Pub/Sub mang tên `fraud_alerts`, một bản dồn vào danh sách (List buffer) `recent_alerts`. Đồng hành việc đếm tổng số GD đẩy vào key `total_transactions`.
3. **Data Serving (`serving/api.py`)**:
   - Sử dụng Framework **FastAPI**, móc ngược vào DB In-memory **Redis** để Query số vội truyền vào các API REST lấy số liệu nhanh.
   - Sử dụng thư viện `sse-starlette` chạy Async Task chạy ngầm kết nối Redis Subscribe để nghe tín hiệu. Push gói dữ liệu qua Server-Sent Events chớp nhoáng (độ trễ siêu thấp) tới thiết bị cuối phía Frontend mà ko bắt Client tải lại trang.
4. **Offline Processing / Cloud Analytics (`databricks/*.py`)**: Giả định code trên cụm Spark đám mây. Đọc lấy nguồn Data từ HDFS để đẩy lên qua ETL Bronze -> Bóc tách Train model bạc Silver -> Tổng hợp KPI Gold - *(Trong workspace chỉ đóng vai trò script chạy Local/tham chiếu)*.

## 8. NHỮNG GÌ CÒN THIẾU DỰA TRÊN THỰC TRẠNG
- **Frontend Dashboard hoàn toàn vắng bóng:** Mặc dù tài liệu sơ đồ và `README.md` trong phần danh sách chia task nhóm thực hiện có nhắc: *"Frontend Dashboard (Next.js)"*, tuy nhiên tìm đỏ mắt quét toàn bộ thư mục workspace không hề tồn tại bất cứ file `.tsx`, `page.tsx`, tệp tin `package.json` React hay thư mục của front-end nào được triển khai tại đây. Dự án đang khuyết hẳn giao diện người dùng.
- **Data Sync File Data Migration HDFS -> Databricks:** Các đoạn file Databricks Python Scripts lưu hành chỉ là mô tập tính để chạy tay. Hiện không có công cụ/Cơ chế trung gian (vd như Airflow hay Rclone) tự động lấy (fetch) các phân vùng file data định dạng Parquet nặng nề nằm ở HDFS DataNode chuyển thẳng lên hệ thống lưu trữ phân tán DBFS Cloud Storage. Trạng thái migration trong README cũng viết rằng: *"Upload Parquet files (Mũi tên chĩa xuống)"* - mang hàm ý phải thao tác thủ công.
