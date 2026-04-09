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
