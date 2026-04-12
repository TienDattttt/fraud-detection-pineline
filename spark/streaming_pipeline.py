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
from preprocessing import (  # noqa: E402
    add_engineered_features,
    clean_paysim_dataframe,
)


# ============================================================
# Configuration
# ============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
MODEL_PATH = os.getenv(
    "MODEL_PATH", "/opt/spark/work/models/fraud_pipeline_model"
)
BLACKLIST_PATH = os.getenv(
    "BLACKLIST_PATH", "/opt/spark/work/data/blacklist_accounts.txt"
)
BLACKLIST_ACCOUNTS = tuple(
    value.strip()
    for value in os.getenv("BLACKLIST_ACCOUNTS", "").split(",")
    if value.strip()
)
BLACKLIST_TRANSFER_TYPES = tuple(
    value.strip().upper()
    for value in os.getenv(
        "BLACKLIST_TRANSFER_TYPES", "TRANSFER"
    ).split(",")
    if value.strip()
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


def load_blacklist_accounts(
    path=BLACKLIST_PATH,
    inline_accounts=BLACKLIST_ACCOUNTS,
):
    """Load blacklisted destination accounts from env and/or file."""
    accounts = {
        account.strip()
        for account in inline_accounts
        if account and account.strip()
    }

    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            for raw_line in file_obj:
                account = raw_line.strip()
                if account and not account.startswith("#"):
                    accounts.add(account)
    except FileNotFoundError:
        if not accounts:
            print(f"[WARN] Blacklist file not found: {path}")
    except Exception as e:
        print(f"[WARN] Failed to load blacklist file {path}: {e}")

    loaded_accounts = sorted(accounts)
    print(
        f"[INFO] Loaded {len(loaded_accounts)} blacklisted accounts"
    )
    return loaded_accounts


def add_rule_based_alerts(df, blacklist_accounts):
    """Combine ML fraud prediction with a blacklist transfer rule."""
    result = df
    blacklist_values = tuple(blacklist_accounts)
    transaction_type = F.upper(
        F.coalesce(F.col("type"), F.lit(""))
    )

    is_ml_alert = F.when(F.col("prediction") == 1.0, 1.0).otherwise(0.0)
    result = result.withColumn("is_ml_alert", is_ml_alert)

    if blacklist_values:
        is_blacklist_destination = F.when(
            F.col("nameDest").isin(*blacklist_values), 1.0
        ).otherwise(0.0)
    else:
        is_blacklist_destination = F.lit(0.0)

    result = result.withColumn(
        "is_blacklist_destination", is_blacklist_destination
    )

    if blacklist_values and BLACKLIST_TRANSFER_TYPES:
        is_rule_alert = F.when(
            (F.col("nameDest").isin(*blacklist_values))
            & (transaction_type.isin(*BLACKLIST_TRANSFER_TYPES)),
            1.0
        ).otherwise(0.0)
    else:
        is_rule_alert = F.lit(0.0)

    result = result.withColumn("is_rule_alert", is_rule_alert)
    result = result.withColumn(
        "is_alert",
        F.when(
            (F.col("is_ml_alert") == 1.0)
            | (F.col("is_rule_alert") == 1.0),
            1.0
        ).otherwise(0.0)
    )
    result = result.withColumn(
        "alert_source",
        F.when(
            (F.col("is_ml_alert") == 1.0)
            & (F.col("is_rule_alert") == 1.0),
            F.lit("ML+BLACKLIST_RULE")
        )
        .when(F.col("is_rule_alert") == 1.0, F.lit("BLACKLIST_RULE"))
        .when(F.col("is_ml_alert") == 1.0, F.lit("ML_MODEL"))
        .otherwise(F.lit("NONE"))
    )

    return result


def process_batch(batch_df, batch_id, model, redis_client, blacklist_accounts):
    """
    Process each micro-batch:
    1. Apply feature engineering
    2. Run ML prediction
    3. Apply rule-based alerts (blacklist)
    4. Write all results to HDFS (Parquet)
    5. Publish fraud alerts to Redis
    """
    if batch_df.isEmpty():
        return

    raw_count = batch_df.count()

    # ------------------------------------------------------------------
    # Step 1: Feature engineering
    # ------------------------------------------------------------------
    cleaned_df = clean_paysim_dataframe(batch_df)
    row_count = cleaned_df.count()
    dropped_count = raw_count - row_count

    if row_count == 0:
        print(
            f"[Batch {batch_id}] "
            f"Skipped: all {raw_count} rows dropped during cleaning"
        )
        return

    featured_df = cleaned_df.withColumn(
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
    predictions_df = add_rule_based_alerts(
        predictions_df, blacklist_accounts
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
        "prediction", "fraud_probability",
        "is_ml_alert", "is_blacklist_destination",
        "is_rule_alert", "is_alert", "alert_source",
        "processed_at",
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
    alert_count = predictions_df.filter(F.col("is_alert") == 1.0).count()
    ml_alert_count = predictions_df.filter(
        F.col("is_ml_alert") == 1.0
    ).count()
    rule_alert_count = predictions_df.filter(
        F.col("is_rule_alert") == 1.0
    ).count()

    if redis_client is not None:
        # Update atomic counters
        try:
            redis_client.incrby("total_transactions", row_count)
            redis_client.incrby("total_fraud", alert_count)
            redis_client.incrby("total_ml_alerts", ml_alert_count)
            redis_client.incrby("total_rule_alerts", rule_alert_count)
        except Exception:
            pass

        # Publish individual fraud alerts
        if alert_count > 0:
            fraud_rows = (
                predictions_df
                .filter(F.col("is_alert") == 1.0)
                .select(
                    "step", "type", "amount",
                    "nameOrig", "nameDest",
                    "fraud_probability",
                    "oldbalanceOrg", "newbalanceOrig",
                    "is_ml_alert", "is_blacklist_destination",
                    "is_rule_alert", "alert_source",
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
                    "is_ml_alert": int(row["is_ml_alert"]),
                    "is_blacklist_destination": int(
                        row["is_blacklist_destination"]
                    ),
                    "is_rule_alert": int(row["is_rule_alert"]),
                    "alert_source": row["alert_source"],
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
        f"Raw: {raw_count} | "
        f"Processed: {row_count} | "
        f"Dropped: {dropped_count} | "
        f"Alerts: {alert_count} | "
        f"ML: {ml_alert_count} | "
        f"Rule: {rule_alert_count}"
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
    blacklist_accounts = load_blacklist_accounts()

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
    print(f"   Blacklist file: {BLACKLIST_PATH}")
    print(
        "   Blacklist rule types: "
        + ", ".join(BLACKLIST_TRANSFER_TYPES)
    )
    print("   Press Ctrl+C to stop.\n")

    query = (
        watermarked_stream.writeStream
        .foreachBatch(
            lambda df, bid: process_batch(
                df, bid, model, redis_client, blacklist_accounts
            )
        )
        .option("checkpointLocation", HDFS_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
