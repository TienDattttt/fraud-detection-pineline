# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Feature Engineering + ML Prediction
# MAGIC
# MAGIC **Purpose:** Clean Bronze data, engineer features, apply trained ML model for scoring.
# MAGIC
# MAGIC **Input:** Delta `dbfs:/delta/bronze/transactions`
# MAGIC **Output:** Delta `dbfs:/delta/silver/transactions_scored`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📤 Hướng dẫn Upload Model lên DBFS
# MAGIC
# MAGIC Trước khi chạy notebook này, cần upload model đã train:
# MAGIC
# MAGIC 1. Vào Databricks → **Data** → **DBFS**
# MAGIC 2. Tạo folder: `/FileStore/fraud-pipeline/models/fraud_pipeline_model/`
# MAGIC 3. Upload toàn bộ nội dung thư mục `models/fraud_pipeline_model/` từ local
# MAGIC    (bao gồm `metadata/` và `stages/` subdirectories)
# MAGIC
# MAGIC **Lưu ý:** Model là output từ `spark/train_model.py`. Hãy chạy train trước khi upload.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)

# Path configuration
BRONZE_PATH = "dbfs:/delta/bronze/transactions"
SILVER_OUTPUT = "dbfs:/delta/silver/transactions_scored"
MODEL_PATH = "dbfs:/FileStore/fraud-pipeline/models/fraud_pipeline_model/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze Data

# COMMAND ----------

bronze_df = spark.read.format("delta").load(BRONZE_PATH)  # noqa: F821
total_rows = bronze_df.count()
print(f"📂 Bronze rows: {total_rows:,}")
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering
# MAGIC
# MAGIC Replicate the same transformations from `spark/preprocessing.py`
# MAGIC to ensure consistency between local training and Databricks scoring.

# COMMAND ----------


def add_engineered_features(df):  # noqa: E302
    """
    Domain-specific features for PaySim fraud detection.

    Must match spark/preprocessing.py exactly.
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
    result = result.withColumn(
        "amount_ratio",
        F.col("amount") / (F.col("oldbalanceOrg") + F.lit(1.0))
    )

    # Binary: origin account drained to zero
    result = result.withColumn(
        "is_zero_balance_orig",
        F.when(F.col("newbalanceOrig") == 0.0, 1.0).otherwise(0.0)
    )

    # Binary: large transaction > 200,000 units
    result = result.withColumn(
        "is_large_amount",
        F.when(F.col("amount") > 200000.0, 1.0).otherwise(0.0)
    )

    return result

# COMMAND ----------


# Prepare features (same as local pipeline)  # noqa: E305
prepared_df = bronze_df.withColumn(
    "label", F.col("isFraud").cast("double")
)
prepared_df = add_engineered_features(prepared_df)

print("✅ Feature engineering complete")
print(f"   Columns: {len(prepared_df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Trained Model & Predict

# COMMAND ----------

# Load PipelineModel from DBFS
model = PipelineModel.load(MODEL_PATH)
print(f"✅ Model loaded from {MODEL_PATH}")
print(f"   Pipeline stages: {len(model.stages)}")

# COMMAND ----------

# Apply ML predictions
scored_df = model.transform(prepared_df)

# Extract fraud probability from probability vector
# probability is a Vector [prob_class_0, prob_class_1]
extract_fraud_prob = F.udf(
    lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0,
    "double"
)
scored_df = scored_df.withColumn(
    "fraud_probability", extract_fraud_prob(F.col("probability"))
)

# Add processing metadata
scored_df = scored_df.withColumn(
    "processed_at", F.current_timestamp()
)

print("✅ Predictions complete")
print(f"   Total scored: {scored_df.count():,}")
print(
    f"   Predicted fraud: "
    f"{scored_df.filter(F.col('prediction') == 1.0).count():,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Silver Delta Table

# COMMAND ----------

# Select final Silver columns
silver_df = scored_df.select(
    "step", "type", "amount",
    "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest",
    "isFraud", "isFlaggedFraud",
    "balance_diff_orig", "balance_diff_dest",
    "amount_ratio", "is_zero_balance_orig", "is_large_amount",
    "prediction", "fraud_probability",
    "processed_at",
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
print(
    f"   Predicted fraud: "
    f"{silver_df.filter(F.col('prediction') == 1.0).count():,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Model Evaluation & MLflow Logging
# MAGIC
# MAGIC Log metrics using Databricks built-in MLflow (no tracking URI needed).

# COMMAND ----------

import mlflow  # noqa: E402

# Evaluate model performance against actual labels
eval_df = scored_df

evaluators = {
    "auc": BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    ),
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
}

# Log to MLflow (Databricks built-in — no set_tracking_uri needed)
with mlflow.start_run(run_name="silver_batch_evaluation"):
    for name, evaluator in evaluators.items():
        score = evaluator.evaluate(eval_df)
        mlflow.log_metric(name, score)
        print(f"   {name:>10}: {score:.6f}")

    mlflow.log_param("dataset", "paysim")
    mlflow.log_param("layer", "silver")
    mlflow.log_param("total_rows", total_rows)

print("\n📝 Metrics logged to MLflow (Databricks)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quick Data Quality Check

# COMMAND ----------

# Verify Silver table
silver_check = spark.read.format("delta").load(SILVER_OUTPUT)  # noqa: F821
display(silver_check.limit(10))  # noqa: F821

# Distribution of predictions
display(  # noqa: F821
    silver_check
    .groupBy("prediction")
    .agg(
        F.count("*").alias("count"),
        F.avg("fraud_probability").alias("avg_probability"),
        F.avg("amount").alias("avg_amount"),
    )
)
