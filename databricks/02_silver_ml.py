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
