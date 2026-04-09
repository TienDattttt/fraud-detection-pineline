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
