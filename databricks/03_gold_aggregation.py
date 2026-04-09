# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer — KPI Aggregations
# MAGIC
# MAGIC **Purpose:** Aggregate Silver data into business KPIs for dashboards and reporting.
# MAGIC
# MAGIC **Input:** Delta `dbfs:/delta/silver/transactions_scored`
# MAGIC **Output:** Multiple Gold Delta tables under `dbfs:/delta/gold/kpis/`
# MAGIC
# MAGIC ### KPIs được tính:
# MAGIC 1. Số giao dịch fraud theo giờ (step)
# MAGIC 2. Tổng amount fraud theo loại giao dịch (type)
# MAGIC 3. Top 10 tài khoản nguồn có fraud_probability trung bình cao nhất
# MAGIC 4. Tỉ lệ fraud theo khoảng amount (bins: <1k, 1k-10k, 10k-100k, >100k)
# MAGIC 5. Model Performance Summary (Confusion Matrix)
# MAGIC 6. Overall KPI Summary

# COMMAND ----------

from pyspark.sql import functions as F

# Path configuration
SILVER_PATH = "dbfs:/delta/silver/transactions_scored"
GOLD_PATH = "dbfs:/delta/gold/kpis"

# COMMAND ----------

# Load Silver data
silver_df = spark.read.format("delta").load(SILVER_PATH)  # noqa: F821
total_rows = silver_df.count()
print(f"📂 Silver rows: {total_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 📊 Số giao dịch fraud theo giờ
# MAGIC
# MAGIC Trong PaySim, mỗi `step` đại diện cho 1 giờ (tổng 744 steps = 31 ngày).

# COMMAND ----------

fraud_by_hour = (
    silver_df.groupBy("step")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(
            F.when(F.col("prediction") == 1.0, 1).otherwise(0)
        ).alias("predicted_fraud"),
        F.sum(
            F.when(F.col("isFraud") == 1, 1).otherwise(0)
        ).alias("actual_fraud"),
        F.sum("amount").alias("total_amount"),
    )
    .withColumn(
        "fraud_rate",
        F.round(
            F.col("predicted_fraud")
            / F.col("total_transactions") * 100, 4
        )
    )
    .orderBy("step")
)

(
    fraud_by_hour.write
    .format("delta").mode("overwrite")
    .save(f"{GOLD_PATH}/fraud_by_hour")
)

print(f"✅ fraud_by_hour: {fraud_by_hour.count()} rows")
display(fraud_by_hour.limit(50))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 💰 Tổng amount fraud theo loại giao dịch

# COMMAND ----------

fraud_by_type = (
    silver_df.groupBy("type")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(
            F.when(F.col("prediction") == 1.0, 1).otherwise(0)
        ).alias("predicted_fraud"),
        F.sum(
            F.when(F.col("isFraud") == 1, 1).otherwise(0)
        ).alias("actual_fraud"),
        F.sum("amount").alias("total_amount"),
        F.sum(
            F.when(F.col("prediction") == 1.0, F.col("amount"))
            .otherwise(0)
        ).alias("fraud_amount"),
        F.avg("fraud_probability").alias("avg_fraud_probability"),
    )
    .withColumn(
        "fraud_rate",
        F.round(
            F.col("predicted_fraud")
            / F.col("total_transactions") * 100, 4
        )
    )
    .withColumn(
        "fraud_amount_pct",
        F.round(
            F.col("fraud_amount") / F.col("total_amount") * 100, 4
        )
    )
    .orderBy(F.desc("predicted_fraud"))
)

(
    fraud_by_type.write
    .format("delta").mode("overwrite")
    .save(f"{GOLD_PATH}/fraud_by_type")
)

print(f"✅ fraud_by_type: {fraud_by_type.count()} rows")
display(fraud_by_type)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 🔝 Top 10 tài khoản nguồn có fraud_probability cao nhất

# COMMAND ----------

top_risky_accounts = (
    silver_df
    .groupBy("nameOrig")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(
            F.when(F.col("prediction") == 1.0, 1).otherwise(0)
        ).alias("fraud_count"),
        F.avg("fraud_probability").alias("avg_fraud_probability"),
        F.max("fraud_probability").alias("max_fraud_probability"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_transactions") >= 1)
    .orderBy(F.desc("avg_fraud_probability"))
    .limit(10)
)

(
    top_risky_accounts.write
    .format("delta").mode("overwrite")
    .save(f"{GOLD_PATH}/top_risky_accounts")
)

print(f"✅ top_risky_accounts: {top_risky_accounts.count()} rows")
display(top_risky_accounts)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 📈 Tỉ lệ fraud theo khoảng amount
# MAGIC
# MAGIC Bins: `<1k`, `1k-10k`, `10k-100k`, `>100k`

# COMMAND ----------

fraud_by_amount = (
    silver_df
    .withColumn(
        "amount_range",
        F.when(F.col("amount") < 1000, "<1k")
        .when(F.col("amount") < 10000, "1k-10k")
        .when(F.col("amount") < 100000, "10k-100k")
        .otherwise(">100k")
    )
    .groupBy("amount_range")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(
            F.when(F.col("prediction") == 1.0, 1).otherwise(0)
        ).alias("predicted_fraud"),
        F.sum(
            F.when(F.col("isFraud") == 1, 1).otherwise(0)
        ).alias("actual_fraud"),
        F.avg("fraud_probability").alias("avg_fraud_probability"),
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
    .withColumn(
        "fraud_rate",
        F.round(
            F.col("predicted_fraud")
            / F.col("total_transactions") * 100, 4
        )
    )
    .orderBy("amount_range")
)

(
    fraud_by_amount.write
    .format("delta").mode("overwrite")
    .save(f"{GOLD_PATH}/fraud_by_amount")
)

print(f"✅ fraud_by_amount: {fraud_by_amount.count()} rows")
display(fraud_by_amount)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 🧮 Model Performance Summary (Confusion Matrix)

# COMMAND ----------

model_summary = (
    silver_df
    .withColumn(
        "true_positive",
        F.when(
            (F.col("prediction") == 1.0) & (F.col("isFraud") == 1), 1
        ).otherwise(0)
    )
    .withColumn(
        "false_positive",
        F.when(
            (F.col("prediction") == 1.0) & (F.col("isFraud") == 0), 1
        ).otherwise(0)
    )
    .withColumn(
        "true_negative",
        F.when(
            (F.col("prediction") == 0.0) & (F.col("isFraud") == 0), 1
        ).otherwise(0)
    )
    .withColumn(
        "false_negative",
        F.when(
            (F.col("prediction") == 0.0) & (F.col("isFraud") == 1), 1
        ).otherwise(0)
    )
    .agg(
        F.sum("true_positive").alias("TP"),
        F.sum("false_positive").alias("FP"),
        F.sum("true_negative").alias("TN"),
        F.sum("false_negative").alias("FN"),
        F.count("*").alias("total"),
    )
)

(
    model_summary.write
    .format("delta").mode("overwrite")
    .save(f"{GOLD_PATH}/model_summary")
)

row = model_summary.collect()[0]
tp, fp, tn, fn = row["TP"], row["FP"], row["TN"], row["FN"]

precision = tp / (tp + fp) if (tp + fp) > 0 else 0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0
f1 = (
    2 * precision * recall / (precision + recall)
    if (precision + recall) > 0 else 0
)

print("📊 Confusion Matrix:")
print(f"   TP={tp:,}  FP={fp:,}")
print(f"   FN={fn:,}  TN={tn:,}")
print(f"   Precision: {precision:.4f}")
print(f"   Recall:    {recall:.4f}")
print(f"   F1-Score:  {f1:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 📋 Overall KPI Summary

# COMMAND ----------

kpi_summary = silver_df.agg(
    F.count("*").alias("total_transactions"),
    F.sum(
        F.when(F.col("prediction") == 1.0, 1).otherwise(0)
    ).alias("predicted_fraud"),
    F.sum(
        F.when(F.col("isFraud") == 1, 1).otherwise(0)
    ).alias("actual_fraud"),
    F.sum("amount").alias("total_volume"),
    F.avg("amount").alias("avg_transaction_amount"),
    F.countDistinct("nameOrig").alias("unique_senders"),
    F.countDistinct("nameDest").alias("unique_receivers"),
    F.avg("fraud_probability").alias("avg_fraud_probability"),
)

(
    kpi_summary.write
    .format("delta").mode("overwrite")
    .save(f"{GOLD_PATH}/kpi_summary")
)

display(kpi_summary)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Gold Layer Complete

# COMMAND ----------

print("🎉 Gold layer complete!")
print(f"Tables written to {GOLD_PATH}/:")
print("  1. fraud_by_hour     — Số GD fraud theo step (giờ)")
print("  2. fraud_by_type     — Tổng amount fraud theo loại GD")
print("  3. top_risky_accounts — Top 10 tài khoản rủi ro cao nhất")
print("  4. fraud_by_amount   — Tỉ lệ fraud theo khoảng amount")
print("  5. model_summary     — Confusion matrix")
print("  6. kpi_summary       — Tổng quan KPI")
