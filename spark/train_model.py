"""
Offline ML training for fraud detection on TWO datasets.

Dataset 1: PaySim (e-wallet simulation) — 11 columns, label=isFraud
Dataset 2: Kaggle Credit Card Fraud — 31 columns (V1-V28 PCA), label=Class

Trains RandomForest AND GBT for each dataset (4 models total).
Handles class imbalance via weight column.
Logs all metrics to MLflow and saves best models.

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
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Add parent dir to path so we can import preprocessing
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from preprocessing import (  # noqa: E402
    create_feature_pipeline,
    prepare_dataframe,
)


# ============================================================
# Configuration
# ============================================================
PAYSIM_CSV = os.getenv(
    "PAYSIM_CSV", "/opt/spark/work/data/paysim_transactions.csv"
)
CREDITCARD_CSV = os.getenv(
    "CREDITCARD_CSV", "/opt/spark/work/data/creditcard.csv"
)
MODEL_OUTPUT_PAYSIM = os.getenv(
    "MODEL_PATH", "/opt/spark/work/models/fraud_pipeline_model"
)
MODEL_OUTPUT_PAYSIM_ALT = os.getenv(
    "MODEL_PATH_ALT", "/opt/spark/work/models/fraud_pipeline_model_rf"
)
MODEL_OUTPUT_CC = os.getenv(
    "MODEL_PATH_CC", "/opt/spark/work/models/creditcard_pipeline_model"
)
MODEL_OUTPUT_CC_ALT = os.getenv(
    "MODEL_PATH_CC_ALT",
    "/opt/spark/work/models/creditcard_pipeline_model_rf"
)
MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI", "file:///opt/spark/work/models/mlruns"
)

# CreditCard PCA feature columns
CC_PCA_COLS = [f"V{i}" for i in range(1, 29)]
CC_FEATURE_COLS = CC_PCA_COLS + ["Amount"]


# ============================================================
# Shared utilities
# ============================================================
def add_class_weights(df, dataset_name="Dataset"):
    """
    Add weight column to handle class imbalance.

    Weight formula: total_samples / (2 * class_count)
    """
    total = df.count()
    fraud_count = df.filter(F.col("label") == 1.0).count()
    non_fraud_count = total - fraud_count

    print(f"\n📊 {dataset_name} — Class Distribution:")
    print(
        f"   Non-Fraud: {non_fraud_count:,} "
        f"({non_fraud_count / total * 100:.2f}%)"
    )
    print(
        f"   Fraud:     {fraud_count:,} "
        f"({fraud_count / total * 100:.2f}%)"
    )

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


def evaluate_model(predictions, model_name):
    """Compute all classification metrics on predictions DataFrame."""
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


def log_to_mlflow(run_name, metrics, params):
    """Log metrics and params to MLflow. Fails silently."""
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("fraud-detection")

        with mlflow.start_run(run_name=run_name):
            for k, v in metrics.items():
                mlflow.log_metric(k, v)
            for k, v in params.items():
                mlflow.log_param(k, v)

        print(f"📝 MLflow logged: {run_name}")
    except Exception as e:
        print(f"⚠️ MLflow logging skipped for {run_name}: {e}")


# ============================================================
# Dataset 1: PaySim
# ============================================================
def train_paysim(spark):
    """
    Train RF and GBT on PaySim dataset.

    Uses preprocessing.py for feature engineering (shared with streaming).
    Returns dict with model results for both classifiers.
    """
    print("\n" + "=" * 60)
    print("🚀 DATASET 1: PaySim — Training Pipeline")
    print("=" * 60)

    # ----------------------------------------------------------
    # 1. Load & prepare
    # ----------------------------------------------------------
    print(f"\n📂 Loading PaySim from {PAYSIM_CSV}...")
    raw_df = spark.read.csv(PAYSIM_CSV, header=True, inferSchema=True)
    total_rows = raw_df.count()
    print(f"   Total rows: {total_rows:,}")

    # Clean: drop rows with nulls in critical columns
    clean_df = raw_df.na.drop(
        subset=["amount", "oldbalanceOrg", "newbalanceOrig",
                "oldbalanceDest", "newbalanceDest", "isFraud"]
    )
    dropped = total_rows - clean_df.count()
    if dropped > 0:
        print(f"   Dropped {dropped:,} rows with null values")

    prepared_df = prepare_dataframe(clean_df)
    weighted_df = add_class_weights(prepared_df, "PaySim")

    # ----------------------------------------------------------
    # 2. Split: 70% train / 15% validation / 15% test
    # ----------------------------------------------------------
    train_df, val_df, test_df = weighted_df.randomSplit(
        [0.7, 0.15, 0.15], seed=42
    )
    train_count = train_df.count()
    val_count = val_df.count()
    test_count = test_df.count()
    print("\n📊 Split:")
    print(f"   Train:      {train_count:,}")
    print(f"   Validation: {val_count:,}")
    print(f"   Test:       {test_count:,}")

    # ----------------------------------------------------------
    # 3. Feature pipeline (from preprocessing.py)
    # ----------------------------------------------------------
    feature_pipeline = create_feature_pipeline()

    # ----------------------------------------------------------
    # 4. Train Random Forest with CrossValidator
    # ----------------------------------------------------------
    print("\n🌲 Training Random Forest (PaySim)...")
    t0 = time.time()

    rf = RandomForestClassifier(
        featuresCol="features", labelCol="label",
        weightCol="weight", numTrees=100, maxDepth=10, seed=42,
    )
    rf_pipeline = Pipeline(
        stages=feature_pipeline.getStages() + [rf]
    )

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
        numFolds=3, seed=42,
    )

    rf_cv_model = rf_cv.fit(train_df)
    rf_time = time.time() - t0
    print(f"   Training time: {rf_time:.1f}s")

    rf_predictions = rf_cv_model.transform(test_df)
    rf_metrics = evaluate_model(rf_predictions, "PaySim — Random Forest")

    log_to_mlflow("paysim-rf", rf_metrics, {
        "dataset": "paysim", "model_type": "RandomForest",
        "train_size": train_count, "val_size": val_count,
        "test_size": test_count, "num_features": len(prepared_df.columns),
    })

    # ----------------------------------------------------------
    # 5. Train GBT
    # ----------------------------------------------------------
    print("\n🌳 Training Gradient Boosted Trees (PaySim)...")
    t0 = time.time()

    gbt = GBTClassifier(
        featuresCol="features", labelCol="label",
        weightCol="weight", maxIter=50, maxDepth=8, seed=42,
    )
    gbt_pipeline = Pipeline(
        stages=feature_pipeline.getStages() + [gbt]
    )
    gbt_model = gbt_pipeline.fit(train_df)
    gbt_time = time.time() - t0
    print(f"   Training time: {gbt_time:.1f}s")

    gbt_predictions = gbt_model.transform(test_df)
    gbt_metrics = evaluate_model(gbt_predictions, "PaySim — GBT")

    log_to_mlflow("paysim-gbt", gbt_metrics, {
        "dataset": "paysim", "model_type": "GBT",
        "train_size": train_count, "val_size": val_count,
        "test_size": test_count, "num_features": len(prepared_df.columns),
    })

    return {
        "rf": {
            "metrics": rf_metrics,
            "model": rf_cv_model.bestModel,
            "time": rf_time,
        },
        "gbt": {
            "metrics": gbt_metrics,
            "model": gbt_model,
            "time": gbt_time,
        },
    }


# ============================================================
# Dataset 2: CreditCard (Kaggle)
# ============================================================
def create_creditcard_pipeline():
    """
    Build feature pipeline for CreditCard dataset.

    CreditCard columns are already PCA-transformed (V1-V28),
    so no StringIndexer needed — just assemble + scale.
    """
    assembler = VectorAssembler(
        inputCols=CC_FEATURE_COLS,
        outputCol="raw_features",
        handleInvalid="keep",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False,
    )
    return Pipeline(stages=[assembler, scaler])


def train_creditcard(spark):
    """
    Train RF and GBT on Kaggle Credit Card Fraud dataset.

    Schema: Time, V1-V28 (PCA features), Amount, Class
    All numeric — no categorical encoding needed.
    """
    print("\n" + "=" * 60)
    print("🚀 DATASET 2: CreditCard — Training Pipeline")
    print("=" * 60)

    # ----------------------------------------------------------
    # 1. Load & prepare
    # ----------------------------------------------------------
    print(f"\n📂 Loading CreditCard from {CREDITCARD_CSV}...")
    raw_df = spark.read.csv(
        CREDITCARD_CSV, header=True, inferSchema=True
    )
    total_rows = raw_df.count()
    print(f"   Total rows: {total_rows:,}")

    # Drop Time column (not useful for fraud prediction)
    # Rename Class → label, cast to double
    prepared_df = (
        raw_df
        .drop("Time")
        .withColumn("label", F.col("Class").cast("double"))
        .drop("Class")
    )

    # Cast all feature columns to double
    for col_name in CC_FEATURE_COLS:
        prepared_df = prepared_df.withColumn(
            col_name, F.col(col_name).cast("double")
        )

    # Drop rows with nulls
    clean_df = prepared_df.na.drop()
    dropped = total_rows - clean_df.count()
    if dropped > 0:
        print(f"   Dropped {dropped:,} rows with null values")

    weighted_df = add_class_weights(clean_df, "CreditCard")

    # ----------------------------------------------------------
    # 2. Split: 70% train / 15% validation / 15% test
    # ----------------------------------------------------------
    train_df, val_df, test_df = weighted_df.randomSplit(
        [0.7, 0.15, 0.15], seed=42
    )
    train_count = train_df.count()
    val_count = val_df.count()
    test_count = test_df.count()
    print("\n📊 Split:")
    print(f"   Train:      {train_count:,}")
    print(f"   Validation: {val_count:,}")
    print(f"   Test:       {test_count:,}")

    # ----------------------------------------------------------
    # 3. Feature pipeline (CreditCard-specific)
    # ----------------------------------------------------------
    feature_pipeline = create_creditcard_pipeline()

    # ----------------------------------------------------------
    # 4. Train Random Forest with CrossValidator
    # ----------------------------------------------------------
    print("\n🌲 Training Random Forest (CreditCard)...")
    t0 = time.time()

    rf = RandomForestClassifier(
        featuresCol="features", labelCol="label",
        weightCol="weight", numTrees=100, maxDepth=10, seed=42,
    )
    rf_pipeline = Pipeline(
        stages=feature_pipeline.getStages() + [rf]
    )

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
        numFolds=3, seed=42,
    )

    rf_cv_model = rf_cv.fit(train_df)
    rf_time = time.time() - t0
    print(f"   Training time: {rf_time:.1f}s")

    rf_predictions = rf_cv_model.transform(test_df)
    rf_metrics = evaluate_model(
        rf_predictions, "CreditCard — Random Forest"
    )

    log_to_mlflow("creditcard-rf", rf_metrics, {
        "dataset": "creditcard", "model_type": "RandomForest",
        "train_size": train_count, "val_size": val_count,
        "test_size": test_count, "num_features": len(CC_FEATURE_COLS),
    })

    # ----------------------------------------------------------
    # 5. Train GBT
    # ----------------------------------------------------------
    print("\n🌳 Training Gradient Boosted Trees (CreditCard)...")
    t0 = time.time()

    gbt = GBTClassifier(
        featuresCol="features", labelCol="label",
        weightCol="weight", maxIter=50, maxDepth=8, seed=42,
    )
    gbt_pipeline = Pipeline(
        stages=feature_pipeline.getStages() + [gbt]
    )
    gbt_model = gbt_pipeline.fit(train_df)
    gbt_time = time.time() - t0
    print(f"   Training time: {gbt_time:.1f}s")

    gbt_predictions = gbt_model.transform(test_df)
    gbt_metrics = evaluate_model(gbt_predictions, "CreditCard — GBT")

    log_to_mlflow("creditcard-gbt", gbt_metrics, {
        "dataset": "creditcard", "model_type": "GBT",
        "train_size": train_count, "val_size": val_count,
        "test_size": test_count, "num_features": len(CC_FEATURE_COLS),
    })

    return {
        "rf": {
            "metrics": rf_metrics,
            "model": rf_cv_model.bestModel,
            "time": rf_time,
        },
        "gbt": {
            "metrics": gbt_metrics,
            "model": gbt_model,
            "time": gbt_time,
        },
    }


# ============================================================
# Summary & Save
# ============================================================
def print_comparison_table(paysim_results, cc_results):
    """Print formatted comparison table of all 4 models."""
    print("\n" + "=" * 90)
    print("🏆 FINAL MODEL COMPARISON — ALL 4 MODELS")
    print("=" * 90)

    header = (
        f"{'Dataset':<12} {'Model':<6} "
        f"{'Accuracy':>10} {'Precision':>10} {'Recall':>10} "
        f"{'F1':>10} {'AUC':>10} {'Time(s)':>8}"
    )
    print(header)
    print("-" * 90)

    rows = [
        ("PaySim", "RF", paysim_results["rf"]),
        ("PaySim", "GBT", paysim_results["gbt"]),
        ("CreditCard", "RF", cc_results["rf"]),
        ("CreditCard", "GBT", cc_results["gbt"]),
    ]

    best_auc = 0.0
    best_label = ""
    for dataset, model_type, result in rows:
        m = result["metrics"]
        t = result["time"]
        print(
            f"{dataset:<12} {model_type:<6} "
            f"{m['accuracy']:>10.6f} {m['precision']:>10.6f} "
            f"{m['recall']:>10.6f} {m['f1']:>10.6f} "
            f"{m['auc']:>10.6f} {t:>8.1f}"
        )
        if m["auc"] > best_auc:
            best_auc = m["auc"]
            best_label = f"{dataset}-{model_type}"

    print("-" * 90)
    print(f"🥇 Overall best: {best_label} (AUC={best_auc:.6f})")


def save_models(paysim_results, cc_results):
    """
    Save all trained models.

    PaySim best → fraud_pipeline_model/ (used by streaming_pipeline.py)
    PaySim other → fraud_pipeline_model_rf/ (or _gbt)
    CreditCard best → creditcard_pipeline_model/
    CreditCard other → creditcard_pipeline_model_rf/ (or _gbt)
    """
    print("\n" + "=" * 60)
    print("💾 Saving Models")
    print("=" * 60)

    # --- PaySim: best goes to main path (streaming compatibility) ---
    ps_rf_auc = paysim_results["rf"]["metrics"]["auc"]
    ps_gbt_auc = paysim_results["gbt"]["metrics"]["auc"]

    if ps_rf_auc >= ps_gbt_auc:
        ps_best_name, ps_alt_name = "RF", "GBT"
        ps_best_model = paysim_results["rf"]["model"]
        ps_alt_model = paysim_results["gbt"]["model"]
    else:
        ps_best_name, ps_alt_name = "GBT", "RF"
        ps_best_model = paysim_results["gbt"]["model"]
        ps_alt_model = paysim_results["rf"]["model"]

    print(
        f"\n   PaySim best ({ps_best_name}) → {MODEL_OUTPUT_PAYSIM}"
    )
    ps_best_model.write().overwrite().save(MODEL_OUTPUT_PAYSIM)
    print("   ✅ Saved")

    print(
        f"   PaySim alt  ({ps_alt_name}) → {MODEL_OUTPUT_PAYSIM_ALT}"
    )
    ps_alt_model.write().overwrite().save(MODEL_OUTPUT_PAYSIM_ALT)
    print("   ✅ Saved")

    # --- CreditCard: separate paths ---
    cc_rf_auc = cc_results["rf"]["metrics"]["auc"]
    cc_gbt_auc = cc_results["gbt"]["metrics"]["auc"]

    if cc_rf_auc >= cc_gbt_auc:
        cc_best_name, cc_alt_name = "RF", "GBT"
        cc_best_model = cc_results["rf"]["model"]
        cc_alt_model = cc_results["gbt"]["model"]
    else:
        cc_best_name, cc_alt_name = "GBT", "RF"
        cc_best_model = cc_results["gbt"]["model"]
        cc_alt_model = cc_results["rf"]["model"]

    print(
        f"\n   CreditCard best ({cc_best_name}) → {MODEL_OUTPUT_CC}"
    )
    cc_best_model.write().overwrite().save(MODEL_OUTPUT_CC)
    print("   ✅ Saved")

    print(
        f"   CreditCard alt  ({cc_alt_name}) → {MODEL_OUTPUT_CC_ALT}"
    )
    cc_alt_model.write().overwrite().save(MODEL_OUTPUT_CC_ALT)
    print("   ✅ Saved")


# ============================================================
# Main
# ============================================================
def main(spark):
    """Orchestrate training on both datasets."""
    print("=" * 60)
    print("🚀 Multi-Dataset Fraud Detection — Model Training")
    print("   Datasets: PaySim + CreditCard")
    print("   Models:   RandomForest + GBT (4 total)")
    print("=" * 60)

    # Train PaySim
    paysim_results = train_paysim(spark)

    # Train CreditCard
    cc_results = train_creditcard(spark)

    # Summary
    print_comparison_table(paysim_results, cc_results)

    # Save
    save_models(paysim_results, cc_results)

    print("\n" + "=" * 60)
    print("🎉 All training complete!")
    print("=" * 60)

    return paysim_results, cc_results


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("FraudDetection-Training-MultiDataset")
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        main(spark)
    finally:
        spark.stop()
