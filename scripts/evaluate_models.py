"""
Standalone model evaluation script for demo purposes.

Loads trained models and re-evaluates on test data from CSV files.
No Kafka, Docker, or Redis required — runs on local Spark.

Usage:
    spark-submit scripts/evaluate_models.py
    # or if pyspark is installed:
    python scripts/evaluate_models.py
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel

from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator,
)

# Add spark/ dir to path for preprocessing imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "spark"))
from preprocessing import prepare_dataframe  # noqa: E402


# ============================================================
# Configuration — auto-detect local vs Docker paths
# ============================================================
def resolve_path(relative):
    """Resolve path relative to project root."""
    return os.path.join(PROJECT_ROOT, relative)


PAYSIM_CSV = os.getenv(
    "PAYSIM_CSV", resolve_path("data/paysim_transactions.csv")
)
CREDITCARD_CSV = os.getenv(
    "CREDITCARD_CSV", resolve_path("data/creditcard.csv")
)

# Model paths — try to detect what's available
MODEL_PATHS = {
    "PaySim-Best": resolve_path("models/fraud_pipeline_model"),
    "PaySim-Alt": resolve_path("models/fraud_pipeline_model_rf"),
    "CreditCard-Best": resolve_path("models/creditcard_pipeline_model"),
    "CreditCard-Alt": resolve_path(
        "models/creditcard_pipeline_model_rf"
    ),
}

CC_PCA_COLS = [f"V{i}" for i in range(1, 29)]
CC_FEATURE_COLS = CC_PCA_COLS + ["Amount"]


def evaluate_predictions(predictions, model_name):
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

    return metrics


def load_paysim_test(spark):
    """Load PaySim CSV and return prepared test split."""
    if not os.path.exists(PAYSIM_CSV):
        print(f"⚠️ PaySim CSV not found: {PAYSIM_CSV}")
        return None

    print(f"📂 Loading PaySim: {PAYSIM_CSV}")
    raw_df = spark.read.csv(PAYSIM_CSV, header=True, inferSchema=True)
    prepared_df = prepare_dataframe(raw_df)

    # Use same split as training (seed=42) to get the exact test set
    _, _, test_df = prepared_df.randomSplit(
        [0.7, 0.15, 0.15], seed=42
    )
    test_count = test_df.count()
    print(f"   Test set: {test_count:,} rows")
    return test_df


def load_creditcard_test(spark):
    """Load CreditCard CSV and return prepared test split."""
    if not os.path.exists(CREDITCARD_CSV):
        print(f"⚠️ CreditCard CSV not found: {CREDITCARD_CSV}")
        return None

    print(f"📂 Loading CreditCard: {CREDITCARD_CSV}")
    raw_df = spark.read.csv(
        CREDITCARD_CSV, header=True, inferSchema=True
    )

    prepared_df = (
        raw_df
        .drop("Time")
        .withColumn("label", F.col("Class").cast("double"))
        .drop("Class")
    )
    for col_name in CC_FEATURE_COLS:
        prepared_df = prepared_df.withColumn(
            col_name, F.col(col_name).cast("double")
        )
    prepared_df = prepared_df.na.drop()

    _, _, test_df = prepared_df.randomSplit(
        [0.7, 0.15, 0.15], seed=42
    )
    test_count = test_df.count()
    print(f"   Test set: {test_count:,} rows")
    return test_df


def main():
    """Load models, evaluate, and print comparison table."""
    print("=" * 70)
    print("📊 Fraud Detection — Model Evaluation Report")
    print("   No Kafka/Docker required — local Spark evaluation")
    print("=" * 70)

    spark = (
        SparkSession.builder
        .appName("FraudDetection-Evaluation")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load test datasets
    paysim_test = load_paysim_test(spark)
    cc_test = load_creditcard_test(spark)

    # Evaluate each available model
    results = []

    for model_label, model_path in MODEL_PATHS.items():
        if not os.path.exists(model_path):
            print(f"\n⚠️ Model not found: {model_label} ({model_path})")
            continue

        print(f"\n🔄 Evaluating: {model_label}")
        try:
            model = PipelineModel.load(model_path)
        except Exception as e:
            print(f"   ❌ Failed to load: {e}")
            continue

        # Determine which test set to use
        if model_label.startswith("PaySim"):
            test_df = paysim_test
            dataset = "PaySim"
        else:
            test_df = cc_test
            dataset = "CreditCard"

        if test_df is None:
            print(f"   ⚠️ No test data for {dataset}")
            continue

        try:
            predictions = model.transform(test_df)
            metrics = evaluate_predictions(predictions, model_label)
            results.append({
                "label": model_label,
                "dataset": dataset,
                "metrics": metrics,
            })
            print(f"   ✅ AUC={metrics['auc']:.6f}")
        except Exception as e:
            print(f"   ❌ Evaluation failed: {e}")

    # Print comparison table
    if results:
        print("\n" + "=" * 80)
        print("🏆 MODEL COMPARISON TABLE")
        print("=" * 80)

        header = (
            f"{'Model':<20} {'Dataset':<12} "
            f"{'Accuracy':>10} {'Precision':>10} {'Recall':>10} "
            f"{'F1':>10} {'AUC':>10}"
        )
        print(header)
        print("-" * 80)

        best_auc = 0.0
        best_label = ""

        for r in results:
            m = r["metrics"]
            print(
                f"{r['label']:<20} {r['dataset']:<12} "
                f"{m['accuracy']:>10.6f} {m['precision']:>10.6f} "
                f"{m['recall']:>10.6f} {m['f1']:>10.6f} "
                f"{m['auc']:>10.6f}"
            )
            if m["auc"] > best_auc:
                best_auc = m["auc"]
                best_label = r["label"]

        print("-" * 80)
        print(f"🥇 Best model: {best_label} (AUC={best_auc:.6f})")
    else:
        print("\n❌ No models were evaluated successfully.")
        print("   Ensure you ran train_model.py first.")

    print("\n" + "=" * 70)
    print("✅ Evaluation complete!")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
