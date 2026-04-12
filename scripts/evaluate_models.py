"""
Standalone PaySim model evaluation script.

Loads the saved PaySim models and re-evaluates them on the deterministic
test split produced from the PaySim CSV.

Usage:
    spark-submit scripts/evaluate_models.py
"""
import os
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "spark"))

from train_model import (  # noqa: E402
    PAYSIM_CSV,
    evaluate_model,
    load_paysim_dataset,
    split_dataset,
)


def resolve_path(relative_path):
    """Resolve a path relative to the project root."""
    return os.path.join(PROJECT_ROOT, relative_path)


MODEL_PATHS = {
    "PaySim-Best": resolve_path("models/fraud_pipeline_model"),
    "PaySim-Alt": resolve_path("models/fraud_pipeline_model_alt"),
}


def main():
    """Load saved models and evaluate them on the PaySim test split."""
    print("=" * 70)
    print("PaySim fraud detection - saved model evaluation")
    print("=" * 70)

    spark = (
        SparkSession.builder
        .appName("FraudDetection-Evaluation-PaySim")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    if not os.path.exists(PAYSIM_CSV):
        print(f"[ERROR] PaySim CSV not found: {PAYSIM_CSV}")
        spark.stop()
        return

    weighted_df, summary = load_paysim_dataset(spark, csv_path=PAYSIM_CSV)
    _, _, test_df = split_dataset(weighted_df)
    print(
        f"\nEvaluating on clean PaySim test split "
        f"({summary['clean_rows']:,} clean rows total)"
    )

    results = []
    for model_label, model_path in MODEL_PATHS.items():
        if not os.path.exists(model_path):
            print(f"[WARN] Model not found: {model_label} ({model_path})")
            continue

        print(f"\nLoading model: {model_label}")
        model = PipelineModel.load(model_path)
        predictions = model.transform(test_df)
        metrics = evaluate_model(predictions)
        results.append((model_label, metrics))

        print(f"  accuracy : {metrics['accuracy']:.6f}")
        print(f"  precision: {metrics['precision']:.6f}")
        print(f"  recall   : {metrics['recall']:.6f}")
        print(f"  f1       : {metrics['f1']:.6f}")
        print(f"  auc      : {metrics['auc']:.6f}")

    if results:
        print("\n" + "=" * 80)
        print("Saved model comparison")
        print("=" * 80)
        header = (
            f"{'Model':<20} {'Accuracy':>10} {'Precision':>10} "
            f"{'Recall':>10} {'F1':>10} {'AUC':>10}"
        )
        print(header)
        print("-" * 80)

        best_label = ""
        best_auc = 0.0
        for model_label, metrics in results:
            print(
                f"{model_label:<20} "
                f"{metrics['accuracy']:>10.6f} "
                f"{metrics['precision']:>10.6f} "
                f"{metrics['recall']:>10.6f} "
                f"{metrics['f1']:>10.6f} "
                f"{metrics['auc']:>10.6f}"
            )
            if metrics["auc"] > best_auc:
                best_auc = metrics["auc"]
                best_label = model_label

        print("-" * 80)
        print(f"Best saved model: {best_label} (AUC={best_auc:.6f})")
    else:
        print("\n[WARN] No saved models were evaluated.")

    spark.stop()


if __name__ == "__main__":
    main()
