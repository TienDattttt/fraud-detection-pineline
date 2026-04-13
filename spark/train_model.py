"""
Offline ML training for the PaySim fraud-detection pipeline.

This script is the source of truth for:
1. Loading and cleaning the PaySim dataset with PySpark.
2. Training candidate models on the training split.
3. Evaluating them on validation and test splits.
4. Saving the deployed model used by the streaming pipeline.

Usage (inside spark-master container):
    spark-submit --master spark://spark-master:7077 \
        /opt/spark/work/spark/train_model.py
"""
import os
import sys
import time
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.functions import vector_to_array
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from preprocessing import (  # noqa: E402
    add_engineered_features,
    clean_paysim_dataframe,
    create_feature_pipeline,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTAINER_ROOT = Path("/opt/spark/work")
RUNTIME_ROOT = CONTAINER_ROOT if CONTAINER_ROOT.exists() else PROJECT_ROOT

PAYSIM_CSV = os.getenv(
    "PAYSIM_CSV", str(RUNTIME_ROOT / "data" / "paysim_transactions.csv")
)
MODEL_OUTPUT = os.getenv(
    "MODEL_PATH", str(RUNTIME_ROOT / "models" / "fraud_pipeline_model")
)
MODEL_OUTPUT_ALT = os.getenv(
    "MODEL_PATH_ALT", str(RUNTIME_ROOT / "models" / "fraud_pipeline_model_alt")
)
MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI", (RUNTIME_ROOT / "models" / "mlruns").resolve().as_uri()
)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_MODEL_METRICS_KEY = os.getenv(
    "REDIS_MODEL_METRICS_KEY", "model_metrics"
)
TRAIN_SPLIT = 0.7
VALIDATION_SPLIT = 0.15
TEST_SPLIT = 0.15
RANDOM_SEED = 42
MAX_ROC_POINTS = 200

MODEL_PROFILES = {
    "full": {
        "rf": {"numTrees": 80, "maxDepth": 10},
        "gbt": {"maxIter": 30, "maxDepth": 8},
    },
    "notebook": {
        "rf": {"numTrees": 40, "maxDepth": 8},
        "gbt": {"maxIter": 15, "maxDepth": 6},
    },
}


def add_class_weights(df):
    """Add a weight column to reduce label-imbalance bias."""
    total = df.count()
    fraud_count = df.filter(F.col("label") == 1.0).count()
    non_fraud_count = total - fraud_count

    print("\nClass distribution:")
    print(
        f"  Non-fraud: {non_fraud_count:,} "
        f"({non_fraud_count / total * 100:.4f}%)"
    )
    print(
        f"  Fraud:     {fraud_count:,} "
        f"({fraud_count / total * 100:.4f}%)"
    )

    weight_fraud = total / (2.0 * fraud_count) if fraud_count else 1.0
    weight_non_fraud = total / (2.0 * non_fraud_count)

    print(f"  Weight non-fraud: {weight_non_fraud:.4f}")
    print(f"  Weight fraud:     {weight_fraud:.4f}")

    return df.withColumn(
        "weight",
        F.when(F.col("label") == 1.0, F.lit(weight_fraud)).otherwise(
            F.lit(weight_non_fraud)
        ),
    )


def evaluate_model(predictions):
    """Compute classification metrics for a predictions DataFrame."""
    evaluators = {
        "accuracy": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy",
        ),
        "precision": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedPrecision",
        ),
        "recall": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedRecall",
        ),
        "f1": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="f1",
        ),
        "auc": BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        ),
    }

    return {
        name: evaluator.evaluate(predictions)
        for name, evaluator in evaluators.items()
    }


def collect_confusion_matrix(predictions):
    """Collect a small 2x2 confusion matrix from Spark predictions."""
    counts = {
        (int(row["label"]), int(row["prediction"])): int(row["count"])
        for row in (
            predictions
            .groupBy("label", "prediction")
            .count()
            .collect()
        )
    }

    return {
        "tn": counts.get((0, 0), 0),
        "fp": counts.get((0, 1), 0),
        "fn": counts.get((1, 0), 0),
        "tp": counts.get((1, 1), 0),
    }


def collect_roc_curve(predictions, max_points=MAX_ROC_POINTS):
    """
    Collect ROC curve points from distributed Spark predictions.

    The resulting list is down-sampled so notebooks can plot it cheaply.
    """
    scored_df = predictions.select(
        vector_to_array("probability")[1].alias("score"),
        F.col("label").cast("double").alias("label"),
    )
    score_and_labels = scored_df.rdd.map(
        lambda row: (float(row["score"]), float(row["label"]))
    )
    metrics = BinaryClassificationMetrics(score_and_labels)
    roc_points = [
        {"fpr": float(point[0]), "tpr": float(point[1])}
        for point in metrics.roc().collect()
    ]

    if len(roc_points) <= max_points:
        return roc_points

    step = max(1, len(roc_points) // (max_points - 1))
    sampled_points = roc_points[::step]
    if sampled_points[-1] != roc_points[-1]:
        sampled_points.append(roc_points[-1])

    return sampled_points


def print_metric_block(title, metrics):
    """Print a metrics block in a compact and readable format."""
    print(f"\n{title}:")
    for metric_name, metric_value in metrics.items():
        print(f"  {metric_name:>10}: {metric_value:.6f}")


def log_to_mlflow(run_name, validation_metrics, test_metrics, params):
    """Log training metadata to MLflow when available."""
    try:
        import mlflow

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("fraud-detection")

        with mlflow.start_run(run_name=run_name):
            for key, value in validation_metrics.items():
                mlflow.log_metric(f"validation_{key}", value)
            for key, value in test_metrics.items():
                mlflow.log_metric(f"test_{key}", value)
            for key, value in params.items():
                mlflow.log_param(key, value)

        print(f"[OK] MLflow logged: {run_name}")
    except Exception as exc:
        print(f"[WARN] MLflow logging skipped for {run_name}: {exc}")


def cache_model_metrics(validation_metrics, test_metrics, training_time):
    """
    Cache deployed model metrics in Redis for the serving API.

    The API consumes a float-only hash, so all values are stored as strings
    representing numbers.
    """
    try:
        import redis

        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_timeout=5,
        )
        payload = {
            **{
                key: f"{float(value):.6f}"
                for key, value in test_metrics.items()
            },
            **{
                f"validation_{key}": f"{float(value):.6f}"
                for key, value in validation_metrics.items()
            },
            "training_time_seconds": f"{float(training_time):.6f}",
        }
        client.delete(REDIS_MODEL_METRICS_KEY)
        client.hset(REDIS_MODEL_METRICS_KEY, mapping=payload)
        client.close()
        print(
            "[OK] Cached deployed model metrics in Redis hash "
            f"'{REDIS_MODEL_METRICS_KEY}'"
        )
    except Exception as exc:
        print(f"[WARN] Redis metrics cache skipped: {exc}")


def load_paysim_dataset(spark, csv_path=None):
    """
    Load, clean, and enrich the PaySim dataset for model training.

    Returns the weighted training DataFrame plus a small summary dict that is
    convenient for notebooks and reporting.
    """
    if csv_path is None:
        csv_path = PAYSIM_CSV

    print("\n" + "=" * 60)
    print("PaySim dataset loading and preprocessing")
    print("=" * 60)
    print(f"\nLoading PaySim from {csv_path}...")

    raw_df = spark.read.csv(csv_path, header=True, inferSchema=True)
    raw_count = raw_df.count()
    print(f"  Raw rows: {raw_count:,}")

    clean_df = clean_paysim_dataframe(raw_df)
    clean_count = clean_df.count()
    dropped_count = raw_count - clean_count
    print(f"  Clean rows: {clean_count:,}")
    print(f"  Dropped rows: {dropped_count:,}")

    prepared_df = add_engineered_features(clean_df).withColumn(
        "label", F.col("isFraud").cast("double")
    )
    weighted_df = add_class_weights(prepared_df)

    summary = {
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "dropped_rows": dropped_count,
        "fraud_rate_percent": weighted_df.agg(
            (F.avg(F.col("label")) * 100).alias("fraud_rate_percent")
        ).collect()[0]["fraud_rate_percent"],
    }

    return weighted_df, summary


def split_dataset(df, seed=RANDOM_SEED):
    """Split the weighted dataset into train, validation, and test sets."""
    train_df, validation_df, test_df = df.randomSplit(
        [TRAIN_SPLIT, VALIDATION_SPLIT, TEST_SPLIT],
        seed=seed,
    )

    print("\nSplit summary:")
    print(f"  Train:      {train_df.count():,}")
    print(f"  Validation: {validation_df.count():,}")
    print(f"  Test:       {test_df.count():,}")

    return train_df, validation_df, test_df


def sample_split(df, sample_fraction, seed):
    """Stratified sampling for interactive notebook training."""
    if sample_fraction is None:
        return df
    if not 0 < sample_fraction <= 1:
        raise ValueError("sample_fraction must be in the interval (0, 1].")
    if sample_fraction == 1:
        return df

    return df.sampleBy(
        "label",
        fractions={0.0: sample_fraction, 1.0: sample_fraction},
        seed=seed,
    )


def get_candidate_estimators(model_profile):
    """Build candidate estimators for the requested training profile."""
    if model_profile not in MODEL_PROFILES:
        raise ValueError(
            f"Unknown model_profile '{model_profile}'. "
            f"Expected one of: {', '.join(sorted(MODEL_PROFILES))}"
        )

    profile = MODEL_PROFILES[model_profile]
    return {
        "rf": (
            "RandomForest",
            RandomForestClassifier(
                featuresCol="features",
                labelCol="label",
                weightCol="weight",
                numTrees=profile["rf"]["numTrees"],
                maxDepth=profile["rf"]["maxDepth"],
                seed=RANDOM_SEED,
            ),
        ),
        "gbt": (
            "GradientBoostedTrees",
            GBTClassifier(
                featuresCol="features",
                labelCol="label",
                weightCol="weight",
                maxIter=profile["gbt"]["maxIter"],
                maxDepth=profile["gbt"]["maxDepth"],
                seed=RANDOM_SEED,
            ),
        ),
    }


def train_candidate_model(
    model_name,
    estimator,
    train_df,
    validation_df,
    test_df,
    feature_pipeline,
    include_model=True,
):
    """Fit one candidate model and evaluate it on validation and test splits."""
    print(f"\nTraining {model_name}...")
    started_at = time.time()

    pipeline = Pipeline(stages=feature_pipeline.getStages() + [estimator])
    fitted_model = pipeline.fit(train_df)
    training_time = time.time() - started_at

    validation_predictions = fitted_model.transform(validation_df).cache()
    test_predictions = fitted_model.transform(test_df).cache()
    validation_metrics = evaluate_model(validation_predictions)
    test_metrics = evaluate_model(test_predictions)
    test_confusion_matrix = collect_confusion_matrix(test_predictions)
    test_roc_curve = collect_roc_curve(test_predictions)

    print(f"  Training time: {training_time:.1f}s")
    print_metric_block(f"{model_name} validation metrics", validation_metrics)
    print_metric_block(f"{model_name} test metrics", test_metrics)

    validation_predictions.unpersist()
    test_predictions.unpersist()

    result = {
        "name": model_name,
        "training_time_seconds": training_time,
        "validation_metrics": validation_metrics,
        "test_metrics": test_metrics,
        "test_confusion_matrix": test_confusion_matrix,
        "test_roc_curve": test_roc_curve,
    }
    if include_model:
        result["model"] = fitted_model

    return result


def train_paysim(
    spark,
    csv_path=None,
    model_profile="full",
    include_models=True,
    sample_fraction=None,
):
    """
    Train candidate PaySim models and return a structured result bundle.

    The best model is chosen by validation AUC, while test metrics are kept
    separate for reporting.
    """
    weighted_df, data_summary = load_paysim_dataset(
        spark,
        csv_path=csv_path,
    )
    train_df, validation_df, test_df = split_dataset(weighted_df)

    if sample_fraction is not None:
        print(
            "\nInteractive sampling enabled "
            f"(fraction={sample_fraction:.2f} per label)"
        )
        train_df = sample_split(train_df, sample_fraction, RANDOM_SEED)
        validation_df = sample_split(
            validation_df, sample_fraction, RANDOM_SEED + 1
        )
        test_df = sample_split(test_df, sample_fraction, RANDOM_SEED + 2)

        print("Sampled split summary:")
        print(f"  Train:      {train_df.count():,}")
        print(f"  Validation: {validation_df.count():,}")
        print(f"  Test:       {test_df.count():,}")

    feature_pipeline = create_feature_pipeline()

    print(f"\nModel profile: {model_profile}")
    candidates = {}
    candidate_estimators = get_candidate_estimators(model_profile)

    for model_key, (model_name, estimator) in candidate_estimators.items():
        candidates[model_key] = train_candidate_model(
            model_name=model_name,
            estimator=estimator,
            train_df=train_df,
            validation_df=validation_df,
            test_df=test_df,
            feature_pipeline=feature_pipeline,
            include_model=include_models,
        )

        if not include_models:
            try:
                spark._jvm.java.lang.System.gc()
            except Exception:
                pass

    for model_key, result in candidates.items():
        log_to_mlflow(
            run_name=f"paysim-{model_key}",
            validation_metrics=result["validation_metrics"],
            test_metrics=result["test_metrics"],
            params={
                "dataset": "paysim",
                "model_key": model_key,
                "model_profile": model_profile,
                "sample_fraction": (
                    1.0 if sample_fraction is None else sample_fraction
                ),
                "train_fraction": TRAIN_SPLIT,
                "validation_fraction": VALIDATION_SPLIT,
                "test_fraction": TEST_SPLIT,
            },
        )

    return {
        "data_summary": data_summary,
        "model_profile": model_profile,
        "sample_fraction": sample_fraction,
        "candidates": candidates,
    }


def select_best_model(training_results):
    """Select the best candidate model using validation AUC."""
    best_key = max(
        training_results["candidates"],
        key=lambda key: training_results["candidates"][key][
            "validation_metrics"
        ]["auc"],
    )
    alt_key = next(
        key for key in training_results["candidates"] if key != best_key
    )
    return best_key, alt_key


def results_to_rows(training_results):
    """Flatten the candidate metrics into row dictionaries for tables."""
    rows = []
    for model_key, result in training_results["candidates"].items():
        rows.append({
            "model_key": model_key,
            "model_name": result["name"],
            "validation_accuracy": result["validation_metrics"]["accuracy"],
            "validation_precision": result["validation_metrics"]["precision"],
            "validation_recall": result["validation_metrics"]["recall"],
            "validation_auc": result["validation_metrics"]["auc"],
            "validation_f1": result["validation_metrics"]["f1"],
            "test_accuracy": result["test_metrics"]["accuracy"],
            "test_precision": result["test_metrics"]["precision"],
            "test_recall": result["test_metrics"]["recall"],
            "test_auc": result["test_metrics"]["auc"],
            "test_f1": result["test_metrics"]["f1"],
            "training_time_seconds": result["training_time_seconds"],
        })
    return rows


def print_results_table(training_results):
    """Print a concise comparison table for the two PaySim candidates."""
    print("\n" + "=" * 90)
    print("PaySim model comparison")
    print("=" * 90)

    header = (
        f"{'Model':<24} "
        f"{'Val AUC':>10} {'Val F1':>10} "
        f"{'Test AUC':>10} {'Test F1':>10} "
        f"{'Time(s)':>10}"
    )
    print(header)
    print("-" * 90)

    for row in results_to_rows(training_results):
        print(
            f"{row['model_name']:<24} "
            f"{row['validation_auc']:>10.6f} "
            f"{row['validation_f1']:>10.6f} "
            f"{row['test_auc']:>10.6f} "
            f"{row['test_f1']:>10.6f} "
            f"{row['training_time_seconds']:>10.1f}"
        )

    best_key, _ = select_best_model(training_results)
    best_result = training_results["candidates"][best_key]
    print("-" * 90)
    print(
        "Best model by validation AUC: "
        f"{best_result['name']} "
        f"({best_result['validation_metrics']['auc']:.6f})"
    )


def save_models(training_results):
    """
    Save the deployed model and the alternate candidate.

    The main model path is consumed by the streaming pipeline. The alternate
    path is kept for offline comparison or rollback.
    """
    best_key, alt_key = select_best_model(training_results)
    best_result = training_results["candidates"][best_key]
    alt_result = training_results["candidates"][alt_key]

    if "model" not in best_result or "model" not in alt_result:
        raise ValueError(
            "Model artifacts are not present in training_results. "
            "Run train_paysim(..., include_models=True) before save_models()."
        )

    print("\n" + "=" * 60)
    print("Saving models")
    print("=" * 60)

    print(f"\n  Best model ({best_result['name']}) -> {MODEL_OUTPUT}")
    best_result["model"].write().overwrite().save(MODEL_OUTPUT)
    print("  [OK] Saved")

    print(f"  Alternate model ({alt_result['name']}) -> {MODEL_OUTPUT_ALT}")
    alt_result["model"].write().overwrite().save(MODEL_OUTPUT_ALT)
    print("  [OK] Saved")

    cache_model_metrics(
        validation_metrics=best_result["validation_metrics"],
        test_metrics=best_result["test_metrics"],
        training_time=best_result["training_time_seconds"],
    )

    return best_key, alt_key


def main(spark):
    """Run the full PaySim training pipeline end-to-end."""
    print("=" * 60)
    print("PaySim fraud detection model training")
    print("  Models: RandomForest vs GradientBoostedTrees")
    print("=" * 60)

    training_results = train_paysim(spark)
    print_results_table(training_results)
    best_key, _ = save_models(training_results)
    best_result = training_results["candidates"][best_key]

    print("\n" + "=" * 60)
    print("Training complete")
    print(
        f"  Deployed model: {best_result['name']} "
        f"(validation AUC={best_result['validation_metrics']['auc']:.6f})"
    )
    print("=" * 60)

    return training_results


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("FraudDetection-Training-PaySim")
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        main(spark)
    finally:
        spark.stop()
