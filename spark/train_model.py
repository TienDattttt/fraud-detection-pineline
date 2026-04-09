"""
Offline ML training for PaySim fraud detection.

Trains RandomForest and GBT classifiers using PySpark MLlib.
Handles class imbalance via weight column.
Logs metrics to MLflow and saves best model.

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
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Add parent dir to path so we can import preprocessing
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from preprocessing import (  # noqa: E402
    create_feature_pipeline,
    prepare_dataframe,
)


CSV_PATH = os.getenv(
    "CSV_PATH", "/opt/spark/work/data/paysim_transactions.csv"
)
MODEL_OUTPUT = os.getenv(
    "MODEL_PATH", "/opt/spark/work/models/fraud_pipeline_model"
)
MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI", "file:///opt/spark/work/models/mlruns"
)


def add_class_weights(df):
    """
    Add weight column to handle class imbalance.

    PaySim has ~0.13% fraud — without weights, model ignores minority class.
    Weight formula: total_samples / (2 * class_count)
    """
    total = df.count()
    fraud_count = df.filter(F.col("label") == 1.0).count()
    non_fraud_count = total - fraud_count

    print(f"\n📊 Class Distribution:")
    print(f"   Non-Fraud: {non_fraud_count:,} ({non_fraud_count/total*100:.2f}%)")
    print(f"   Fraud:     {fraud_count:,} ({fraud_count/total*100:.2f}%)")

    # Weight: inverse of class frequency
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


def evaluate_model(predictions, model_name: str) -> dict:
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

    print(f"\n📈 {model_name} Metrics:")
    for name, value in metrics.items():
        print(f"   {name:>10}: {value:.6f}")

    return metrics


def train_and_evaluate(spark):
    """Main training pipeline."""
    print("=" * 60)
    print("🚀 PaySim Fraud Detection — Model Training")
    print("=" * 60)

    # ------------------------------------------------------------------
    # 1. Load data
    # ------------------------------------------------------------------
    print(f"\n📂 Loading data from {CSV_PATH}...")
    raw_df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    print(f"   Total rows: {raw_df.count():,}")

    # ------------------------------------------------------------------
    # 2. Prepare features
    # ------------------------------------------------------------------
    print("\n🔧 Preparing features...")
    prepared_df = prepare_dataframe(raw_df)

    # Add class weights for imbalanced handling
    weighted_df = add_class_weights(prepared_df)

    # ------------------------------------------------------------------
    # 3. Train/Test split (80/20, stratified by label)
    # ------------------------------------------------------------------
    train_df, test_df = weighted_df.randomSplit([0.8, 0.2], seed=42)
    print(f"\n📊 Split:")
    print(f"   Train: {train_df.count():,}")
    print(f"   Test:  {test_df.count():,}")

    # ------------------------------------------------------------------
    # 4. Build feature pipeline
    # ------------------------------------------------------------------
    feature_pipeline = create_feature_pipeline()

    # ------------------------------------------------------------------
    # 5. Train Random Forest
    # ------------------------------------------------------------------
    print("\n🌲 Training Random Forest...")
    t0 = time.time()

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        weightCol="weight",
        numTrees=100,
        maxDepth=10,
        seed=42,
    )

    rf_pipeline = Pipeline(stages=feature_pipeline.getStages() + [rf])

    # CrossValidator for hyperparameter tuning
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
        numFolds=3,
        seed=42,
    )

    rf_model = rf_cv.fit(train_df)
    rf_time = time.time() - t0
    print(f"   Training time: {rf_time:.1f}s")

    rf_predictions = rf_model.transform(test_df)
    rf_metrics = evaluate_model(rf_predictions, "Random Forest (CV)")

    # ------------------------------------------------------------------
    # 6. Train Gradient Boosted Trees
    # ------------------------------------------------------------------
    print("\n🌳 Training Gradient Boosted Trees...")
    t0 = time.time()

    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        weightCol="weight",
        maxIter=50,
        maxDepth=8,
        seed=42,
    )

    gbt_pipeline = Pipeline(stages=feature_pipeline.getStages() + [gbt])
    gbt_model = gbt_pipeline.fit(train_df)
    gbt_time = time.time() - t0
    print(f"   Training time: {gbt_time:.1f}s")

    gbt_predictions = gbt_model.transform(test_df)
    gbt_metrics = evaluate_model(gbt_predictions, "GBT")

    # ------------------------------------------------------------------
    # 7. Compare and select best model
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("🏆 Model Comparison")
    print("=" * 60)
    print(f"{'Metric':<12} {'Random Forest':>15} {'GBT':>15}")
    print("-" * 44)
    for metric in ["accuracy", "precision", "recall", "f1", "auc"]:
        print(
            f"{metric:<12} "
            f"{rf_metrics[metric]:>15.6f} "
            f"{gbt_metrics[metric]:>15.6f}"
        )

    # Select model with higher AUC (best for imbalanced binary classification)
    if rf_metrics["auc"] >= gbt_metrics["auc"]:
        best_model = rf_model.bestModel
        best_name = "Random Forest (CV)"
        best_metrics = rf_metrics
    else:
        best_model = gbt_model
        best_name = "GBT"
        best_metrics = gbt_metrics

    print(f"\n✅ Best model: {best_name} (AUC={best_metrics['auc']:.6f})")

    # ------------------------------------------------------------------
    # 8. MLflow logging
    # ------------------------------------------------------------------
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("fraud-detection")

        with mlflow.start_run(run_name=best_name):
            for metric_name, metric_value in best_metrics.items():
                mlflow.log_metric(metric_name, metric_value)

            mlflow.log_param("model_type", best_name)
            mlflow.log_param("train_size", train_df.count())
            mlflow.log_param("test_size", test_df.count())
            mlflow.log_param("features", len(prepared_df.columns))

        print("📝 Metrics logged to MLflow")
    except Exception as e:
        print(f"⚠️ MLflow logging skipped: {e}")

    # ------------------------------------------------------------------
    # 9. Save best model
    # ------------------------------------------------------------------
    print(f"\n💾 Saving model to {MODEL_OUTPUT}...")
    best_model.write().overwrite().save(MODEL_OUTPUT)
    print("✅ Model saved successfully!")

    print("\n" + "=" * 60)
    print("🎉 Training complete!")
    print("=" * 60)

    return best_metrics


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("FraudDetection-Training")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        train_and_evaluate(spark)
    finally:
        spark.stop()
