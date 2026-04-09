"""
Feature engineering for PaySim fraud detection.

All transformations use PySpark ONLY — no Pandas.
This module is shared between train_model.py and streaming_pipeline.py.
"""
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
    StandardScaler,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# PaySim transaction types
TRANSACTION_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]

# Numeric features to assemble
NUMERIC_FEATURES = [
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "balance_diff_orig",
    "balance_diff_dest",
    "amount_ratio",
    "is_zero_balance_orig",
    "is_large_amount",
]

# Final feature vector column names (after StringIndexer)
ASSEMBLER_INPUT_COLS = NUMERIC_FEATURES + ["type_index"]


def add_engineered_features(df: DataFrame) -> DataFrame:
    """
    Add domain-specific features for fraud detection.

    These features capture common fraud patterns in e-wallet transactions:
    - Draining account to zero (TRANSFER/CASH_OUT fraud pattern)
    - Amount larger than current balance
    - Mismatches between expected and actual balance changes
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
    # +1 to avoid division by zero
    result = result.withColumn(
        "amount_ratio",
        F.col("amount") / (F.col("oldbalanceOrg") + F.lit(1.0))
    )

    # Binary: origin account drained to zero after transaction
    result = result.withColumn(
        "is_zero_balance_orig",
        F.when(F.col("newbalanceOrig") == 0.0, 1.0).otherwise(0.0)
    )

    # Binary: transaction amount > 200,000 (PaySim units)
    result = result.withColumn(
        "is_large_amount",
        F.when(F.col("amount") > 200000.0, 1.0).otherwise(0.0)
    )

    return result


def create_feature_pipeline() -> Pipeline:
    """
    Build a PySpark ML Pipeline for feature engineering.

    Steps:
    1. StringIndexer: type (categorical) → type_index (numeric)
    2. VectorAssembler: combine all features → raw_features
    3. StandardScaler: normalize → features

    Returns:
        Pipeline ready to .fit() on training data
    """
    # Encode transaction type as numeric index
    type_indexer = StringIndexer(
        inputCol="type",
        outputCol="type_index",
        handleInvalid="keep",
    )

    # Combine all numeric features into a single vector
    assembler = VectorAssembler(
        inputCols=ASSEMBLER_INPUT_COLS,
        outputCol="raw_features",
        handleInvalid="keep",
    )

    # Scale features to zero mean, unit variance
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False,  # Sparse vectors don't support withMean=True
    )

    return Pipeline(stages=[type_indexer, assembler, scaler])


def prepare_dataframe(df: DataFrame) -> DataFrame:
    """
    Prepare raw PaySim DataFrame for ML pipeline.

    Casts types, adds engineered features, renames label column.
    """
    result = df

    # Ensure correct types (CSV reads everything as string)
    result = (
        result
        .withColumn("step", F.col("step").cast("integer"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("oldbalanceOrg", F.col("oldbalanceOrg").cast("double"))
        .withColumn("newbalanceOrig", F.col("newbalanceOrig").cast("double"))
        .withColumn("oldbalanceDest", F.col("oldbalanceDest").cast("double"))
        .withColumn("newbalanceDest", F.col("newbalanceDest").cast("double"))
        .withColumn("isFraud", F.col("isFraud").cast("double"))
        .withColumn(
            "isFlaggedFraud",
            F.col("isFlaggedFraud").cast("integer")
        )
    )

    # Add engineered features
    result = add_engineered_features(result)

    # Rename label column for ML
    result = result.withColumnRenamed("isFraud", "label")

    return result
