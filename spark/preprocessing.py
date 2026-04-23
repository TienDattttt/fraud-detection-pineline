"""
PaySim preprocessing utilities shared by training and streaming.

All heavy transformations use PySpark only.
"""
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


TRANSACTION_TYPES = [
    "CASH_IN",
    "CASH_OUT",
    "DEBIT",
    "PAYMENT",
    "TRANSFER",
]

STRING_COLUMNS = ["type", "nameOrig", "nameDest"]
FLOAT_COLUMNS = [
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]
INT_COLUMNS = ["step", "isFlaggedFraud"]
#Validate cột
REQUIRED_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]
CRITICAL_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
]

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
ASSEMBLER_INPUT_COLS = NUMERIC_FEATURES + ["type_index"]

#Validate cột
def validate_required_columns(df: DataFrame) -> None:
    """Raise a clear error when the PaySim schema is incomplete."""
    missing_columns = sorted(
        set(REQUIRED_COLUMNS).difference(df.columns)
    )
    if missing_columns:
        raise ValueError(
            "Missing required PaySim columns: "
            + ", ".join(missing_columns)
        )

#Chuẩn hóa chuỗi
def normalize_string_columns(df: DataFrame) -> DataFrame:
    """Trim and normalize string columns used by the PaySim pipeline."""
    result = df
    result = result.withColumn(
        "type",
        F.upper(
            F.regexp_replace(
                F.trim(F.coalesce(F.col("type"), F.lit(""))),
                r"\s+",
                "_",
            )
        ),
    )

    for column_name in ("nameOrig", "nameDest"):
        result = result.withColumn(
            column_name,
            F.upper(F.trim(F.coalesce(F.col(column_name), F.lit("")))),
        )

    return result

#Làm sạch dữ liệu
def clean_paysim_dataframe(df: DataFrame) -> DataFrame:
    """
    Clean and standardize a raw PaySim DataFrame.

    Steps:
    1. Validate the expected schema.
    2. Cast numeric columns to the correct Spark types.
    3. Trim and normalize string columns.
    4. Fill nullable operational fields when a safe default exists.
    5. Drop invalid or incomplete rows.
    """
    validate_required_columns(df)

    result = df

    for column_name in INT_COLUMNS:
        result = result.withColumn(
            column_name, F.col(column_name).cast("integer")
        )

    for column_name in FLOAT_COLUMNS:
        result = result.withColumn(
            column_name, F.col(column_name).cast("double")
        )

    result = result.withColumn("isFraud", F.col("isFraud").cast("double"))
    result = normalize_string_columns(result)
    result = result.withColumn(
        "isFlaggedFraud",
        F.coalesce(F.col("isFlaggedFraud"), F.lit(0)).cast("integer"),
    )
    result = result.dropna(subset=CRITICAL_COLUMNS)
    result = result.filter(
        (F.length(F.col("type")) > 0)
        & (F.length(F.col("nameOrig")) > 0)
        & (F.length(F.col("nameDest")) > 0)
    )
    result = result.filter(F.col("type").isin(TRANSACTION_TYPES))

    return result


def add_engineered_features(df: DataFrame) -> DataFrame:
    """
    Add domain-specific features for fraud detection.

    These features capture common fraud patterns in e-wallet transactions:
    - Draining account to zero after a transfer/cash-out.
    - Large-value transactions.
    - Balance movement mismatches between origin and destination accounts.
    """
    result = df

    result = result.withColumn(
        "balance_diff_orig",
        F.col("oldbalanceOrg") - F.col("newbalanceOrig"),
    )
    result = result.withColumn(
        "balance_diff_dest",
        F.col("newbalanceDest") - F.col("oldbalanceDest"),
    )
    result = result.withColumn(
        "amount_ratio",
        F.col("amount") / (F.col("oldbalanceOrg") + F.lit(1.0)),
    )
    result = result.withColumn(
        "is_zero_balance_orig",
        F.when(F.col("newbalanceOrig") == 0.0, 1.0).otherwise(0.0),
    )
    result = result.withColumn(
        "is_large_amount",
        F.when(F.col("amount") > 200000.0, 1.0).otherwise(0.0),
    )

    return result

#Tạo feature pipeline
def create_feature_pipeline() -> Pipeline:
    """
    Build the PySpark feature pipeline used by the deployed model.

    Steps:
    1. Encode the transaction type with StringIndexer.
    2. Assemble numeric and encoded features.
    3. Scale the resulting vector.
    """
    type_indexer = StringIndexer(
        inputCol="type",
        outputCol="type_index",
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=ASSEMBLER_INPUT_COLS,
        outputCol="raw_features",
        handleInvalid="keep",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False,
    )

    return Pipeline(stages=[type_indexer, assembler, scaler])


def prepare_dataframe(df: DataFrame) -> DataFrame:
    """
    Prepare a raw PaySim DataFrame for ML training and offline evaluation.

    The returned DataFrame is cleaned, enriched, and exposes the label column
    expected by the PySpark classification pipeline.
    """
    result = clean_paysim_dataframe(df)
    result = add_engineered_features(result)
    result = result.withColumnRenamed("isFraud", "label")
    return result
