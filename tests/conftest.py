"""
Pytest fixtures shared across test modules.
"""
import os
import sys
import pytest

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "spark"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "serving"))

SPARK_TEST_ROOT = os.path.join(PROJECT_ROOT, ".spark-test")
SPARK_WAREHOUSE_DIR = os.path.join(SPARK_TEST_ROOT, "warehouse")
SPARK_LOCAL_DIR = os.path.join(SPARK_TEST_ROOT, "local")


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing (local mode)."""
    try:
        from pyspark.sql import SparkSession
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        os.makedirs(SPARK_WAREHOUSE_DIR, exist_ok=True)
        os.makedirs(SPARK_LOCAL_DIR, exist_ok=True)

        session = (
            SparkSession.builder
            .master("local[2]")
            .appName("FraudDetection-Tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "1g")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR)
            .config("spark.local.dir", SPARK_LOCAL_DIR)
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        yield session
        session.stop()
    except ImportError:
        pytest.skip("PySpark not installed")


@pytest.fixture
def sample_paysim_data(spark):
    """Create a sample PaySim-like DataFrame for testing."""
    data = [
        # Normal PAYMENT transaction
        (1, "PAYMENT", 9839.64, "C1231006815", 170136.0, 160296.36,
         "M1979787155", 0.0, 0.0, 0, 0),
        # Normal TRANSFER
        (1, "TRANSFER", 181.0, "C1305486145", 181.0, 0.0,
         "C553264065", 0.0, 0.0, 0, 0),
        # Fraudulent TRANSFER (drains account to zero)
        (1, "TRANSFER", 181000.0, "C840083671", 181000.0, 0.0,
         "C38997010", 0.0, 0.0, 1, 0),
        # Fraudulent CASH_OUT (large amount)
        (1, "CASH_OUT", 500000.0, "C2054744914", 500000.0, 0.0,
         "C1286084959", 21182.0, 521182.0, 1, 1),
        # Normal CASH_IN
        (2, "CASH_IN", 1000.0, "C1234567890", 5000.0, 6000.0,
         "M9876543210", 0.0, 0.0, 0, 0),
        # Normal DEBIT
        (3, "DEBIT", 5000.0, "C1111111111", 50000.0, 45000.0,
         "M2222222222", 0.0, 0.0, 0, 0),
    ]

    columns = [
        "step", "type", "amount", "nameOrig",
        "oldbalanceOrg", "newbalanceOrig",
        "nameDest", "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud",
    ]

    return spark.createDataFrame(data, columns)
