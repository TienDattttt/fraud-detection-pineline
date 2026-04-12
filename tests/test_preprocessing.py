"""
Unit tests for spark/preprocessing.py.

Tests feature engineering functions and ML pipeline creation.
All tests use PySpark local mode — no cluster needed.
"""
import pytest


class TestCleanPaySimDataframe:
    """Test raw PaySim cleaning and normalization rules."""

    def test_trim_and_normalize_strings(self, spark):
        """type and account identifiers should be trimmed and uppercased."""
        from preprocessing import clean_paysim_dataframe

        rows = [
            (
                1, " transfer ", 100.0, " c123 ", 500.0, 400.0,
                " c999 ", 0.0, 100.0, 0, None,
            ),
        ]
        columns = [
            "step", "type", "amount", "nameOrig",
            "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest",
            "isFraud", "isFlaggedFraud",
        ]

        row = clean_paysim_dataframe(
            spark.createDataFrame(rows, columns)
        ).collect()[0]

        assert row["type"] == "TRANSFER"
        assert row["nameOrig"] == "C123"
        assert row["nameDest"] == "C999"
        assert row["isFlaggedFraud"] == 0

    def test_drop_invalid_type_and_blank_destination(self, spark):
        """Rows with unsupported transaction types or blank IDs are dropped."""
        from preprocessing import clean_paysim_dataframe

        rows = [
            (
                1, "PAYMENT", 100.0, "C1", 500.0, 400.0,
                "M1", 0.0, 100.0, 0, 0,
            ),
            (
                2, "UNKNOWN", 50.0, "C2", 500.0, 450.0,
                "M2", 0.0, 50.0, 0, 0,
            ),
            (
                3, "TRANSFER", 50.0, "C3", 500.0, 450.0,
                " ", 0.0, 50.0, 0, 0,
            ),
        ]
        columns = [
            "step", "type", "amount", "nameOrig",
            "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest",
            "isFraud", "isFlaggedFraud",
        ]

        cleaned = clean_paysim_dataframe(
            spark.createDataFrame(rows, columns)
        )

        assert cleaned.count() == 1
        assert cleaned.collect()[0]["type"] == "PAYMENT"


class TestAddEngineeredFeatures:
    """Test add_engineered_features() function."""

    def test_balance_diff_orig(self, sample_paysim_data):
        """Balance diff should equal old - new balance."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        row = result.collect()[0]

        expected = row["oldbalanceOrg"] - row["newbalanceOrig"]
        assert row["balance_diff_orig"] == pytest.approx(expected)

    def test_balance_diff_dest(self, sample_paysim_data):
        """Dest balance diff should equal new - old."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        # Use the CASH_OUT row (index 3) which has non-zero dest balances
        rows = result.collect()
        cashout_row = rows[3]

        expected = (
            cashout_row["newbalanceDest"]
            - cashout_row["oldbalanceDest"]
        )
        assert cashout_row["balance_diff_dest"] == pytest.approx(expected)

    def test_amount_ratio(self, sample_paysim_data):
        """Amount ratio = amount / (oldbalanceOrg + 1)."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        row = result.collect()[0]

        expected = row["amount"] / (row["oldbalanceOrg"] + 1.0)
        assert row["amount_ratio"] == pytest.approx(expected, rel=1e-4)

    def test_is_zero_balance_fraud(self, sample_paysim_data):
        """Fraud transaction that drains account should have is_zero_balance=1."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        # Row index 2: fraud TRANSFER, newbalanceOrig = 0
        fraud_row = result.collect()[2]

        assert fraud_row["is_zero_balance_orig"] == 1.0

    def test_is_zero_balance_normal(self, sample_paysim_data):
        """Normal transaction with remaining balance should have is_zero_balance=0."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        # Row index 0: normal PAYMENT, newbalanceOrig = 160296.36
        normal_row = result.collect()[0]

        assert normal_row["is_zero_balance_orig"] == 0.0

    def test_is_large_amount(self, sample_paysim_data):
        """Transaction > 200,000 should be flagged as large."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        rows = result.collect()

        # Row 3: CASH_OUT 500,000 → large
        assert rows[3]["is_large_amount"] == 1.0
        # Row 0: PAYMENT 9,839.64 → not large
        assert rows[0]["is_large_amount"] == 0.0

    def test_output_columns_added(self, sample_paysim_data):
        """Should add exactly 5 engineered feature columns."""
        from preprocessing import add_engineered_features

        result = add_engineered_features(sample_paysim_data)
        new_cols = set(result.columns) - set(sample_paysim_data.columns)

        expected_new = {
            "balance_diff_orig",
            "balance_diff_dest",
            "amount_ratio",
            "is_zero_balance_orig",
            "is_large_amount",
        }
        assert new_cols == expected_new


class TestPrepareDataframe:
    """Test prepare_dataframe() function."""

    def test_label_column_created(self, sample_paysim_data):
        """isFraud should be renamed to label."""
        from preprocessing import prepare_dataframe

        result = prepare_dataframe(sample_paysim_data)
        assert "label" in result.columns
        assert "isFraud" not in result.columns

    def test_label_values(self, sample_paysim_data):
        """Label should be double (0.0 or 1.0)."""
        from preprocessing import prepare_dataframe

        result = prepare_dataframe(sample_paysim_data)
        labels = [row["label"] for row in result.select("label").collect()]

        assert all(isinstance(l, float) for l in labels)
        assert set(labels) == {0.0, 1.0}


class TestCreateFeaturePipeline:
    """Test create_feature_pipeline() function."""

    def test_pipeline_stages(self):
        """Pipeline should have 3 stages: StringIndexer, VectorAssembler, StandardScaler."""
        pytest.importorskip("pyspark")
        from preprocessing import create_feature_pipeline

        pipeline = create_feature_pipeline()
        stages = pipeline.getStages()

        assert len(stages) == 3
        assert "StringIndexer" in type(stages[0]).__name__
        assert "VectorAssembler" in type(stages[1]).__name__
        assert "StandardScaler" in type(stages[2]).__name__

    def test_pipeline_fit_transform(self, sample_paysim_data):
        """Pipeline should fit and produce 'features' column."""
        from preprocessing import (
            create_feature_pipeline,
            prepare_dataframe,
        )

        prepared = prepare_dataframe(sample_paysim_data)
        pipeline = create_feature_pipeline()

        model = pipeline.fit(prepared)
        result = model.transform(prepared)

        assert "features" in result.columns
        assert "type_index" in result.columns
        assert result.count() == sample_paysim_data.count()


class TestStreamingBlacklistRules:
    """Test blacklist-based rule alert enrichment."""

    def test_blacklist_transfer_becomes_rule_alert(self, sample_paysim_data):
        """TRANSFER into a blacklisted destination should trigger rule alert."""
        from streaming_pipeline import add_rule_based_alerts
        from pyspark.sql import functions as F

        predictions = sample_paysim_data.withColumn("prediction", F.lit(0.0))
        predictions = predictions.withColumn(
            "fraud_probability", F.lit(0.05)
        )

        result = add_rule_based_alerts(
            predictions,
            blacklist_accounts=["C553264065"],
        )
        transfer_row = next(
            row for row in result.collect()
            if row["nameDest"] == "C553264065"
        )

        assert transfer_row["is_blacklist_destination"] == 1.0
        assert transfer_row["is_rule_alert"] == 1.0
        assert transfer_row["is_alert"] == 1.0
        assert transfer_row["alert_source"] == "BLACKLIST_RULE"

    def test_ml_alert_and_blacklist_merge_sources(self, sample_paysim_data):
        """ML and blacklist alerts should be merged into a combined source."""
        from streaming_pipeline import add_rule_based_alerts
        from pyspark.sql import functions as F

        predictions = sample_paysim_data.withColumn(
            "prediction",
            F.when(F.col("nameDest") == "C553264065", 1.0).otherwise(0.0)
        ).withColumn("fraud_probability", F.lit(0.95))

        result = add_rule_based_alerts(
            predictions,
            blacklist_accounts=["C553264065"],
        )
        transfer_row = next(
            row for row in result.collect()
            if row["nameDest"] == "C553264065"
        )

        assert transfer_row["is_ml_alert"] == 1.0
        assert transfer_row["is_rule_alert"] == 1.0
        assert transfer_row["is_alert"] == 1.0
        assert transfer_row["alert_source"] == "ML+BLACKLIST_RULE"

    def test_blacklist_non_transfer_does_not_trigger_rule(self, spark):
        """A blacklisted destination alone should not alert on non-transfer types."""
        from streaming_pipeline import add_rule_based_alerts
        from pyspark.sql import functions as F

        data = [
            (
                1, "CASH_OUT", 9000.0, "C100", 9000.0, 0.0,
                "C553264065", 0.0, 9000.0, 0, 0,
            ),
        ]
        columns = [
            "step", "type", "amount", "nameOrig",
            "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest",
            "isFraud", "isFlaggedFraud",
        ]

        predictions = spark.createDataFrame(data, columns).withColumn(
            "prediction", F.lit(0.0)
        ).withColumn("fraud_probability", F.lit(0.02))

        row = add_rule_based_alerts(
            predictions,
            blacklist_accounts=["C553264065"],
        ).collect()[0]

        assert row["is_blacklist_destination"] == 1.0
        assert row["is_rule_alert"] == 0.0
        assert row["is_alert"] == 0.0
        assert row["alert_source"] == "NONE"
