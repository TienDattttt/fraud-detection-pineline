"""
Integration tests for FastAPI serving layer.

Tests API endpoints with mocked Redis.
"""
import json
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def mock_redis():
    """Create a mock async Redis client."""
    mock = AsyncMock()
    mock.ping = AsyncMock(return_value=True)
    mock.get = AsyncMock(return_value=None)
    mock.lrange = AsyncMock(return_value=[])
    mock.hgetall = AsyncMock(return_value={})
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def client(mock_redis):
    """Create a test client with mocked Redis."""
    import sys
    import os
    sys.path.insert(
        0, os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "serving"
        )
    )

    # Patch Redis before importing the app
    with patch("api.aioredis") as mock_aioredis:
        mock_aioredis.Redis.return_value = mock_redis
        from api import app
        import api
        api.redis_pool = mock_redis

        with TestClient(app, raise_server_exceptions=False) as c:
            yield c


class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_health_ok(self, client, mock_redis):
        """Health endpoint returns 200 with status ok."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"


class TestKPIsEndpoint:
    """Tests for GET /kpis."""

    def test_kpis_empty(self, client, mock_redis):
        """KPIs return zeros when no data."""
        mock_redis.get = AsyncMock(return_value=None)
        response = client.get("/kpis")
        assert response.status_code == 200
        data = response.json()
        assert data["total_transactions"] == 0
        assert data["total_fraud"] == 0
        assert data["total_ml_alerts"] == 0
        assert data["total_rule_alerts"] == 0
        assert data["fraud_rate"] == 0.0

    def test_kpis_with_data(self, client, mock_redis):
        """KPIs return correct values."""
        mock_redis.get = AsyncMock(
            side_effect=lambda k: "1000" if k == "total_transactions"
            else "13" if k == "total_fraud"
            else "8" if k == "total_ml_alerts"
            else "5" if k == "total_rule_alerts" else None
        )
        response = client.get("/kpis")
        assert response.status_code == 200
        data = response.json()
        assert data["total_transactions"] == 1000
        assert data["total_fraud"] == 13
        assert data["total_ml_alerts"] == 8
        assert data["total_rule_alerts"] == 5
        assert data["fraud_rate"] == pytest.approx(1.3, rel=0.01)


class TestRecentAlertsEndpoint:
    """Tests for GET /recent-alerts."""

    def test_recent_alerts_empty(self, client, mock_redis):
        """Returns empty list when no alerts."""
        mock_redis.lrange = AsyncMock(return_value=[])
        response = client.get("/recent-alerts")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert data["alerts"] == []

    def test_recent_alerts_with_data(self, client, mock_redis):
        """Returns parsed alerts from Redis list."""
        alerts = [
            json.dumps({
                "type": "TRANSFER",
                "amount": 500000,
                "rule_score": 1.0,
                "hybrid_score": 0.965,
                "alert_source": "ML+BLACKLIST_RULE",
            }),
            json.dumps({"type": "CASH_OUT", "amount": 200000}),
        ]
        mock_redis.lrange = AsyncMock(return_value=alerts)
        response = client.get("/recent-alerts")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert data["alerts"][0]["type"] == "TRANSFER"
        assert data["alerts"][0]["hybrid_score"] == pytest.approx(0.965)
        assert data["alerts"][0]["rule_score"] == pytest.approx(1.0)
        assert data["alerts"][0]["alert_source"] == "ML+BLACKLIST_RULE"

    def test_recent_alerts_limit(self, client, mock_redis):
        """Respects limit parameter."""
        mock_redis.lrange = AsyncMock(return_value=[])
        response = client.get("/recent-alerts?limit=10")
        assert response.status_code == 200
        mock_redis.lrange.assert_called_once_with("recent_alerts", 0, 9)


class TestModelMetricsEndpoint:
    """Tests for GET /model-metrics."""

    def test_no_metrics(self, client, mock_redis):
        """Returns 404 when no metrics available."""
        mock_redis.hgetall = AsyncMock(return_value={})
        response = client.get("/model-metrics")
        assert response.status_code == 404

    def test_with_metrics(self, client, mock_redis):
        """Returns metrics as floats."""
        mock_redis.hgetall = AsyncMock(return_value={
            "accuracy": "0.998",
            "auc": "0.975",
        })
        response = client.get("/model-metrics")
        assert response.status_code == 200
        data = response.json()
        assert data["accuracy"] == pytest.approx(0.998)
        assert data["auc"] == pytest.approx(0.975)
