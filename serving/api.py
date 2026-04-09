"""
FastAPI serving layer for fraud detection results.

Endpoints:
    GET /health           - Health check
    GET /kpis             - Transaction and fraud KPIs
    GET /recent-alerts    - Latest 50 fraud alerts
    GET /stream/alerts    - SSE real-time fraud alert stream
"""
import os
import json
import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
import redis.asyncio as aioredis

from redis_listener import RedisAlertListener


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_CHANNEL = "fraud_alerts"


# Global state
redis_pool: Optional[aioredis.Redis] = None
alert_listener: Optional[RedisAlertListener] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle."""
    global redis_pool, alert_listener

    # Startup
    redis_pool = aioredis.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        decode_responses=True,
        socket_timeout=5,
    )
    alert_listener = RedisAlertListener(
        REDIS_HOST, REDIS_PORT, REDIS_CHANNEL
    )
    asyncio.create_task(alert_listener.start())
    print(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

    yield

    # Shutdown
    await alert_listener.stop()
    await redis_pool.close()


app = FastAPI(
    title="Fraud Detection API",
    description="Real-time fraud detection serving layer",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Response Models
# ============================================================
class HealthResponse(BaseModel):
    status: str
    redis: str


class KPIResponse(BaseModel):
    total_transactions: int
    total_fraud: int
    total_ml_alerts: int
    total_rule_alerts: int
    fraud_rate: float


class AlertItem(BaseModel):
    step: Optional[int] = None
    type: Optional[str] = None
    amount: Optional[float] = None
    nameOrig: Optional[str] = None
    nameDest: Optional[str] = None
    fraud_probability: Optional[float] = None
    oldbalanceOrg: Optional[float] = None
    newbalanceOrig: Optional[float] = None
    is_ml_alert: Optional[int] = None
    is_blacklist_destination: Optional[int] = None
    is_rule_alert: Optional[int] = None
    alert_source: Optional[str] = None


# ============================================================
# Endpoints
# ============================================================
@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    try:
        await redis_pool.ping()
        redis_status = "connected"
    except Exception:
        redis_status = "disconnected"

    return HealthResponse(status="ok", redis=redis_status)


@app.get("/kpis", response_model=KPIResponse)
async def get_kpis():
    """
    Get transaction and fraud KPI counters.

    These counters are atomically incremented by the Spark streaming
    pipeline via Redis INCRBY.
    """
    total_txn = await redis_pool.get("total_transactions")
    total_fraud = await redis_pool.get("total_fraud")
    total_ml_alerts = await redis_pool.get("total_ml_alerts")
    total_rule_alerts = await redis_pool.get("total_rule_alerts")

    total_txn = int(total_txn) if total_txn else 0
    total_fraud = int(total_fraud) if total_fraud else 0
    total_ml_alerts = int(total_ml_alerts) if total_ml_alerts else 0
    total_rule_alerts = int(total_rule_alerts) if total_rule_alerts else 0
    fraud_rate = (
        (total_fraud / total_txn * 100) if total_txn > 0 else 0.0
    )

    return KPIResponse(
        total_transactions=total_txn,
        total_fraud=total_fraud,
        total_ml_alerts=total_ml_alerts,
        total_rule_alerts=total_rule_alerts,
        fraud_rate=round(fraud_rate, 4),
    )


@app.get("/recent-alerts")
async def get_recent_alerts(
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Get the most recent fraud alerts.

    Alerts are stored in a Redis list by the Spark streaming pipeline.
    """
    raw_alerts = await redis_pool.lrange("recent_alerts", 0, limit - 1)
    alerts = []
    for raw in raw_alerts:
        try:
            alerts.append(json.loads(raw))
        except json.JSONDecodeError:
            continue

    return JSONResponse(content={
        "count": len(alerts),
        "alerts": alerts,
    })


@app.get("/stream/alerts")
async def stream_alerts():
    """
    SSE (Server-Sent Events) endpoint for real-time fraud alerts.

    Connect with EventSource in browser:
        const es = new EventSource('/stream/alerts');
        es.onmessage = (e) => console.log(JSON.parse(e.data));
    """
    async def event_generator():
        queue = alert_listener.subscribe()
        try:
            while True:
                try:
                    alert = await asyncio.wait_for(
                        queue.get(), timeout=30.0
                    )
                    yield {"event": "fraud_alert", "data": alert}
                except asyncio.TimeoutError:
                    # Send keepalive to prevent connection timeout
                    yield {"event": "keepalive", "data": ""}
        except asyncio.CancelledError:
            alert_listener.unsubscribe(queue)
            raise

    return EventSourceResponse(event_generator())


@app.get("/model-metrics")
async def get_model_metrics():
    """
    Get cached model metrics (stored by train_model.py).

    Metrics are stored as a Redis hash by the training pipeline.
    """
    metrics = await redis_pool.hgetall("model_metrics")
    if not metrics:
        return JSONResponse(
            content={"message": "No metrics available"},
            status_code=404,
        )
    return JSONResponse(content={
        k: float(v) for k, v in metrics.items()
    })
