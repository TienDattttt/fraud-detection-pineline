# Real-time Fraud Detection Pipeline


## Kiến trúc hệ thống

```
┌──────────────────────────── LOCAL (Docker Compose) ─────────────────────────────┐
│                                                                                 │
│  PaySim CSV ──► Kafka Producer ──► Kafka (KRaft) ──► Spark Structured Streaming │
│                                                        │                        │
│                                          ┌─────────────┼──────────────┐         │
│                                          ▼             ▼              ▼         │
│                                     Load Model    HDFS Parquet    Redis Pub/Sub │
│                                  (PipelineModel)  (dt/hour part)  (fraud_alerts)│
│                                                                       │         │
│                                                          FastAPI ◄────┘         │
│                                                     /kpis /alerts /stream       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

##  Yêu cầu

- Docker & Docker Compose
- Python 3.11+
- ~4GB RAM trống cho Docker containers
- Tài khoản Kaggle (để tải dataset) hoặc tải thủ công

##  Quick Start

### 1. Clone repo
```bash
git clone <repo-url>
cd fraud-detection-pipeline
```

### 2. Download dataset (PaySim)
```bash
python scripts/download_dataset.py
```
Hoặc tải thủ công từ [Kaggle PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1), đặt file CSV vào `data/paysim_transactions.csv`.

### 3. Khởi động pipeline
```bash
chmod +x scripts/start_pipeline.sh
./scripts/start_pipeline.sh
```

Windows PowerShell:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_pipeline.ps1
```

### 4. Train model (lần đầu)
```bash
docker exec spark-master spark-submit /opt/spark/work/spark/train_model.py
```

### 5. Chạy Streaming Pipeline
```bash
docker exec spark-master spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /opt/spark/work/spark/streaming_pipeline.py
```

### 6. Chạy Producer (terminal khác)
```bash
docker exec spark-master python3 /opt/spark/work/producer/kafka_producer.py --speed 0.5
```

### 7. Kiểm tra kết quả
- API docs: http://localhost:8000/docs
- KPIs: http://localhost:8000/kpis
- Alerts: http://localhost:8000/recent-alerts
- Real-time SSE: http://localhost:8000/stream/alerts
- HDFS UI: http://localhost:9870
- Spark UI: http://localhost:8081
- Frontend can consume these APIs from a separate Next.js/Vercel repo

## Blacklist Rule

The streaming pipeline combines:
- ML fraud prediction from the trained PaySim model
- A rule-based alert for `TRANSFER` transactions into destination
  accounts listed in `data/blacklist_accounts.txt`

You can customize the blacklist with:
```bash
# One account per line
data/blacklist_accounts.txt

# or override via env when running spark-submit
BLACKLIST_ACCOUNTS=C553264065,C38997010
BLACKLIST_TRANSFER_TYPES=TRANSFER
```

## Dataset Strategy

- `PaySim` is the primary dataset for the real-time pipeline:
  producer -> Kafka -> Spark Structured Streaming -> Redis/HDFS
- `creditcard.csv` is a secondary reference dataset for offline
  benchmarking and model comparison only
- The live streaming demo, blacklist rule, and alert APIs are all built
  around the PaySim transaction schema

##  Cấu trúc project

```
fraud-detection-pipeline/
├── docker-compose.yml          # 7 services: Kafka, HDFS, Spark, Redis, FastAPI
├── Dockerfile                  # Custom Spark image with ML libs
├── producer/
│   ├── kafka_producer.py       # Đọc PaySim CSV → Kafka topic
│   └── requirements.txt
├── spark/
│   ├── preprocessing.py        # Feature engineering (PySpark ONLY, NO Pandas)
│   ├── train_model.py          # RF vs GBT training + CrossValidator + MLflow
│   ├── streaming_pipeline.py   # Kafka → ML predict → HDFS + Redis
│   └── requirements.txt
├── serving/
│   ├── api.py                  # FastAPI: /kpis, /alerts, /stream (SSE)
│   ├── redis_listener.py       # Async Redis subscriber → SSE fan-out
│   ├── Dockerfile
│   └── requirements.txt
├── tests/
│   ├── conftest.py             # SparkSession fixtures
│   ├── test_preprocessing.py   # 14 unit tests cho feature engineering
│   └── test_api.py             # 8 integration tests cho API endpoints
├── scripts/
│   ├── start_pipeline.sh       # One-command startup
│   ├── start_pipeline.ps1      # Windows PowerShell startup
│   ├── stop_pipeline.sh        # Graceful shutdown
│   ├── stop_pipeline.ps1       # Windows PowerShell shutdown
│   └── download_dataset.py     # PaySim dataset downloader
├── data/                       # PaySim CSV (gitignored)
├── models/                     # Trained PipelineModel (gitignored)
└── .github/workflows/ci.yml    # GitHub Actions: lint + test + docker validate
```

##  ML Pipeline

### Features (5 engineered + 6 raw)
| Feature | Mô tả |
|---------|--------|
| `balance_diff_orig` | Chênh lệch số dư tài khoản nguồn |
| `balance_diff_dest` | Chênh lệch số dư tài khoản đích |
| `amount_ratio` | Tỷ lệ số tiền GD / số dư hiện có |
| `is_zero_balance_orig` | Tài khoản rút sạch tiền (pattern fraud) |
| `is_large_amount` | Giao dịch > 200,000 units |
| `type_index` | Loại GD encoded (StringIndexer) |

### Models so sánh
- **Random Forest** (numTrees=100, maxDepth=10) + CrossValidator 3-fold
- **Gradient Boosted Trees** (maxIter=50, maxDepth=8)
- Chọn model tốt hơn dựa trên AUC-ROC
- Giải quyết imbalanced data bằng `weightCol`

##  API Endpoints

| Method | Path | Mô tả |
|--------|------|--------|
| GET | `/health` | Health check |
| GET | `/kpis` | Tổng GD, tổng alert, breakdown ML/rule, fraud rate |
| GET | `/recent-alerts?limit=50` | 50 cảnh báo gần nhất |
| GET | `/stream/alerts` | SSE push fraud alerts real-time |
| GET | `/model-metrics` | Accuracy, AUC... của model |

##  Testing

```bash
# Unit tests (cần PySpark)
pytest tests/test_preprocessing.py -v

# API tests (không cần PySpark)
pip install -r serving/requirements.txt pytest httpx
pytest tests/test_api.py -v
```

##  Tech Stack

| Layer | Công nghệ |
|-------|-----------|
| Message Queue | Apache Kafka 3.7 (KRaft mode) |
| Stream Processing | Apache Spark 3.5.3 Structured Streaming |
| Storage | HDFS (Parquet) |
| ML | PySpark MLlib + MLflow |
| Serving | FastAPI + Redis Pub/Sub + SSE |
| Infrastructure | Docker Compose |
| CI/CD | GitHub Actions |

##  Nhóm thực hiện

| Thành viên | Vai trò |
|------------|---------|
|  | Infrastructure & Kafka Producer |
|  | ML Pipeline & Spark Streaming |
|  | Backend Serving (FastAPI + Redis) |
|  | Frontend Dashboard (Next.js) |
