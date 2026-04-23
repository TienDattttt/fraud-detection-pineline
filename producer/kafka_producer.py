"""
Kafka Producer for PaySim e-wallet transaction data.

Reads PaySim CSV and streams each row as JSON to Kafka topic 'transactions'.
Simulates real-time delay between messages for demo purposes.
"""
import time
import json
import random
import csv
import os
import argparse
import sys

from kafka import KafkaProducer

#Topic Kafka transactions
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "transactions")
CSV_FILE_PATH = os.getenv(
    "CSV_PATH", "/opt/spark/work/data/paysim_transactions.csv"
)

# PaySim numeric fields to cast from CSV strings
FLOAT_FIELDS = [
    "amount", "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest",
]
INT_FIELDS = ["step", "isFraud", "isFlaggedFraud"]

#Hàm parse JSON giao dịch
def parse_row(row: dict) -> dict:
    """Cast PaySim CSV string values to proper numeric types."""
    parsed = {}
    for key, value in row.items():
        clean_key = key.strip()
        if clean_key in FLOAT_FIELDS:
            parsed[clean_key] = float(value)
        elif clean_key in INT_FIELDS:
            parsed[clean_key] = int(value)
        else:
            parsed[clean_key] = value.strip()
    return parsed


def create_producer(retries: int = 5) -> KafkaProducer:
    """Create Kafka producer with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                acks="all",
                retries=3,
                batch_size=16384,
                linger_ms=10,
            )
            print(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except Exception as e:
            print(f"❌ Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(5)
    print("💀 Could not connect to Kafka. Exiting.")
    sys.exit(1)

#Producer đọc CSV và gửi Kafka
def run_producer(speed: float = 1.0, limit: int = 0):
    """
    Stream PaySim CSV rows to Kafka.

    Args:
        speed: Multiplier for delay. 1.0 = default (0.1-1.5s).
               0.0 = no delay (max speed). 2.0 = slower.
        limit: Max rows to send. 0 = all rows.
    """
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ File not found: {CSV_FILE_PATH}")
        print("   Run: python scripts/download_dataset.py")
        sys.exit(1)

    producer = create_producer()
    fraud_count = 0
    total_count = 0

    try:
        with open(CSV_FILE_PATH, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                parsed = parse_row(row)
                producer.send(TOPIC_NAME, value=parsed)

                total_count += 1
                if parsed.get("isFraud") == 1:
                    fraud_count += 1

                if total_count % 50 == 0:
                    producer.flush()

                if total_count % 500 == 0:
                    fraud_pct = (
                        (fraud_count / total_count * 100)
                        if total_count > 0 else 0
                    )
                    print(
                        f"[{total_count:,}] Sent | "
                        f"Fraud: {fraud_count:,} ({fraud_pct:.2f}%) | "
                        f"Last: {parsed.get('type')} "
                        f"${parsed.get('amount', 0):,.2f}"
                    )

                if limit > 0 and total_count >= limit:
                    break

                if speed > 0:
                    time.sleep(random.uniform(0.1, 1.5) * speed)

        producer.flush()
        print(f"\n{'='*60}")
        print(f"✅ Done! Sent {total_count:,} transactions")
        print(f"   Fraud: {fraud_count:,} / {total_count:,}")
        print(f"{'='*60}")

    except KeyboardInterrupt:
        print(f"\n⏹ Stopped. Sent {total_count:,} rows.")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PaySim Kafka Producer"
    )
    parser.add_argument(
        "--speed", type=float, default=1.0,
        help="Delay multiplier. 0=instant, 1=normal, 2=slow"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max rows to send. 0=all"
    )
    args = parser.parse_args()
    run_producer(speed=args.speed, limit=args.limit)
