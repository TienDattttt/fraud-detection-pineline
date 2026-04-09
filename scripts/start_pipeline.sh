#!/bin/bash
# ============================================================
# start_pipeline.sh — One-command startup for the full pipeline
# ============================================================
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=================================${NC}"
echo -e "${GREEN} Fraud Detection Pipeline Startup${NC}"
echo -e "${GREEN}=================================${NC}"

# ------------------------------------------------------------------
# Step 1: Build and start Docker containers
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[1/5] Starting Docker containers...${NC}"
docker compose up --build -d

# ------------------------------------------------------------------
# Step 2: Wait for services to be healthy
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[2/5] Waiting for services...${NC}"

echo -n "  Kafka..."
until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo -n "."
    sleep 3
done
echo -e " ${GREEN}OK${NC}"

echo -n "  HDFS..."
until docker exec namenode hdfs dfs -ls / > /dev/null 2>&1; do
    echo -n "."
    sleep 3
done
echo -e " ${GREEN}OK${NC}"

echo -n "  Redis..."
until docker exec redis redis-cli ping > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}OK${NC}"

echo -n "  Spark..."
until curl -s http://localhost:8081/ > /dev/null 2>&1; do
    echo -n "."
    sleep 3
done
echo -e " ${GREEN}OK${NC}"

# ------------------------------------------------------------------
# Step 3: Setup HDFS directories
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[3/5] Setting up HDFS directories...${NC}"
docker exec namenode hdfs dfs -mkdir -p /datalake/transactions
docker exec namenode hdfs dfs -mkdir -p /datalake/checkpoints/streaming
docker exec namenode hdfs dfs -chmod -R 777 /datalake
echo -e "  ${GREEN}HDFS directories created${NC}"

# ------------------------------------------------------------------
# Step 4: Create Kafka topic
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[4/5] Creating Kafka topic...${NC}"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic transactions \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || true
echo -e "  ${GREEN}Topic 'transactions' ready${NC}"

# ------------------------------------------------------------------
# Step 5: Check data file
# ------------------------------------------------------------------
echo -e "\n${YELLOW}[5/5] Checking dataset...${NC}"
if [ -f "./data/paysim_transactions.csv" ]; then
    ROW_COUNT=$(wc -l < ./data/paysim_transactions.csv)
    echo -e "  ${GREEN}Dataset found: ${ROW_COUNT} rows${NC}"
else
    echo -e "  ${RED}Dataset not found!${NC}"
    echo "  Run: python scripts/download_dataset.py"
fi

# ------------------------------------------------------------------
# Done
# ------------------------------------------------------------------
echo -e "\n${GREEN}=================================${NC}"
echo -e "${GREEN} All services running!${NC}"
echo -e "${GREEN}=================================${NC}"
echo ""
echo "  Kafka:       localhost:9092"
echo "  HDFS UI:     http://localhost:9870"
echo "  Spark UI:    http://localhost:8081"
echo "  API:         http://localhost:8000"
echo "  API Docs:    http://localhost:8000/docs"
echo ""
echo "Next steps:"
echo "  1. Train model (if not done):"
echo "     docker exec spark-master spark-submit /opt/spark/work/spark/train_model.py"
echo ""
echo "  2. Start streaming:"
echo "     docker exec spark-master spark-submit \\"
echo "       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\"
echo "       /opt/spark/work/spark/streaming_pipeline.py"
echo ""
echo "  3. Start producer:"
echo "     docker exec spark-master python3 /opt/spark/work/producer/kafka_producer.py --speed 0.5"
