#!/bin/bash
# ============================================================
# stop_pipeline.sh — Graceful shutdown
# ============================================================
set -e

echo "🛑 Stopping Fraud Detection Pipeline..."

echo "  [1/2] Stopping containers..."
docker compose down

echo "  [2/2] Cleaning up..."
# Optional: remove volumes for fresh restart
# docker compose down -v

echo "✅ Pipeline stopped."
echo ""
echo "To also remove data volumes:"
echo "  docker compose down -v"
