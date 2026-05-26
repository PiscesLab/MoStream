#!/bin/bash
# apt051 — Kafka node setup (finish remaining steps)
# Run this on apt051.apt.emulab.net
set -e

REPO_DIR="$HOME/MoStream"

echo "=== apt051: Verifying Kafka ==="
cd "$REPO_DIR"
docker compose ps

echo "=== Creating Kafka topics ==="
docker compose exec -T kafka kafka-topics \
  --create --if-not-exists --topic Simulation \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

docker compose exec -T kafka kafka-topics \
  --create --if-not-exists --topic Result \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

echo "=== Topic list ==="
docker compose exec -T kafka kafka-topics \
  --list --bootstrap-server localhost:9092

echo "=== apt051 DONE: Kafka is ready ==="
