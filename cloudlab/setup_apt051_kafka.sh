#!/bin/bash
# apt051 — Kafka node setup
# Run this on apt051.apt.emulab.net
set -e

REPO_DIR="$HOME/MoStream"

# Auto-detect internal IP (10.10.x.x interface)
KAFKA_IP=$(ip route | grep "10\.10\." | awk '{print $9}' | head -1)
if [ -z "$KAFKA_IP" ]; then
  KAFKA_IP=$(hostname -I | tr ' ' '\n' | grep "^10\." | head -1)
fi
echo "=== apt051: Using KAFKA_IP=$KAFKA_IP ==="

# Fix /etc/hosts so Kafka container can resolve the hostname
FULL_HOSTNAME=$(hostname)
if ! grep -q "$FULL_HOSTNAME" /etc/hosts; then
  sudo sh -c "echo '$KAFKA_IP $FULL_HOSTNAME' >> /etc/hosts"
fi

# Start Kafka — KAFKA_HOST must be the internal IP so other nodes can reach it
cd "$REPO_DIR"
KAFKA_HOST=$KAFKA_IP docker compose down 2>/dev/null || true
KAFKA_HOST=$KAFKA_IP docker compose up -d

echo "=== Waiting for Kafka to be ready... ==="
for i in $(seq 1 12); do
  if docker compose exec -T kafka kafka-broker-api-versions \
      --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "=== Kafka is up ==="
    break
  fi
  echo "Not ready yet ($i/12)..." && sleep 5
done

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

echo "=== apt051 DONE: Kafka advertised on $KAFKA_IP:9092 ==="
