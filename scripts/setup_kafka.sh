#!/bin/bash
# Run ON the Kafka node — native Kafka, KRaft mode (no Zookeeper, no Docker)
# Usage: bash setup_kafka.sh <internal_ip>
# Example: bash setup_kafka.sh 10.10.1.3
set -e

KAFKA_VERSION="3.6.1"
SCALA_VERSION="2.13"
KAFKA_DIR="$HOME/kafka"
KAFKA_IP=${1:-$(hostname -I | awk '{print $1}')}
echo "[kafka] Using KAFKA_IP=$KAFKA_IP"

# --- Java ---
if ! command -v java >/dev/null 2>&1; then
    echo "[kafka] Installing Java..."
    sudo apt-get update -y
    sudo apt-get install -y openjdk-17-jre-headless
fi

# --- Download and extract Kafka ---
if [ ! -d "$KAFKA_DIR" ]; then
    echo "[kafka] Downloading Kafka $KAFKA_VERSION..."
    curl -fsSL "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz" -o /tmp/kafka.tgz
    mkdir -p "$KAFKA_DIR"
    tar -xzf /tmp/kafka.tgz -C "$KAFKA_DIR" --strip-components=1
fi

# --- Configure KRaft (single-node, combined broker+controller) ---
CONFIG_FILE="$KAFKA_DIR/config/kraft/server.properties"
CLUSTER_ID_FILE="$HOME/.kafka_cluster_id"

sed -i \
    -e "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://$KAFKA_IP:9092|" \
    -e "s|^log.dirs=.*|log.dirs=$KAFKA_DIR/data|" \
    "$CONFIG_FILE"

# --- Format storage (only on first run) ---
if [ ! -f "$CLUSTER_ID_FILE" ]; then
    echo "[kafka] Formatting storage (first run)..."
    CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
    echo "$CLUSTER_ID" > "$CLUSTER_ID_FILE"
    "$KAFKA_DIR/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONFIG_FILE"
fi

# --- (Re)start Kafka in the background ---
pkill -f "kafka.Kafka" 2>/dev/null || true
sleep 2
nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$CONFIG_FILE" > "$HOME/kafka.log" 2>&1 &
disown

echo "[kafka] Waiting for broker to be ready..."
for i in $(seq 1 12); do
    if "$KAFKA_DIR/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        echo "[kafka] Kafka is up."
        break
    fi
    echo "[kafka] Not ready yet ($i/12)..."
    sleep 5
done

# --- Create topics (partitions must equal Flink job parallelism) ---
PARTITIONS=${PARTITIONS:-8}
for TOPIC in Simulation Recommend; do
    "$KAFKA_DIR/bin/kafka-topics.sh" --create --if-not-exists \
        --topic "$TOPIC" \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 --partitions "$PARTITIONS"
    echo "[kafka] Topic '$TOPIC' ready ($PARTITIONS partitions)."
done

echo "[kafka] Setup complete. Topics:"
"$KAFKA_DIR/bin/kafka-topics.sh" --list --bootstrap-server localhost:9092
