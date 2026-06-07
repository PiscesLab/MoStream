#!/bin/bash
# One-time setup for Kafka broker node.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster.conf"

echo "=== [1/2] Configure Docker vfs storage driver ==="
sudo mkdir -p /opt/docker-data
sudo tee /etc/docker/daemon.json << 'EOF'
{
  "data-root": "/opt/docker-data",
  "storage-driver": "vfs"
}
EOF
sudo systemctl restart docker
sleep 3

echo "=== [2/2] Write .env with Kafka advertised IP ==="
echo "KAFKA_HOST=$KAFKA_HOST" > ~/MoStream/.env

echo ""
echo "=== Kafka setup complete (KAFKA_HOST: $KAFKA_HOST) ==="
