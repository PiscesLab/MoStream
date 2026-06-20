#!/bin/bash
# Run this on apt041 AFTER both JobManager and TaskManager are running.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF="$SCRIPT_DIR/cluster.conf"
if [ ! -f "$CONF" ]; then
    echo "ERROR: $CONF not found. Copy cluster.conf.template to cluster.conf and fill in IPs."
    exit 1
fi
source "$CONF"

if [ -z "$KAFKA_HOST" ] || [ -z "$TM_HOST" ]; then
    echo "ERROR: KAFKA_HOST and TM_HOST must be set in cluster.conf"
    exit 1
fi

# Pre-flight: fix DNS (resolv.conf resets on reboot)
sudo tee /etc/resolv.conf <<< "nameserver 8.8.8.8" > /dev/null
grep -q "$TM_HOST" /etc/hosts || sudo bash -c "echo '$TM_HOST apt063.apt.emulab.net apt063' >> /etc/hosts"

source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate mostream

cd "$HOME/MoStream/MoStream/MDStream/StreamML"

find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
rm -rf tmp/

# Clear JM logs before each submission
for f in "$FLINK_HOME"/log/flink-*-standalonesession-*.log \
          "$FLINK_HOME"/log/flink-*-standalonesession-*.out; do
    [ -f "$f" ] && > "$f"
done

FLINK_HOME="${FLINK_HOME:-$HOME/flink}"
PARALLELISM="${PARALLELISM:-1}"
echo "=== Submitting PyFlink job (Flink: $FLINK_HOME, Kafka: $KAFKA_HOST, Parallelism: $PARALLELISM) ==="
PYTHON_EXEC="${PYTHON_EXEC:-$HOME/miniconda3/envs/mostream/bin/python3.9}"
"$FLINK_HOME/bin/flink" run \
  -p "$PARALLELISM" \
  -pyexec "$PYTHON_EXEC" \
  -py MDWorkflow.py \
  --kafka-bootstrap "$KAFKA_HOST:9092"
