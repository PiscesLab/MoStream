#!/bin/bash
# One-time setup for Flink TaskManager node.
set -e

echo "=== [1/4] Flink env: force IPv4 to fix gRPC localhost resolution ==="
grep -q "preferIPv4Stack" ~/flink/conf/flink-env.sh || \
    echo 'export JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"' >> ~/flink/conf/flink-env.sh

echo "=== [2/4] Flink config: host binding + memory ==="
# Bind TM services (including metrics actor) to the experiment network IP, not loopback.
# Without this the JM can't reach the metrics actor and web UI shows "loading...".
TM_IP=$(hostname -I | tr ' ' '\n' | grep '^10\.10\.1\.' | head -1)
if [ -n "$TM_IP" ]; then
    grep -q "taskmanager.host" ~/flink/conf/config.yaml || \
        echo "taskmanager.host: $TM_IP" >> ~/flink/conf/config.yaml
fi
# bind-host: 0.0.0.0 ensures all TM services (including metrics actor) listen on all interfaces,
# not just loopback. Required for JM to collect metrics from TM across the network.
grep -q "taskmanager.bind-host" ~/flink/conf/config.yaml || \
    echo "taskmanager.bind-host: 0.0.0.0" >> ~/flink/conf/config.yaml
# Fix metrics query service port so socat relay always knows which port to forward.
grep -q "metrics.internal.query-service.port" ~/flink/conf/config.yaml || \
    echo "metrics.internal.query-service.port: 9998" >> ~/flink/conf/config.yaml
# Increase process size to accommodate extra off-heap
grep -q "taskmanager.memory.task.off-heap.size" ~/flink/conf/config.yaml || \
    echo "taskmanager.memory.task.off-heap.size: 4096m" >> ~/flink/conf/config.yaml
grep -q "taskmanager.memory.framework.off-heap.size" ~/flink/conf/config.yaml || \
    echo "taskmanager.memory.framework.off-heap.size: 256m" >> ~/flink/conf/config.yaml
grep -q "taskmanager.memory.managed.size" ~/flink/conf/config.yaml || \
    echo "taskmanager.memory.managed.size: 512m" >> ~/flink/conf/config.yaml
grep -q "taskmanager.memory.task.heap.size" ~/flink/conf/config.yaml || \
    echo "taskmanager.memory.task.heap.size: 3072m" >> ~/flink/conf/config.yaml
grep -q "taskmanager.memory.process.size" ~/flink/conf/config.yaml || \
    echo "taskmanager.memory.process.size: 10112m" >> ~/flink/conf/config.yaml
# flink.size: task.heap(3072) + task.off-heap(4096) + managed(512) + network(512) + framework.off-heap(256) + framework.heap(128) = 8576m
sed -i '/^taskmanager\.memory\.flink\.size:/d' ~/flink/conf/config.yaml
echo "taskmanager.memory.flink.size: 8576m" >> ~/flink/conf/config.yaml

echo "=== [3/4] Python packages ==="
source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream
pip install -q apache-flink==2.0.0 tensorflow==2.14.0 \
    "h5py==3.1.0" "nfp==0.1.3" rdkit "pandas<2" "numpy<2"

echo "=== [4/4] Create search_space directory ==="
mkdir -p ~/MoStream/MoStream/MDStream/StreamML/search_space

echo "=== [5/5] Metrics relay (socat): forward 10.10.1.2:9998 → 127.0.0.1:9998 ==="
# Flink 2.0 MetricQueryService binds to 127.0.0.1 but advertises the external IP.
# socat bridges the gap so the JM can reach the metrics actor.
which socat > /dev/null 2>&1 || sudo apt-get install -y socat
pkill -f "socat.*9998" 2>/dev/null || true
TM_IP=$(hostname -I | tr ' ' '\n' | grep '^10\.10\.1\.' | head -1)
nohup socat TCP-LISTEN:9998,bind=${TM_IP},reuseaddr,fork TCP:127.0.0.1:9998 > /tmp/socat-metrics.log 2>&1 &
echo "socat relay started (PID $!): ${TM_IP}:9998 → 127.0.0.1:9998"

echo ""
echo "=== TaskManager setup complete ==="
echo "NOTE: Copy MOS-search-simple.txt to:"
echo "  ~/MoStream/MoStream/MDStream/StreamML/search_space/"
