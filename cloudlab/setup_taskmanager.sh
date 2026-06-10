#!/bin/bash
# One-time setup for Flink TaskManager node.
set -e

echo "=== [1/4] Flink env: force IPv4 to fix gRPC localhost resolution ==="
grep -q "preferIPv4Stack" ~/flink/conf/flink-env.sh || \
    echo 'export JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"' >> ~/flink/conf/flink-env.sh

echo "=== [2/4] Flink memory config: fix off-heap OOM and budget overflow ==="
# Increase process size to accommodate extra off-heap
grep -q "taskmanager.memory.task.off-heap.size" ~/flink/conf/config.yaml || \
    echo "taskmanager.memory.task.off-heap.size: 512m" >> ~/flink/conf/config.yaml
grep -q "taskmanager.memory.framework.off-heap.size" ~/flink/conf/config.yaml || \
    echo "taskmanager.memory.framework.off-heap.size: 256m" >> ~/flink/conf/config.yaml
grep -q "taskmanager.memory.managed.size" ~/flink/conf/config.yaml || \
    echo "taskmanager.memory.managed.size: 512m" >> ~/flink/conf/config.yaml
# Increase process size (must be done in nested YAML section)
sed -i '/taskmanager\.memory/,/process/{s/size: 1728m/size: 2560m/}' ~/flink/conf/config.yaml || true

echo "=== [3/4] Python packages ==="
source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream
pip install -q apache-flink==2.0.0 tensorflow==2.14.0 \
    "h5py==3.1.0" "nfp==0.1.3" rdkit "pandas<2" "numpy<2"

echo "=== [4/4] Create search_space directory ==="
mkdir -p ~/MoStream/MoStream/MDStream/StreamML/search_space

echo ""
echo "=== TaskManager setup complete ==="
echo "NOTE: Copy MOS-search-simple.txt to:"
echo "  ~/MoStream/MoStream/MDStream/StreamML/search_space/"
