#!/bin/bash
# One-time setup for Flink TaskManager node.
set -e

echo "=== [1/3] Flink env: force IPv4 to fix gRPC localhost resolution ==="
grep -q "preferIPv4Stack" ~/flink/conf/flink-env.sh || \
    echo 'export JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"' >> ~/flink/conf/flink-env.sh

echo "=== [2/3] Python packages ==="
source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream
pip install -q apache-flink==2.0.0 tensorflow==2.14.0 \
    "h5py==3.1.0" "nfp==0.1.3" rdkit "pandas<2" "numpy<2"

echo "=== [3/3] Create search_space directory ==="
mkdir -p ~/MoStream/MoStream/MDStream/StreamML/search_space

echo ""
echo "=== TaskManager setup complete ==="
echo "NOTE: Copy MOS-search-simple.txt to:"
echo "  ~/MoStream/MoStream/MDStream/StreamML/search_space/"
