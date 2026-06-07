#!/bin/bash
# Run this on apt041 AFTER both JobManager and TaskManager are running
# to submit the PyFlink MDWorkflow job to the Flink cluster.
set -e

KAFKA_HOST="128.110.96.51"
CONDA_ENV="mostream"

source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

cd "$HOME/MoStream/MoStream/MDStream/StreamML"

# Clean __pycache__ and tmp/ to avoid FileAlreadyExistsException when PyFlink
# distributes the directory to workers.
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
rm -rf tmp/

echo "=== Submitting PyFlink job to cluster ==="
"$HOME/flink/bin/flink" run \
  -py MDWorkflow.py \
  --kafka-bootstrap "$KAFKA_HOST:9092"
