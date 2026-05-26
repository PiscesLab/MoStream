#!/bin/bash
# Run this on apt041 AFTER both JobManager and TaskManager are running
# to submit the PyFlink MDWorkflow job to the Flink cluster.
set -e

KAFKA_HOST="apt051.apt.emulab.net"
CONDA_ENV="mostream"

source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

cd "$HOME/MoStream/MoStream/MDStream/StreamML"

# Clean __pycache__ so they are not included in the distributed Python files,
# which causes FileAlreadyExistsException when multiple tasks extract concurrently.
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

echo "=== Submitting PyFlink job to cluster ==="
"$HOME/flink/bin/flink" run \
  -py MDWorkflow.py \
  -pyfs . \
  --kafka-bootstrap "$KAFKA_HOST:9092"
