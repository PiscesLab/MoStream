#!/bin/bash
# Run this on apt041 AFTER both JobManager and TaskManager are running
# to submit the PyFlink MDWorkflow job to the Flink cluster.
set -e

KAFKA_HOST="apt051.apt.emulab.net"
JOBMANAGER_HOST="apt041.apt.emulab.net"
CONDA_ENV="mostream"

source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

cd "$HOME/MoStream/MoStream/MDStream/StreamML"

# Run via python3 directly (not flink run -py) to avoid PyFlink 2.0 bug where
# add_jars() serializes JAR paths as ['file:/...'] causing MalformedURLException.
# --jm-host triggers create_remote_execution_environment() which passes JARs
# as individual strings, bypassing the broken list serialization path.
echo "=== Submitting PyFlink job to cluster ==="
python3 MDWorkflow.py \
  --kafka-bootstrap "$KAFKA_HOST:9092" \
  --jm-host "$JOBMANAGER_HOST" \
  --jm-port 6123
