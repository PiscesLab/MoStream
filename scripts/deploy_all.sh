#!/bin/bash
# Run LOCALLY — orchestrates full CloudLab deployment from scratch.
# Prerequisites:
#   - ~/.ssh/config has: kafka, jobmanager, taskmanager, simulator aliases
#   - Data files exist at the paths below
#   - MoStream repo is cloned on all nodes (git pull first if needed)
set -e

# ---- Data file paths (edit if your paths differ) ----
LOCAL_MODEL_H5="/mnt/n/Applications/MoStreamData/MoStream/MoStream/MDStream/StreamML/networks/model.h5"
LOCAL_TRAINING_DATA="/mnt/n/Applications/MoStreamData/MoStream/MoStream/WLGenerator-node1/dataset/training-data-simple.txt"

SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "========================================"
echo " MoStream CloudLab Full Deployment"
echo "========================================"

# --- Step 0: Get Kafka node internal IP ---
echo "[deploy] Detecting Kafka node internal IP..."
KAFKA_IP=$(ssh kafka "ip route | grep 10.10 | awk '{print \$9}'")
echo "[deploy] KAFKA_IP=$KAFKA_IP"

# --- Step 1: Setup Kafka node ---
echo ""
echo "[deploy] === Step 1: Setup Kafka (apt051) ==="
scp "$SCRIPTS_DIR/setup_kafka.sh" kafka:~/setup_kafka.sh
ssh kafka "bash ~/setup_kafka.sh $KAFKA_IP"

# --- Step 2: Setup JobManager node ---
echo ""
echo "[deploy] === Step 2: Setup JobManager (apt041) ==="
scp "$SCRIPTS_DIR/setup_jobmanager.sh" jobmanager:~/setup_jobmanager.sh
JM_IP=$(ssh jobmanager "ip route | grep 10.10 | awk '{print \$9}'")
ssh jobmanager "bash ~/setup_jobmanager.sh $JM_IP"

# Copy model.h5
echo "[deploy] Copying model.h5 to jobmanager..."
scp "$LOCAL_MODEL_H5" jobmanager:/mnt/media/MDStream/StreamML/networks/model.h5

# --- Step 3: Setup Simulator node ---
echo ""
echo "[deploy] === Step 3: Setup Simulator (apt055) ==="
scp "$SCRIPTS_DIR/setup_simulator.sh" simulator:~/setup_simulator.sh
ssh simulator "bash ~/setup_simulator.sh"

# Copy training data
echo "[deploy] Copying training data to simulator..."
scp "$LOCAL_TRAINING_DATA" simulator:/mnt/media/MDStream/WLGenerator/dataset/training-data-simple.txt

echo ""
echo "========================================"
echo " Deployment complete!"
echo "========================================"
echo ""
echo "Next steps — open 2 terminals:"
echo ""
echo "  Terminal 1 (JobManager):"
echo "    ssh jobmanager"
echo "    source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream"
echo "    cd ~/MoStream/MoStream/MDStream/StreamML"
echo "    python MDWorkflow.py --kafka-bootstrap ${KAFKA_IP}:9092 \\"
echo "      --starting-offset earliest --group-id run-1 2>&1 | tee output.log"
echo ""
echo "  Terminal 2 (Simulator):"
echo "    ssh simulator"
echo "    source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream"
echo "    cd ~/MoStream/MoStream/WLGenerator-node1"
echo "    KAFKA_BOOTSTRAP=${KAFKA_IP}:9092 python simulator.py \\"
echo "      --topic Simulation --interval 1.0"