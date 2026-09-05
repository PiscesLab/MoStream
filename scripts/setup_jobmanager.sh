#!/bin/bash
# Run ON the JobManager node (apt029).
# Usage: bash setup_jobmanager.sh <jm_internal_ip>
# Example: bash setup_jobmanager.sh 10.10.1.4
set -e

FLINK_VERSION="2.0.0"
FLINK_HOME="$HOME/flink"
MINICONDA_PATH="$HOME/miniconda3"
JM_IP=${1:?"Usage: bash setup_jobmanager.sh <jm_internal_ip>"}

echo "[JM] JM_IP=$JM_IP"

# --- DNS fix (systemd-resolved sometimes broken on CloudLab) ---
if ! nslookup pypi.org >/dev/null 2>&1; then
    echo "[JM] Fixing DNS..."
    sudo bash -c 'echo "nameserver 8.8.8.8" > /etc/resolv.conf'
fi

# --- Hostname fix ---
FULL_HOSTNAME=$(hostname)
if ! grep -q "$FULL_HOSTNAME" /etc/hosts; then
    echo "[JM] Adding $JM_IP $FULL_HOSTNAME to /etc/hosts"
    sudo sh -c "echo '$JM_IP $FULL_HOSTNAME' >> /etc/hosts"
fi

# --- Java ---
if ! command -v java >/dev/null 2>&1; then
    echo "[JM] Installing Java..."
    sudo apt-get update -y
    sudo apt-get install -y openjdk-17-jre-headless
fi

# --- Flink ---
if [ ! -d "$FLINK_HOME" ]; then
    echo "[JM] Downloading Flink $FLINK_VERSION..."
    curl -fsSL "https://archive.apache.org/dist/flink/flink-$FLINK_VERSION/flink-$FLINK_VERSION-bin-scala_2.12.tgz" \
        -o /tmp/flink.tgz
    mkdir -p "$FLINK_HOME"
    tar -xzf /tmp/flink.tgz -C "$FLINK_HOME" --strip-components=1
fi

# --- Flink config ---
CONFIG="$FLINK_HOME/conf/config.yaml"
FLINK_ENV="$FLINK_HOME/conf/flink-env.sh"

# JM address (its own IP)
grep -q "^jobmanager.rpc.address:" "$CONFIG" \
    && sed -i "s|^jobmanager.rpc.address:.*|jobmanager.rpc.address: $JM_IP|" "$CONFIG" \
    || echo "jobmanager.rpc.address: $JM_IP" >> "$CONFIG"

# SECURITY: bind REST/UI and RPC to the INTERNAL address only, NEVER 0.0.0.0. Flink's REST API is
# unauthenticated and allows job submission, i.e. arbitrary code execution; a 0.0.0.0 bind exposes
# that RCE surface to the public internet and led to a cluster compromise (rogue services / rev
# shell dropped within days). For remote UI access use an SSH tunnel, not an open port.
grep -q "^rest.bind-address:" "$CONFIG"    || echo "rest.bind-address: $JM_IP"    >> "$CONFIG"
grep -q "^jobmanager.bind-host:" "$CONFIG" || echo "jobmanager.bind-host: $JM_IP" >> "$CONFIG"
grep -q "^rest.address:" "$CONFIG"      || echo "rest.address: $JM_IP"       >> "$CONFIG"

# Python bundle settings (match what MDWorkflow.py sets via Configuration object)
grep -q "python.fn-execution.bundle.time" "$CONFIG" || \
    echo "python.fn-execution.bundle.time: 60000" >> "$CONFIG"
grep -q "python.fn-execution.bundle.size" "$CONFIG" || \
    echo "python.fn-execution.bundle.size: 1" >> "$CONFIG"

# Clear workers file — TM is started manually on its own node, not via start-cluster.sh
echo "" > "$FLINK_HOME/conf/workers"

# CRITICAL: prevent Python 3.10 ~/.local from polluting Beam worker sys.path
grep -q "PYTHONNOUSERSITE" "$FLINK_ENV" || \
    echo 'export PYTHONNOUSERSITE=1' >> "$FLINK_ENV"
# Tell flink run -py which Python to use for the client side
grep -q "PYFLINK_PYTHON" "$FLINK_ENV" || \
    echo 'export PYFLINK_PYTHON=/users/NamSDSU/miniconda3/envs/mostream/bin/python3.9' >> "$FLINK_ENV"
grep -q "preferIPv4Stack" "$FLINK_ENV" || \
    echo 'export JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"' >> "$FLINK_ENV"

# --- Miniconda ---
if [ ! -f "$MINICONDA_PATH/bin/conda" ]; then
    echo "[JM] Installing Miniconda..."
    curl -fsSL "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" \
        -o /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p "$MINICONDA_PATH"
fi
export PATH="$MINICONDA_PATH/bin:$PATH"
source "$MINICONDA_PATH/etc/profile.d/conda.sh"
grep -q "miniconda3/bin" ~/.bashrc || echo 'export PATH="$HOME/miniconda3/bin:$PATH"' >> ~/.bashrc

# --- Accept Anaconda TOS (required by recent Miniconda versions) ---
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r

# --- mostream conda env (Python 3.9) ---
if ! conda env list | grep -q "^mostream "; then
    echo "[JM] Creating mostream env..."
    conda create -y -n mostream python=3.9
fi
conda activate mostream

# --- Python packages (same as TM — job graph building imports all UDF modules) ---
echo "[JM] Installing Python packages..."
pip install --no-user \
    apache-flink==2.0.0 \
    tensorflow==2.14.0 \
    "h5py==3.1.0" \
    "nfp==0.1.3" \
    rdkit \
    "pandas<2" \
    "numpy<2" \
    kafka-python \
    networkx

# Symlink so Flink's JM Java process can find 'python' with pyflink
sudo ln -sf "$MINICONDA_PATH/envs/mostream/bin/python" /usr/local/bin/python

# --- Data directories ---
sudo mkdir -p /mnt/media/MDStream/StreamML/networks
sudo chmod 777 /mnt/media/MDStream/StreamML/networks
mkdir -p "$HOME/MoStream/MoStream/MDStream/StreamML/search_space"

echo ""
echo "=== JobManager setup complete ==="
echo ""
echo "To start the JobManager:"
echo "  $FLINK_HOME/bin/jobmanager.sh start"
echo ""
echo "Flink web UI (once JM is running):"
echo "  http://$JM_IP:8081"
echo ""
echo "NOTE: Copy model.h5 to /mnt/media/MDStream/StreamML/networks/ before submitting the job."
echo "NOTE: Copy MOS-search-simple.txt to ~/MoStream/MoStream/MDStream/StreamML/search_space/"
