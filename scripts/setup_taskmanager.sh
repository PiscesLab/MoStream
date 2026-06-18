#!/bin/bash
# Run ON the TaskManager node (apt030).
# Usage: bash setup_taskmanager.sh <jm_internal_ip>
# Example: bash setup_taskmanager.sh 10.10.1.2
set -e

FLINK_VERSION="2.0.0"
FLINK_HOME="$HOME/flink"
MINICONDA_PATH="$HOME/miniconda3"
JM_IP=${1:?"Usage: bash setup_taskmanager.sh <jm_internal_ip>"}

echo "[TM] JM_IP=$JM_IP"

# --- Java ---
if ! command -v java >/dev/null 2>&1; then
    echo "[TM] Installing Java..."
    sudo apt-get update -y
    sudo apt-get install -y openjdk-17-jre-headless
fi

# --- Flink ---
if [ ! -d "$FLINK_HOME" ]; then
    echo "[TM] Downloading Flink $FLINK_VERSION..."
    curl -fsSL "https://archive.apache.org/dist/flink/flink-$FLINK_VERSION/flink-$FLINK_VERSION-bin-scala_2.12.tgz" \
        -o /tmp/flink.tgz
    mkdir -p "$FLINK_HOME"
    tar -xzf /tmp/flink.tgz -C "$FLINK_HOME" --strip-components=1
fi

# --- Flink config ---
CONFIG="$FLINK_HOME/conf/config.yaml"
FLINK_ENV="$FLINK_HOME/conf/flink-env.sh"

# Point TM at the JobManager
grep -q "^jobmanager.rpc.address:" "$CONFIG" \
    && sed -i "s|^jobmanager.rpc.address:.*|jobmanager.rpc.address: $JM_IP|" "$CONFIG" \
    || echo "jobmanager.rpc.address: $JM_IP" >> "$CONFIG"

# Memory settings
grep -q "taskmanager.memory.task.off-heap.size" "$CONFIG" || \
    echo "taskmanager.memory.task.off-heap.size: 512m" >> "$CONFIG"
grep -q "taskmanager.memory.framework.off-heap.size" "$CONFIG" || \
    echo "taskmanager.memory.framework.off-heap.size: 256m" >> "$CONFIG"
grep -q "taskmanager.memory.managed.size" "$CONFIG" || \
    echo "taskmanager.memory.managed.size: 512m" >> "$CONFIG"
grep -q "taskmanager.memory.network.min" "$CONFIG" || \
    echo "taskmanager.memory.network.min: 512m" >> "$CONFIG"
grep -q "taskmanager.memory.network.max" "$CONFIG" || \
    echo "taskmanager.memory.network.max: 512m" >> "$CONFIG"
grep -q "taskmanager.memory.flink.size" "$CONFIG" || \
    echo "taskmanager.memory.flink.size: 2560m" >> "$CONFIG"
grep -q "taskmanager.memory.process.size" "$CONFIG" || \
    echo "taskmanager.memory.process.size: 3200m" >> "$CONFIG"

# CRITICAL: prevent Python 3.10 ~/.local site-packages from polluting the
# Beam worker's sys.path (root cause of the Python ABI segfault on old cluster).
grep -q "PYTHONNOUSERSITE" "$FLINK_ENV" || \
    echo 'export PYTHONNOUSERSITE=1' >> "$FLINK_ENV"
# Force IPv4 to fix gRPC localhost resolution
grep -q "preferIPv4Stack" "$FLINK_ENV" || \
    echo 'export JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"' >> "$FLINK_ENV"

# --- Miniconda ---
if [ ! -f "$MINICONDA_PATH/bin/conda" ]; then
    echo "[TM] Installing Miniconda..."
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
    echo "[TM] Creating mostream env..."
    conda create -y -n mostream python=3.9
fi
conda activate mostream

# --- Python packages (ONLY in conda env — never pip install --user) ---
echo "[TM] Installing Python packages..."
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

# --- Sanity check: pyflink must NOT come from ~/.local ---
PYFLINK_FILE=$(python3.9 -c "import pyflink.datastream.data_stream as ds; print(ds.__file__)")
if echo "$PYFLINK_FILE" | grep -q ".local/lib/python3.10"; then
    echo "[TM] ERROR: pyflink resolving to Python 3.10 user-site: $PYFLINK_FILE"
    echo "[TM] Run: rm -rf ~/.local/lib/python3.10/site-packages/pyflink"
    exit 1
fi
echo "[TM] pyflink OK: $PYFLINK_FILE"

# Symlink so Flink's TM Java process can find 'python' with pyflink
sudo ln -sf "$MINICONDA_PATH/envs/mostream/bin/python" /usr/local/bin/python

# --- Data directories ---
sudo mkdir -p /mnt/media/MDStream/StreamML/networks
sudo chmod 777 /mnt/media/MDStream/StreamML/networks
mkdir -p "$HOME/MoStream/MoStream/MDStream/StreamML/search_space"

echo ""
echo "=== TaskManager setup complete ==="
echo ""
echo "To start the TaskManager:"
echo "  $FLINK_HOME/bin/taskmanager.sh start"
echo ""
echo "NOTE: Copy model.h5 to /mnt/media/MDStream/StreamML/networks/ before running the job."
