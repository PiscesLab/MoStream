#!/bin/bash
# Run ON a Simulator node (apt032 / apt034 / apt035).
# No arguments needed — Kafka bootstrap is passed at runtime to simulator.py.
set -e

MINICONDA_PATH="$HOME/miniconda3"

echo "[simulator] Starting setup..."

# --- DNS fix ---
if ! nslookup pypi.org >/dev/null 2>&1; then
    echo "[simulator] Fixing DNS..."
    sudo bash -c 'echo "nameserver 8.8.8.8" > /etc/resolv.conf'
fi

# --- Miniconda ---
if [ ! -f "$MINICONDA_PATH/bin/conda" ]; then
    echo "[simulator] Installing Miniconda..."
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
    echo "[simulator] Creating mostream env..."
    conda create -y -n mostream python=3.9
fi
conda activate mostream

# --- Python packages ---
echo "[simulator] Installing Python packages..."
pip install --no-user \
    kafka-python \
    requests \
    rdkit \
    qcelemental \
    qcengine \
    ase \
    "numpy<2"

# --- gitinfo stub (missing dependency in repo) ---
GITINFO="$HOME/MoStream/MoStream/WLGenerator-node1/gitinfo.py"
if [ ! -f "$GITINFO" ]; then
    echo "[simulator] Creating gitinfo.py stub..."
    cat > "$GITINFO" << 'EOF'
def get_git_info():
    return {'commit': 'unknown'}
EOF
fi

# --- Data directory for training data ---
sudo mkdir -p /mnt/media/MDStream/WLGenerator/dataset
sudo chmod 777 /mnt/media/MDStream/WLGenerator/dataset

echo ""
echo "=== Simulator setup complete ==="
echo ""
echo "To run the simulator:"
echo "  source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream"
echo "  cd ~/MoStream/MoStream/WLGenerator-node1"
echo "  KAFKA_BOOTSTRAP=10.10.1.3:9092 python simulator.py --topic Simulation --interval 1.0"
echo ""
echo "NOTE: Copy training-data-simple.txt to /mnt/media/MDStream/WLGenerator/dataset/ before running."
