#!/bin/bash
# apt055 — WLGenerator / Simulator node
# Run this on apt055.apt.emulab.net
set -e

KAFKA_HOST="apt051.apt.emulab.net"
CONDA_ENV="mostream"
REPO_URL="https://github.com/PiscesLab/MoStream.git"   # update if using a fork

# ── Conda init helper ────────────────────────────────────────────────────────
source_conda() {
  if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
    source "$HOME/miniconda3/etc/profile.d/conda.sh"
  else
    echo "ERROR: conda not found." && exit 1
  fi
}

# ── 1. Miniconda ─────────────────────────────────────────────────────────────
if [ ! -d "$HOME/miniconda3" ]; then
  echo "=== Installing Miniconda ==="
  wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
  bash /tmp/miniconda.sh -b
  "$HOME/miniconda3/bin/conda" init bash
fi
source_conda

# ── 2. Conda ToS ─────────────────────────────────────────────────────────────
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main || true
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r   || true

# ── 3. Conda env ─────────────────────────────────────────────────────────────
if ! conda env list | grep -q "^$CONDA_ENV "; then
  echo "=== Creating conda env ==="
  conda create -n "$CONDA_ENV" python=3.9 -y
fi
conda activate "$CONDA_ENV"

# ── 4. Python packages ───────────────────────────────────────────────────────
echo "=== Installing Python packages ==="
pip install --quiet kafka-python rdkit

# ── 5. Clone repo ────────────────────────────────────────────────────────────
if [ ! -d "$HOME/MoStream" ]; then
  echo "=== Cloning repo ==="
  git clone "$REPO_URL" "$HOME/MoStream"
fi

echo ""
echo "=== apt055 setup done ==="
echo ""
echo "To run the simulator:"
echo "  conda activate $CONDA_ENV"
echo "  cd $HOME/MoStream"
echo "  python3 MoStream/MDStream/WLGenerator/simulator.py \\"
echo "    --kafka-bootstrap $KAFKA_HOST:9092 \\"
echo "    --topic Simulation \\"
echo "    --interval 1.0 \\"
echo "    --model-id 1"
