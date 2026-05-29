#!/bin/bash
# apt063 — Flink TaskManager node (runs Train/Infer/Rank operators)
# Run this on apt063.apt.emulab.net
set -e

JOBMANAGER_HOST="apt041.apt.emulab.net"
FLINK_VERSION="2.0.0"
CONDA_ENV="mostream"
REPO_URL="https://github.com/PiscesLab/MoStream.git"   # update if using a fork

# ── Conda init helper ────────────────────────────────────────────────────────
source_conda() {
  if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
    source "$HOME/miniconda3/etc/profile.d/conda.sh"
  else
    echo "ERROR: conda not found. Install Miniconda first." && exit 1
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
echo "=== Accepting conda ToS ==="
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main || true
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r   || true

# ── 3. Conda env ─────────────────────────────────────────────────────────────
if ! conda env list | grep -q "^$CONDA_ENV "; then
  echo "=== Creating conda env '$CONDA_ENV' ==="
  conda create -n "$CONDA_ENV" python=3.9 -y
fi
conda activate "$CONDA_ENV"

# ── 4. Python packages ───────────────────────────────────────────────────────
echo "=== Installing Python packages ==="
# h5py==3.1.0: newer versions cannot read model.h5 (non-standard HDF5 float type)
# nfp==0.1.3:  needs GlobalUpdate + ConcatDense (present when model.h5 was saved)
# pandas<2:    apache_beam (PyFlink internals) fails with pandas>=2 on Python 3.9
pip install --quiet apache-flink==2.0.0 kafka-python tensorflow==2.14.0 \
  "nfp==0.1.3" "h5py==3.1.0" rdkit "pandas<2"
conda install -n "$CONDA_ENV" -y "numpy<2"
conda install -n "$CONDA_ENV" -y libstdcxx-ng

# ── 5. Fix libstdc++ for TF ──────────────────────────────────────────────────
echo "=== Fixing libstdc++ ==="
mkdir -p "$CONDA_PREFIX/etc/conda/activate.d"
cat > "$CONDA_PREFIX/etc/conda/activate.d/libstdcxx.sh" <<'EOF'
export LD_LIBRARY_PATH=$CONDA_PREFIX/lib:$LD_LIBRARY_PATH
EOF
conda deactivate && conda activate "$CONDA_ENV"

# ── 6. Java 11 ───────────────────────────────────────────────────────────────
if ! java -version 2>/dev/null | grep -q "11"; then
  echo "=== Installing Java 11 ==="
  sudo apt-get update -y && sudo apt-get install -y openjdk-11-jdk
fi

# ── 7. Flink ─────────────────────────────────────────────────────────────────
if [ ! -d "$HOME/flink" ]; then
  echo "=== Downloading Flink $FLINK_VERSION ==="
  cd ~
  wget -q "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz" \
    -O /tmp/flink.tgz
  tar -xzf /tmp/flink.tgz
  mv "flink-${FLINK_VERSION}" flink
fi

# ── 8. Flink config ──────────────────────────────────────────────────────────
echo "=== Configuring Flink ==="
# Flink 2.0 uses config.yaml; older versions use flink-conf.yaml
if [ -f "$HOME/flink/conf/config.yaml" ]; then
  FLINK_CONF="$HOME/flink/conf/config.yaml"
elif [ -f "$HOME/flink/conf/flink-conf.yaml" ]; then
  FLINK_CONF="$HOME/flink/conf/flink-conf.yaml"
else
  echo "ERROR: No Flink config file found in ~/flink/conf/" && ls ~/flink/conf/ && exit 1
fi
grep -q "jobmanager.rpc.address" "$FLINK_CONF" || \
  echo "jobmanager.rpc.address: $JOBMANAGER_HOST" >> "$FLINK_CONF"
grep -q "python.executable" "$FLINK_CONF" || \
  echo "python.executable: $HOME/miniconda3/envs/$CONDA_ENV/bin/python" >> "$FLINK_CONF"
# Network buffer settings — use a unique flat-key grep so nested YAML doesn't fool us
grep -q "taskmanager.memory.network.fraction" "$FLINK_CONF" || \
  cat >> "$FLINK_CONF" << 'NETCONF'
taskmanager.memory.network.fraction: 0.2
taskmanager.memory.network.min: 256m
taskmanager.memory.network.max: 1g
NETCONF

# ── 9. Clone repo ────────────────────────────────────────────────────────────
if [ ! -d "$HOME/MoStream" ]; then
  echo "=== Cloning repo ==="
  git clone "$REPO_URL" "$HOME/MoStream"
fi

# ── 10. Data directories + /mnt/media symlink ────────────────────────────────
echo "=== Setting up data paths ==="
mkdir -p "$HOME/MoStream/MoStream/MDStream/StreamML/networks"
mkdir -p "$HOME/MoStream/MoStream/MDStream/StreamML/search_space"
sudo mkdir -p /mnt/media/MDStream/StreamML/networks
sudo mkdir -p /mnt/media/MDStream/StreamML/search_space
sudo chown -R "$(id -un):$(id -gn)" /mnt/media

# Symlinks so NPMMModel.py finds model at /mnt/media/...
MODEL_SRC="$HOME/MoStream/MoStream/MDStream/StreamML/networks/model.h5"
MODEL_DST="/mnt/media/MDStream/StreamML/networks/model.h5"
[ -f "$MODEL_SRC" ] && [ ! -e "$MODEL_DST" ] && ln -s "$MODEL_SRC" "$MODEL_DST"

SPACE_SRC="$HOME/MoStream/MoStream/MDStream/StreamML/search_space/MOS-search-simple.txt"
SPACE_DST="/mnt/media/MDStream/StreamML/search_space/MOS-search-simple.txt"
[ -f "$SPACE_SRC" ] && [ ! -e "$SPACE_DST" ] && ln -s "$SPACE_SRC" "$SPACE_DST"

echo ""
echo "=== apt063 setup done ==="
echo "Next steps:"
echo "  1. Copy model.h5 and MOS-search-simple.txt into:"
echo "       $HOME/MoStream/MoStream/MDStream/StreamML/networks/"
echo "       $HOME/MoStream/MoStream/MDStream/StreamML/search_space/"
echo "  2. Start TaskManager:"
echo "       $HOME/flink/bin/taskmanager.sh start"
