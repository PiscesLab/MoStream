#!/bin/bash
# apt041 — Flink JobManager node (starts cluster, submits PyFlink job)
# Run this on apt041.apt.emulab.net
set -e

KAFKA_HOST="apt051.apt.emulab.net"
JOBMANAGER_HOST="apt041.apt.emulab.net"
FLINK_VERSION="2.0.0"
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

# ── 4. Python packages (full stack needed — job graph building imports all UDF modules) ──
echo "=== Installing Python packages ==="
pip install --quiet apache-flink==2.0.0 kafka-python tensorflow==2.14.0 nfp h5py rdkit "pandas<2"
conda install -n "$CONDA_ENV" -y "numpy<2"
conda install -n "$CONDA_ENV" -y libstdcxx-ng

# Fix libstdc++ for TF
mkdir -p "$CONDA_PREFIX/etc/conda/activate.d"
cat > "$CONDA_PREFIX/etc/conda/activate.d/libstdcxx.sh" <<'EOF'
export LD_LIBRARY_PATH=$CONDA_PREFIX/lib:$LD_LIBRARY_PATH
EOF
conda deactivate && conda activate "$CONDA_ENV"

# ── 5. Java 11 ───────────────────────────────────────────────────────────────
if ! java -version 2>/dev/null | grep -q "11"; then
  echo "=== Installing Java 11 ==="
  sudo apt-get update -y && sudo apt-get install -y openjdk-11-jdk
fi

# ── 6. Flink ─────────────────────────────────────────────────────────────────
if [ ! -d "$HOME/flink" ]; then
  echo "=== Downloading Flink $FLINK_VERSION ==="
  wget -q "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz" \
    -O /tmp/flink.tgz
  tar -xzf /tmp/flink.tgz -C ~
  mv "$HOME/flink-${FLINK_VERSION}" "$HOME/flink"
fi

# ── 7. Flink config ──────────────────────────────────────────────────────────
echo "=== Configuring Flink JobManager ==="
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
# Network buffer settings
grep -q "taskmanager.memory.network.fraction" "$FLINK_CONF" || \
  cat >> "$FLINK_CONF" << 'NETCONF'
taskmanager.memory.network.fraction: 0.2
taskmanager.memory.network.min: 256m
taskmanager.memory.network.max: 1g
NETCONF

# ── 8. Clone repo ────────────────────────────────────────────────────────────
if [ ! -d "$HOME/MoStream" ]; then
  echo "=== Cloning repo ==="
  git clone "$REPO_URL" "$HOME/MoStream"
fi

# ── 9. Start JobManager ──────────────────────────────────────────────────────
echo "=== Starting Flink JobManager ==="
"$HOME/flink/bin/jobmanager.sh" start

echo ""
echo "=== Waiting 10s for JobManager to be ready ==="
sleep 10

echo "=== Flink web UI available at: http://$JOBMANAGER_HOST:8081 ==="
echo ""
echo "=== apt041 setup done ==="
echo ""
echo "To submit the PyFlink job (run AFTER TaskManager is started on apt063):"
echo "  conda activate $CONDA_ENV"
echo "  cd $HOME/MoStream/MoStream/MDStream/StreamML"
echo "  $HOME/flink/bin/flink run -py MDWorkflow.py -pyfs . \\"
echo "    --kafka-bootstrap $KAFKA_HOST:9092"
