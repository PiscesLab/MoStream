#!/bin/bash
# ============================================================================
# TaskManager setup -- THE ONLY TaskManager setup script. Installs AND configures.
#
# Usage:
#   bash setup_taskmanager.sh <jm_internal_ip> [--arm tuned|e2before]
#
#   --arm tuned     (default) The memory budget the paper advocates. Stable multi-day.
#   --arm e2before            The PRE-FIX budget. EXPECTED TO OOM after ~18h.
#                             Used only to produce the "before" half of experiment E2.
#
# HISTORY / WHY THIS FILE EXISTS
# There used to be two TaskManager scripts that could not be composed:
#   - scripts/setup_taskmanager.sh    : full installer, but STALE memory settings
#   - cloudlab/setup_taskmanager.sh   : config-only patcher with the CORRECT settings,
#                                       using `grep -q ... ||` guards
# Running the installer then the patcher silently produced a BROKEN hybrid: the guards
# skipped every key the installer had already written, so task.off-heap stayed at 512m
# and process.size at 3200m, while flink.size WAS forced to 8576m (that one used sed).
# Result: flink.size (8576m) > process.size (3200m) -> Flink config validation fails and
# the TaskManager never starts. Neither script worked alone either.
#
# This script replaces both. Memory keys are written AUTHORITATIVELY (delete-then-write),
# never guarded, so the arm you ask for is the arm you get.
# ============================================================================
set -e

FLINK_VERSION="2.0.0"
FLINK_HOME="$HOME/flink"
MINICONDA_PATH="$HOME/miniconda3"
SLOTS="${SLOTS:-32}"          # must be >= the job parallelism (experiments use up to 8)

JM_IP=${1:?"Usage: bash setup_taskmanager.sh <jm_internal_ip> [--arm tuned|e2before]"}
shift || true
ARM="tuned"
while [ $# -gt 0 ]; do
    case "$1" in
        --arm) ARM="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done
case "$ARM" in
    tuned|e2before) ;;
    *) echo "--arm must be 'tuned' or 'e2before' (got '$ARM')" >&2; exit 1 ;;
esac

echo "[TM] JM_IP=$JM_IP  ARM=$ARM  SLOTS=$SLOTS"
if [ "$ARM" = "e2before" ]; then
    echo ""
    echo "############################################################"
    echo "#  E2 'BEFORE' ARM -- this TaskManager is EXPECTED to OOM.  #"
    echo "#  Do not use it for E0/E1/E3-E6 runs.                      #"
    echo "############################################################"
    echo ""
fi

# --- DNS (systemd-resolved is sometimes broken on CloudLab) ---
if ! nslookup pypi.org >/dev/null 2>&1; then
    echo "[TM] Fixing DNS..."
    sudo bash -c 'echo "nameserver 8.8.8.8" > /etc/resolv.conf'
fi

# --- Java ---
if ! command -v java >/dev/null 2>&1; then
    echo "[TM] Installing Java..."
    sudo apt-get update -y -qq
    sudo apt-get install -y -qq openjdk-17-jre-headless
fi

# --- socat (metrics relay; see step 5) ---
command -v socat >/dev/null 2>&1 || sudo apt-get install -y -qq socat

# --- Flink ---
if [ ! -d "$FLINK_HOME" ]; then
    echo "[TM] Downloading Flink $FLINK_VERSION..."
    curl -fsSL "https://archive.apache.org/dist/flink/flink-$FLINK_VERSION/flink-$FLINK_VERSION-bin-scala_2.12.tgz" \
        -o /tmp/flink.tgz
    mkdir -p "$FLINK_HOME"
    tar -xzf /tmp/flink.tgz -C "$FLINK_HOME" --strip-components=1
fi

CONFIG="$FLINK_HOME/conf/config.yaml"
FLINK_ENV="$FLINK_HOME/conf/flink-env.sh"
TM_IP=$(hostname -I | tr ' ' '\n' | grep '^10\.10\.1\.' | head -1)
[ -n "$TM_IP" ] || { echo "[TM] ERROR: no 10.10.1.x address found" >&2; exit 1; }

# --- Flink config: authoritative rewrite of every key we own ---
# set_cfg <key> <value> -- delete any existing line for the key, then append ours.
set_cfg() {
    sed -i "\|^$1:|d" "$CONFIG"
    echo "$1: $2" >> "$CONFIG"
}

set_cfg "jobmanager.rpc.address"                "$JM_IP"
set_cfg "taskmanager.host"                      "$TM_IP"
set_cfg "taskmanager.bind-host"                 "0.0.0.0"
set_cfg "taskmanager.numberOfTaskSlots"         "$SLOTS"
# Flink 2.0's MetricQueryService binds 127.0.0.1 but advertises the external IP; pinning the
# port lets the socat relay bridge the gap (see step 5).
set_cfg "metrics.internal.query-service.port"   "9998"

# --- Memory: the whole point of the arm flag ---
# Wipe every taskmanager.memory.* key first so arms never contaminate each other.
sed -i '/^taskmanager\.memory\./d' "$CONFIG"
if [ "$ARM" = "tuned" ]; then
    # Budget derived in cloudlab/notes.md. Off-heap is sized for the two consumers Flink's
    # own budget cannot see: TensorFlow's native (JNI) tensor allocations, and the Beam
    # portability layer's gRPC/Netty direct byte buffers.
    #   flink.size = task.heap 3072 + task.off-heap 4096 + managed 512
    #              + network 512 + framework.off-heap 256 + framework.heap 128 = 8576m
    set_cfg "taskmanager.memory.task.heap.size"            "3072m"
    set_cfg "taskmanager.memory.task.off-heap.size"        "4096m"
    set_cfg "taskmanager.memory.framework.off-heap.size"   "256m"
    set_cfg "taskmanager.memory.managed.size"              "512m"
    set_cfg "taskmanager.memory.network.min"               "512m"
    set_cfg "taskmanager.memory.network.max"               "512m"
    set_cfg "taskmanager.memory.flink.size"                "8576m"
    set_cfg "taskmanager.memory.process.size"              "10112m"
else
    # PRE-FIX. task.heap deliberately UNSET so Flink auto-derives it (~640m) -- that is the
    # JVM-heap half of the bug. task.off-heap 512m is the direct-buffer half.
    set_cfg "taskmanager.memory.task.off-heap.size"        "512m"
    set_cfg "taskmanager.memory.framework.off-heap.size"   "256m"
    set_cfg "taskmanager.memory.managed.size"              "512m"
    set_cfg "taskmanager.memory.network.min"               "512m"
    set_cfg "taskmanager.memory.network.max"               "512m"
    set_cfg "taskmanager.memory.flink.size"                "2560m"
    set_cfg "taskmanager.memory.process.size"              "3200m"
fi

# --- flink-env.sh ---
# PYTHONNOUSERSITE: stops a system Python 3.10's ~/.local site-packages from leaking into
# the Beam worker's sys.path, which segfaults the worker on an ABI mismatch.
grep -q "PYTHONNOUSERSITE" "$FLINK_ENV" 2>/dev/null || \
    echo 'export PYTHONNOUSERSITE=1' >> "$FLINK_ENV"
grep -q "preferIPv4Stack" "$FLINK_ENV" 2>/dev/null || \
    echo 'export JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"' >> "$FLINK_ENV"

# --- Miniconda ---
if [ ! -f "$MINICONDA_PATH/bin/conda" ]; then
    echo "[TM] Installing Miniconda..."
    curl -fsSL "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" -o /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p "$MINICONDA_PATH"
fi
export PATH="$MINICONDA_PATH/bin:$PATH"
source "$MINICONDA_PATH/etc/profile.d/conda.sh"
grep -q "miniconda3/bin" ~/.bashrc || echo 'export PATH="$HOME/miniconda3/bin:$PATH"' >> ~/.bashrc
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main >/dev/null 2>&1 || true
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r    >/dev/null 2>&1 || true

if ! conda env list | grep -q "^mostream "; then
    echo "[TM] Creating mostream env (Python 3.9)..."
    conda create -y -q -n mostream python=3.9
fi
conda activate mostream

# --- Python packages. Versions are hard-pinned; see requirements.txt for why. ---
echo "[TM] Installing Python packages (this takes a few minutes)..."
pip install -q --no-user \
    apache-flink==2.0.0 tensorflow==2.14.0 "h5py==3.1.0" "nfp==0.1.3" \
    rdkit "pandas<2" "numpy<2" kafka-python networkx

# --- Sanity check: pyflink must resolve inside the conda env, not a 3.10 user-site ---
PYFLINK_FILE=$(python3.9 -c "import pyflink.datastream.data_stream as ds; print(ds.__file__)")
if echo "$PYFLINK_FILE" | grep -q ".local/lib/python3.10"; then
    echo "[TM] ERROR: pyflink resolving to Python 3.10 user-site: $PYFLINK_FILE" >&2
    echo "[TM] Fix: rm -rf ~/.local/lib/python3.10/site-packages/pyflink" >&2
    exit 1
fi
echo "[TM] pyflink OK: $PYFLINK_FILE"
sudo ln -sf "$MINICONDA_PATH/envs/mostream/bin/python" /usr/local/bin/python

# --- Data directories ---
sudo mkdir -p /mnt/media/MDStream/StreamML/networks
sudo chmod -R 777 /mnt/media/MDStream
mkdir -p "$HOME/MoStream/MoStream/MDStream/StreamML/search_space"

# --- Metrics relay ---
# Without this the JobManager cannot reach the TM's MetricQueryService, so plot_heap.py
# collects nothing -- and the memory experiments (E1/E2) become unmeasurable. It is
# retained in BOTH arms on purpose: the crash arm exists to be observed.
pkill -f "socat.*9998" 2>/dev/null || true
nohup socat TCP-LISTEN:9998,bind=${TM_IP},reuseaddr,fork TCP:127.0.0.1:9998 \
    > /tmp/socat-metrics.log 2>&1 &
disown
echo "[TM] socat relay: ${TM_IP}:9998 -> 127.0.0.1:9998"

echo ""
echo "=== TaskManager configured (arm=$ARM) ==="
grep -E '^taskmanager\.(memory|numberOfTaskSlots|host)' "$CONFIG" | sed 's/^/  /'
echo ""
echo "Next:"
echo "  1. Copy model.h5            -> /mnt/media/MDStream/StreamML/networks/"
echo "  2. Copy MOS-search-simple.txt -> ~/MoStream/MoStream/MDStream/StreamML/search_space/"
echo "  3. Start TM:      bash ~/MoStream/cloudlab/restart_taskmanager.sh"
echo "  4. Start monitor: bash ~/MoStream/cloudlab/monitor_taskmanager.sh   # BEFORE submitting"
