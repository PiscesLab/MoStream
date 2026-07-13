#!/bin/bash
# ============================================================================
# E2 "BEFORE" ARM -- the pre-fix memory configuration. THIS IS EXPECTED TO CRASH.
#
# Do NOT use this to bring up a working cluster. Its only purpose is to reproduce
# the out-of-memory failure that the memory budget in setup_taskmanager.sh fixes,
# so the paper can show a before/after (experiment E2).
#
# It is a byte-for-byte copy of cloudlab/setup_taskmanager.sh EXCEPT for the
# memory block, which is reverted to the pre-fix values:
#
#                        BEFORE (here)        AFTER (setup_taskmanager.sh)
#   task.heap            unset -> ~640m auto  3072m
#   task.off-heap        512m                 4096m
#   framework.off-heap   256m                 256m      (unchanged)
#   managed              512m                 512m      (unchanged)
#   network              512m                 512m      (unchanged)
#   flink.size           2560m                8576m
#   process.size         3200m                10112m
#
# Expected failure: direct-buffer / off-heap OOM after roughly 18 hours at
# parallelism 8. The JVM heap also runs hot (task.heap auto-derives to ~640m and
# peaked at 99% with 8 parallel gRPC model-weight transfers).
#
# The socat metrics relay IS retained on purpose. Without it the MetricQueryService
# is unreachable, plot_heap.py collects nothing, and the crash you are trying to
# MEASURE becomes invisible. Never drop it from the crash arm.
#
# Usage: bash setup_taskmanager_e2before.sh <jm_internal_ip>
# ============================================================================
set -e

JM_IP=${1:?"Usage: bash setup_taskmanager_e2before.sh <jm_internal_ip>"}

echo "############################################################"
echo "#  E2 'BEFORE' ARM -- this TaskManager is EXPECTED to OOM.  #"
echo "#  Do not use for E0/E1/E3-E6 runs.                         #"
echo "############################################################"
echo ""

echo "=== [1/5] Flink env: force IPv4 to fix gRPC localhost resolution ==="
grep -q "preferIPv4Stack" ~/flink/conf/flink-env.sh || \
    echo 'export JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"' >> ~/flink/conf/flink-env.sh

echo "=== [2/5] Flink config: host binding + PRE-FIX memory ==="
TM_IP=$(hostname -I | tr ' ' '\n' | grep '^10\.10\.1\.' | head -1)
if [ -n "$TM_IP" ]; then
    grep -q "taskmanager.host" ~/flink/conf/config.yaml || \
        echo "taskmanager.host: $TM_IP" >> ~/flink/conf/config.yaml
fi
grep -q "taskmanager.bind-host" ~/flink/conf/config.yaml || \
    echo "taskmanager.bind-host: 0.0.0.0" >> ~/flink/conf/config.yaml
grep -q "metrics.internal.query-service.port" ~/flink/conf/config.yaml || \
    echo "metrics.internal.query-service.port: 9998" >> ~/flink/conf/config.yaml

# --- THE ONLY DIFFERENCE FROM THE TUNED SCRIPT IS BELOW ---
# Strip any memory settings a previous run may have left, so the arm is clean.
sed -i '/^taskmanager\.memory\./d' ~/flink/conf/config.yaml
# task.heap deliberately NOT set -> Flink auto-derives it (~640m). This is the bug.
echo "taskmanager.memory.task.off-heap.size: 512m"      >> ~/flink/conf/config.yaml
echo "taskmanager.memory.framework.off-heap.size: 256m" >> ~/flink/conf/config.yaml
echo "taskmanager.memory.managed.size: 512m"            >> ~/flink/conf/config.yaml
echo "taskmanager.memory.network.min: 512m"             >> ~/flink/conf/config.yaml
echo "taskmanager.memory.network.max: 512m"             >> ~/flink/conf/config.yaml
echo "taskmanager.memory.flink.size: 2560m"             >> ~/flink/conf/config.yaml
echo "taskmanager.memory.process.size: 3200m"           >> ~/flink/conf/config.yaml
# --- END OF DIFFERENCE ---

echo "=== [3/5] Python packages ==="
source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream
pip install -q apache-flink==2.0.0 tensorflow==2.14.0 \
    "h5py==3.1.0" "nfp==0.1.3" rdkit "pandas<2" "numpy<2"

echo "=== [4/5] Create search_space directory ==="
mkdir -p ~/MoStream/MoStream/MDStream/StreamML/search_space

echo "=== [5/5] Metrics relay (socat) -- REQUIRED so the OOM is observable ==="
which socat > /dev/null 2>&1 || sudo apt-get install -y socat
pkill -f "socat.*9998" 2>/dev/null || true
nohup socat TCP-LISTEN:9998,bind=${TM_IP},reuseaddr,fork TCP:127.0.0.1:9998 > /tmp/socat-metrics.log 2>&1 &
echo "socat relay started (PID $!): ${TM_IP}:9998 -> 127.0.0.1:9998"

echo ""
echo "=== E2-BEFORE TaskManager configured (expected to OOM in ~18h) ==="
echo "Effective memory config:"
grep '^taskmanager\.memory\.' ~/flink/conf/config.yaml | sed 's/^/  /'
echo ""
echo "Next:"
echo "  1. Copy MOS-search-simple.txt to ~/MoStream/MoStream/MDStream/StreamML/search_space/"
echo "  2. Start TM:      bash ~/MoStream/cloudlab/restart_taskmanager.sh"
echo "  3. Start monitor: bash ~/MoStream/cloudlab/monitor_taskmanager.sh   # <-- BEFORE submitting"
echo "  4. Submit job:    bash ~/MoStream/cloudlab/submit_job.sh"
echo "  5. Walk away. Expect the OOM in the TM log after ~18h; keep the monitor CSV."
