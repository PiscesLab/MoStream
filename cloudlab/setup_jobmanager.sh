#!/bin/bash
# One-time setup for Flink JobManager node.
# Run after cloning the repo. Sources cluster.conf for IPs.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster.conf"

JM_IP="$JM_HOST"
TM_IP="$TM_HOST"

echo "=== [1/5] DNS fixes ==="
sudo tee /etc/resolv.conf <<< "nameserver 8.8.8.8" > /dev/null
grep -q "$(hostname -s)" /etc/hosts || sudo bash -c "echo '127.0.0.1 $(hostname -s)' >> /etc/hosts"
grep -q "$TM_IP" /etc/hosts || sudo bash -c "echo '$TM_IP apt063.apt.emulab.net apt063' >> /etc/hosts"

echo "=== [2/5] Flink config: replace any old hostname/IP with current JM IP ==="
sed -i "s/jobmanager\.rpc\.address:.*/jobmanager.rpc.address: $JM_IP/" ~/flink/conf/config.yaml
sed -i "/address:/s/[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}/$JM_IP/g" ~/flink/conf/config.yaml

grep -q "bundle.time" ~/flink/conf/config.yaml || cat >> ~/flink/conf/config.yaml << 'EOF'
python.fn-execution.bundle.time: 60000
python.fn-execution.bundle.size: 1
EOF

echo "=== [3/5] Clear workers file (TM started manually) ==="
echo "" > ~/flink/conf/workers

echo "=== [4/5] Python packages ==="
source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream
pip install -q "h5py==3.1.0" "nfp==0.1.3"

echo "=== [5/5] Fix submit_job.sh line endings ==="
sed -i 's/\r//' "$SCRIPT_DIR/submit_job.sh"

echo ""
echo "=== JobManager setup complete (JM: $JM_IP, TM: $TM_IP) ==="
echo "NOTE: Copy MOS-search-simple.txt to:"
echo "  ~/MoStream/MoStream/MDStream/StreamML/search_space/"
