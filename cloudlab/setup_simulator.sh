#!/bin/bash
# One-time setup for apt055 (WLGenerator / Simulator)
# Run this after cloning the repo on a fresh CloudLab node.
set -e

echo "=== [1/2] Python packages ==="
source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream
pip install -q requests qcelemental

echo "=== [2/2] Create stub gitinfo.py ==="
cat > ~/MoStream/MoStream/WLGenerator-node1/gitinfo.py << 'EOF'
def get_git_info():
    return {'commit': 'unknown'}
EOF

echo ""
echo "=== Setup complete for Simulator ==="
echo "NOTE: Copy dataset manually (from local machine):"
echo "  ssh simulator 'sudo mkdir -p /mnt/media/MDStream/WLGenerator/dataset && sudo chmod 777 /mnt/media/MDStream/WLGenerator/dataset'"
echo "  scp training-data-simple.txt simulator:/mnt/media/MDStream/WLGenerator/dataset/"
echo ""
echo "Start with:"
echo "  cd ~/MoStream/MoStream/WLGenerator-node1"
echo "  KAFKA_BOOTSTRAP=<kafka-ip>:9092 python simulator.py --interval 1.0"
