#!/usr/bin/env bash
# Run Colmena's own molecular-design campaign against our data.
#
# This is the baseline arm. It runs upstream's run.py unmodified, so what it
# measures is Colmena's behaviour rather than our reading of it.
#
#   ./cloudlab/colmena_baseline/run_upstream.sh
#
# Budget must match whatever the streaming arm was given, or the two curves are not
# comparable. SEARCH_SIZE is the number of simulations the campaign performs.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UPSTREAM_DIR="${UPSTREAM_DIR:-$HOME/colmena-upstream}"
ENV_NAME="${ENV_NAME:-colmena-upstream}"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/results/colmena}"

# The three inputs, all shared with the streaming arm. MOS-search.csv and
# training-data.json are the gitignored files from the data download; see the
# repository README.
# First match wins: the repo checkout, a local copy, then the shared data drive. Copy
# off a network mount before running; pandas reads the 1.5 GB search space at startup.
first_existing() { for f in "$@"; do [[ -f "$f" ]] && { echo "$f"; return; }; done; echo "$1"; }
DATA_MNT="${DATA_MNT:-/mnt/n/Applications/MoStreamData/MoStream/MoStream/MDStream/WLGenerator}"
SEARCH_SPACE="${SEARCH_SPACE:-$(first_existing \
  "$REPO_ROOT/MoStream/MDStream/WLGenerator/search_space/MOS-search.csv" \
  "$HOME/colmena-data/MOS-search.csv" \
  "$DATA_MNT/search_space/MOS-search.csv")}"
TRAINING_SET="${TRAINING_SET:-$(first_existing \
  "$REPO_ROOT/MoStream/MDStream/WLGenerator/dataset/training-data.json" \
  "$HOME/colmena-data/training-data.json" \
  "$DATA_MNT/dataset/training-data.json")}"
MPNN_MODEL="${MPNN_MODEL:-$REPO_ROOT/MoStream/MDStream/StreamML/networks/model.h5}"

# Campaign parameters. SEARCH_SIZE is the shared compute budget.
SEARCH_SIZE="${SEARCH_SIZE:-5000}"
QC_WORKERS="${QC_WORKERS:-3}"        # one per simulation node, as the streaming arm has
# Ensemble size. Upstream's default is 8, but on a 16-core host each model costs
# 23-42 min to train and ~35 min to score the search space, and with one ML worker
# they run serially: eight would not submit a first simulation for 7.6-10.3 h, past a
# 6 h window. One model is what fits, and it matches the streaming arm's single model.
MODEL_COUNT="${MODEL_COUNT:-1}"
NUM_EPOCHS="${NUM_EPOCHS:-128}"
RETRAIN_FREQ="${RETRAIN_FREQ:-1}"
# Wall-clock window. The discovery curve is hits against time, so every arm gets the
# same window on the same host. SEARCH_SIZE is set high so it is not the limiter.
WINDOW_H="${WINDOW_H:-6}"
REDIS_PORT="${REDIS_PORT:-6379}"
# Molecules scored per inference task. Upstream scores the WHOLE search space each
# round, split into tasks of this size; that global rescore is the cost the streaming
# arm avoids, so leave it at upstream's value when measuring.
ML_TASK_SIZE="${ML_TASK_SIZE:-50000}"

# Threads per xTB call. Upstream sizes this for a 64-core Theta node. For a fair
# comparison it must match the streaming arm's per-simulation allocation, and the
# product QC_WORKERS x XTB_CORES must not exceed the cores available, or the xTB
# processes oversubscribe the machine and every simulation slows down.
XTB_CORES="${XTB_CORES:-4}"
export COLMENA_QC_WORKERS="$QC_WORKERS" COLMENA_ML_WORKERS="${ML_WORKERS:-1}" COLMENA_XTB_CORES="$XTB_CORES"

total=$(( QC_WORKERS * XTB_CORES ))
if (( total > $(nproc) )); then
  echo "warning: $QC_WORKERS workers x $XTB_CORES threads = $total, but this host has $(nproc) cores" >&2
fi

for f in "$SEARCH_SPACE" "$TRAINING_SET" "$MPNN_MODEL"; do
  [[ -f "$f" ]] || { echo "missing input: $f" >&2; exit 1; }
done

mkdir -p "$OUT_DIR"

# shellcheck disable=SC1091
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$ENV_NAME"

if ! redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; then
  echo "==> starting redis on $REDIS_PORT"
  redis-server --port "$REDIS_PORT" --daemonize yes --save '' --appendonly no
  sleep 2
fi

cd "$UPSTREAM_DIR/molecular-design"
echo "==> running Colmena campaign, budget $SEARCH_SIZE, $QC_WORKERS workers"
# ProxyStore on, with upstream's own backends from run-xtb-lambda-parsl.sh.
# --no-proxystore is their ablation, and it runs out of memory at this scale: it ships
# each 50k-molecule chunk as a ~58 MB message with ~20 in flight, 4-5 GB per round.
timeout --signal=INT --kill-after=120 "$(( WINDOW_H * 3600 ))" \
python run.py \
  --use-parsl \
  --infer-ps-backend redis --train-ps-backend redis --simulate-ps-backend file \
  --ps-threshold 10000 \
  --ps-file-dir proxy-store-scratch \
  --redisport "$REDIS_PORT" \
  --qc-specification xtb \
  --mpnn-model-path "$MPNN_MODEL" \
  --training-set "$TRAINING_SET" \
  --search-space "$SEARCH_SPACE" \
  --model-count "$MODEL_COUNT" \
  --num-epochs "$NUM_EPOCHS" \
  --retrain-frequency "$RETRAIN_FREQ" \
  --search-size "$SEARCH_SIZE" \
  --num-qc-workers "$QC_WORKERS" \
  --molecules-per-ml-task "$ML_TASK_SIZE" \
  2>&1 | tee "$OUT_DIR/run.log" || true

echo
echo "==> campaign finished. Upstream writes a runs/ directory next to run.py."
RUN_DIR="$(ls -td "$UPSTREAM_DIR"/molecular-design/runs/*/ | head -1)"
echo "    run dir: $RUN_DIR"
python "$REPO_ROOT/cloudlab/colmena_baseline/make_curve.py" \
  --upstream "$RUN_DIR" --window-h "$WINDOW_H" --out "$REPO_ROOT/results/campaign/colmena.csv"
