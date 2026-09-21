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
SEARCH_SPACE="${SEARCH_SPACE:-$REPO_ROOT/MoStream/MDStream/WLGenerator/search_space/MOS-search.csv}"
TRAINING_SET="${TRAINING_SET:-$REPO_ROOT/MoStream/MDStream/WLGenerator/dataset/training-data.json}"
MPNN_MODEL="${MPNN_MODEL:-$REPO_ROOT/MoStream/MDStream/StreamML/networks/model.h5}"

# Campaign parameters. SEARCH_SIZE is the shared compute budget.
SEARCH_SIZE="${SEARCH_SIZE:-512}"
QC_WORKERS="${QC_WORKERS:-3}"        # one per simulation node, as the streaming arm has
MODEL_COUNT="${MODEL_COUNT:-8}"      # upstream's ensemble size; see README on this
NUM_EPOCHS="${NUM_EPOCHS:-128}"
RETRAIN_FREQ="${RETRAIN_FREQ:-1}"
REDIS_PORT="${REDIS_PORT:-6379}"
# Molecules scored per inference task. Upstream scores the WHOLE search space each
# round, split into tasks of this size; that global rescore is the cost the streaming
# arm avoids, so leave it at upstream's value when measuring.
ML_TASK_SIZE="${ML_TASK_SIZE:-50000}"

# Threads per xTB call. Upstream sizes this for a 64-core Theta node. For a fair
# comparison it must match the streaming arm's per-simulation allocation, and the
# product QC_WORKERS x XTB_CORES must not exceed the cores available, or the xTB
# processes oversubscribe the machine and every simulation slows down.
XTB_CORES="${XTB_CORES:-16}"
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
python run.py \
  --use-parsl \
  --no-proxystore \
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
  2>&1 | tee "$OUT_DIR/run.log"

echo
echo "==> campaign finished. Upstream writes a runs/ directory next to run.py."
echo "    Convert its results into the discovery curve with:"
echo "      python cloudlab/colmena_baseline/make_curve.py --upstream <runs/dir> --out results/campaign/colmena.csv"
