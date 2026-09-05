#!/bin/bash
# ============================================================================
# E7 -- WORKER-RECYCLING BOUNDS THE FOOTPRINT. Run ON the TaskManager node.
#
#   bash e7_recycle_memory.sh [sample_s] [recycle_every_s] [duration_s]
#   e.g. bash e7_recycle_memory.sh 15 480 2100
#
# Samples the SUM of RSS over the real Beam worker processes (pgrep bracket-trick, so the
# inflated pyflink-udf-runner wrappers and any orphans are excluded) every sample_s seconds,
# and every recycle_every_s seconds SIGKILLs all Beam workers. Flink restarts the region,
# TrainFunction.open() re-runs and reloads the persisted weights, and the worker memory drops
# back to baseline before climbing again -- a BOUNDED SAWTOOTH, the measured solution to the
# unbounded pile-up of fig:footprint. Weight persistence (E6) is what makes each recycle
# non-destructive to training.
# ============================================================================
set -u
SAMPLE=${1:-15}
RECYCLE_EVERY=${2:-480}
DURATION=${3:-2100}
OUT="$HOME/MoStream/results/e7_mem.csv"
EV="$HOME/MoStream/results/e7_events.log"
PAT="pyflink.fn_execution.beam.beam_[b]oot"

echo "t,n_workers,total_mb,event" > "$OUT"
: > "$EV"
t0=$(date +%s)
last_recycle=0
echo "[e7] sample=${SAMPLE}s recycle_every=${RECYCLE_EVERY}s duration=${DURATION}s -> $OUT"
while [ $(( $(date +%s) - t0 )) -lt "$DURATION" ]; do
    now=$(( $(date +%s) - t0 ))
    pids=$(pgrep -f "$PAT")
    n=$(printf '%s\n' "$pids" | grep -c .)
    tot=0
    for p in $pids; do
        rss=$(awk '/VmRSS/{print $2}' "/proc/$p/status" 2>/dev/null)
        tot=$(( tot + ${rss:-0} ))
    done
    mb=$(awk "BEGIN{printf \"%.1f\", $tot/1024}")
    ev=""
    if [ "$now" -ge "$RECYCLE_EVERY" ] && [ $(( now - last_recycle )) -ge "$RECYCLE_EVERY" ]; then
        pkill -9 -f "$PAT" 2>/dev/null
        ev="recycle"
        last_recycle=$now
        echo "$now recycle (killed workers)" >> "$EV"
        echo "[e7] t=${now}s RECYCLE"
    fi
    echo "$now,$n,$mb,$ev" >> "$OUT"
    echo "[e7] t=${now}s n=$n total=${mb}MB $ev"
    sleep "$SAMPLE"
done
echo "[e7] done"
