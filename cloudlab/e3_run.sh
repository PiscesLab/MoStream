#!/bin/bash
# E3 -- parallelism sweep driver. Runs ON the JobManager (same host as submit_job.sh).
#
# For each P in the sweep it: submits MDWorkflow at parallelism P, waits for the job to
# reach RUNNING and warm up, samples Flink REST metrics (e3_metrics.py) AND steering
# latency (e0_steering_latency.py) for a fixed window, then cancels the job and moves on.
# Per-arm output lands in results/e3/{metrics_pP.csv, latency_pP.csv}; a manifest records
# the P -> job-id -> [start,end] mapping.
#
#   bash cloudlab/e3_run.sh
#   SWEEP="1 2 4 8" WARMUP=300 WINDOW=600 OFFSET=earliest bash cloudlab/e3_run.sh
#
# KNOBS (env-overridable):
#   SWEEP    parallelisms to test            (default "1 2 4 8")
#   WARMUP   settle time before measuring     (default 300 s)
#   WINDOW   measurement window per arm       (default 600 s)
#   OFFSET   Kafka start offset per arm        (default earliest)
#   JM_REST  JobManager REST host:port        (default 10.10.1.4:8081)
#   OUTDIR   results dir                       (default <repo>/results/e3)
#   INTERVAL REST sampling cadence            (default 15 s)
#
# REGIME NOTE -- OFFSET decides what you actually measure:
#   earliest : each arm replays the backlog, so it runs SATURATED. Throughput is then the
#              MAX sustainable rate at P (the scaling curve), and busy/backpressure show
#              where P=1 chokes. Steering latency in this regime is under-load latency,
#              not steady state. This is the default because the scaling CURVE is E3's
#              headline and needs P=1 to be the bottleneck.
#   latest   : each arm processes only the live sim rate. Latency is steady-state, but if
#              the input never saturates P=1 the throughput curve is flat (nothing to
#              scale). Use only if the sims are driving the cluster hard.
# Total wall-clock ~= |SWEEP| * (WARMUP + WINDOW + ~30 s cancel/cooldown).
#
# The TM leaks Metaspace PER SUBMISSION (~10 submits -> OOM, misreported as
# NoResourceAvailableException). A 4-arm sweep is well under that, but if E1 already
# churned the TM, run restart_taskmanager.sh ON THE TM NODE before starting E3.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONF="$SCRIPT_DIR/cluster.conf"
[ -f "$CONF" ] || { echo "ERROR: $CONF missing (copy cluster.conf.template)"; exit 1; }
source "$CONF"

SWEEP="${SWEEP:-1 2 4 8}"
WARMUP="${WARMUP:-300}"
WINDOW="${WINDOW:-600}"
OFFSET="${OFFSET:-earliest}"
JM_REST="${JM_REST:-10.10.1.4:8081}"
OUTDIR="${OUTDIR:-$REPO_ROOT/results/e3}"
FLINK_HOME="${FLINK_HOME:-$HOME/flink}"
INTERVAL="${INTERVAL:-15}"
REST="http://$JM_REST"

[ -n "${KAFKA_HOST:-}" ] || { echo "ERROR: KAFKA_HOST unset in cluster.conf"; exit 1; }

source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate mostream

mkdir -p "$OUTDIR"
MANIFEST="$OUTDIR/manifest.csv"
[ -f "$MANIFEST" ] || \
    echo "parallelism,jid,warmup_s,window_s,offset,start_epoch,end_epoch" > "$MANIFEST"

# Print the id of the single RUNNING job (empty if none). curl+python, no jq dependency.
running_jid() {
    curl -s --max-time 10 "$REST/jobs" 2>/dev/null | python3 -c "
import sys, json
try: d = json.load(sys.stdin)
except Exception: print(''); sys.exit()
r = [j['id'] for j in d.get('jobs', []) if j.get('status') == 'RUNNING']
print(r[0] if r else '')
"
}

cancel_all() {
    local jids
    jids="$(curl -s --max-time 10 "$REST/jobs" 2>/dev/null | python3 -c "
import sys, json
try: d = json.load(sys.stdin)
except Exception: sys.exit()
print(' '.join(j['id'] for j in d.get('jobs', []) if j.get('status') == 'RUNNING'))
")"
    for jid in $jids; do
        echo "[e3] cancelling lingering job $jid"
        "$FLINK_HOME/bin/flink" cancel "$jid" >/dev/null 2>&1 || true
    done
}

echo "[e3] sweep P in [$SWEEP]  warmup=${WARMUP}s window=${WINDOW}s offset=$OFFSET"
echo "[e3] REST=$REST  kafka=$KAFKA_HOST:9092  out=$OUTDIR"

for P in $SWEEP; do
    echo ""
    echo "===================== P=$P ====================="
    cancel_all
    sleep 10   # let slots free before resubmit

    echo "[e3] submitting at parallelism $P (offset=$OFFSET)"
    if ! PARALLELISM="$P" STARTING_OFFSET="$OFFSET" bash "$SCRIPT_DIR/submit_job.sh" \
            > "$OUTDIR/submit_p${P}.log" 2>&1; then
        echo "[e3] submit FAILED for P=$P (see $OUTDIR/submit_p${P}.log); skipping arm"
        continue
    fi

    # wait up to 120 s for a RUNNING job to appear
    JID=""
    for _ in $(seq 1 24); do
        JID="$(running_jid)"
        [ -n "$JID" ] && break
        sleep 5
    done
    if [ -z "$JID" ]; then
        echo "[e3] no RUNNING job appeared for P=$P (see $OUTDIR/submit_p${P}.log); skipping"
        continue
    fi
    echo "[e3] job $JID RUNNING; warming up ${WARMUP}s"
    sleep "$WARMUP"

    if [ "$(running_jid)" != "$JID" ]; then
        echo "[e3] job $JID no longer the RUNNING job after warm-up (crash/restart?); skipping"
        continue
    fi

    START=$(date +%s)
    echo "[e3] measuring ${WINDOW}s: REST metrics + steering latency (concurrent)"
    python3 "$SCRIPT_DIR/e3_metrics.py" --jm "$JM_REST" --jid "$JID" \
        --parallelism "$P" --duration "$WINDOW" --interval "$INTERVAL" \
        --out "$OUTDIR/metrics_p${P}.csv" > "$OUTDIR/metrics_p${P}.log" 2>&1 &
    MPID=$!
    python3 "$SCRIPT_DIR/e0_steering_latency.py" --bootstrap "$KAFKA_HOST:9092" \
        --duration "$WINDOW" --out "$OUTDIR/latency_p${P}" \
        > "$OUTDIR/latency_p${P}.log" 2>&1 &
    LPID=$!
    wait "$MPID"; wait "$LPID"
    END=$(date +%s)

    echo "$P,$JID,$WARMUP,$WINDOW,$OFFSET,$START,$END" >> "$MANIFEST"
    echo "[e3] P=$P done; cancelling $JID"
    "$FLINK_HOME/bin/flink" cancel "$JID" >/dev/null 2>&1 || true
    sleep 15
done

cancel_all
echo ""
echo "[e3] SWEEP COMPLETE. Per-arm data in $OUTDIR; manifest $MANIFEST"
echo "[e3] pull results/ to the laptop, then:"
echo "[e3]   python cloudlab/make_figures.py --only scaling"
echo "[e3]   python cloudlab/make_figures.py --only utilisation"
