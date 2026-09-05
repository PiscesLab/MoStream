#!/bin/bash
# ============================================================================
# E6 -- INDUCED WORKER TEARDOWN. Run ON the TaskManager node.
#
#   bash e6_induce_teardown.sh <interval_seconds> [duration_seconds]
#   e.g.  bash e6_induce_teardown.sh 600     # tear a worker down every 10 min
#         bash e6_induce_teardown.sh 1200    # ... every 20 min
#
# WHY THIS EXISTS
# The paper originally claimed the engine "recycles Python worker environments roughly
# every ten minutes". It does not. Verified empirically on Flink 2.0.0: the Beam worker
# PIDs are stable across a healthy run (same PIDs, uptime growing monotonically past 13+
# minutes), and Beam's environmentExpirationMillis defaults to 0 -- never expire. Flink
# exposes no python.* option to enable it.
#
# So worker teardown is not something that happens TO us on a timer; it is something we
# must INDUCE. That is strictly better experimentally: the teardown interval becomes an
# independent variable we set, rather than a confound we suffer.
#
# WHAT IT TESTS (this is the whole point of contribution 2)
# The teardown is the only moment at which weight persistence matters. Kill the Beam
# worker -> Flink restarts the affected region -> TrainFunction.open() re-executes. Then:
#
#   WITH persistence:     open() reloads /tmp/mostream_weights_<subtask>.json.
#                         Training MAE should continue converging smoothly.
#
#   WITHOUT persistence:  open() rebuilds the model from model.h5 at initial weights.
#                         Training MAE should JUMP BACK to its initial value at every
#                         teardown -- a SAWTOOTH.
#
# The sawtooth is the falsifiable prediction. If it does not appear, contribution 2 is
# wrong and must be withdrawn.
#
# To run the no-persistence arm, disable the checkpoint reload in TrainFunction.open()
# (or point MOSTREAM_WEIGHTS_DIR at a non-writable path) and re-submit the job.
#
# Emits an event log so teardowns can be aligned against the MAE trace afterwards.
# ============================================================================
set -u

INTERVAL="${1:?Usage: bash e6_induce_teardown.sh <interval_seconds> [duration_seconds]}"
DURATION="${2:-0}"                     # 0 = run until killed
EVENTS="$HOME/e6_teardown_events.log"

echo "=== E6: inducing Beam worker teardown every ${INTERVAL}s ==="
echo "    event log -> $EVENTS"
[ "$DURATION" -gt 0 ] && echo "    will stop after ${DURATION}s"
echo ""

start=$(date +%s)
n=0

while true; do
    if [ "$DURATION" -gt 0 ] && [ $(( $(date +%s) - start )) -ge "$DURATION" ]; then
        echo "=== done: ${n} teardowns induced ==="
        exit 0
    fi

    sleep "$INTERVAL"

    # Pick one live Beam SDK worker. Killing a single worker (rather than the whole
    # TaskManager) is the closest analogue of an environment recycle: the JVM survives,
    # only the Python process holding the model dies.
    # Match the actual Python worker, which runs `python -m pyflink.fn_execution.beam.beam_boot`.
    # TWO traps here, both of which silently induced ZERO teardowns until fixed:
    #   1. pgrep uses EXTENDED regex, so alternation is a bare '|'. An escaped '\|' matches a
    #      literal pipe and never matches anything.
    #   2. pgrep -f matches against the FULL command line, INCLUDING this script's own pgrep
    #      invocation, so a plain pattern self-matches the shell running it and returns the wrong
    #      PID (never the worker). The '[b]' bracket makes the pattern text in our own command
    #      line ("beam_[b]oot") NOT match the regex, the classic `ps | grep [x]` trick, so only
    #      the real worker matches.
    pid=$(pgrep -f 'pyflink.fn_execution.beam.beam_[b]oot' | head -1)

    if [ -z "$pid" ]; then
        echo "$(date '+%F %T')  WARN  no Beam worker found (job warming up or down?)" | tee -a "$EVENTS"
        continue
    fi

    rss=$(awk '/VmRSS/{print $2}' "/proc/$pid/status" 2>/dev/null)
    n=$((n + 1))
    echo "$(date '+%F %T')  TEARDOWN #${n}  pid=${pid}  rss=$(( ${rss:-0} / 1024 ))MB" | tee -a "$EVENTS"
    kill -9 "$pid" 2>/dev/null || true
done
