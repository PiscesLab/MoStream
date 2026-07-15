#!/bin/bash
# E6 teardown driver. Runs ON the TaskManager. Induces N Beam-worker teardowns at INT-second
# intervals and logs each with an epoch timestamp so the training-MAE trace can be aligned to it.
#   bash e6_run.sh <N> <interval_s> <tag>
# Worker match uses the '[b]oot' bracket trick: pgrep -f matches the FULL command line including
# this script's own pgrep, so a plain pattern self-matches the shell and returns the wrong PID.
N="${1:-6}"; INT="${2:-90}"; TAG="${3:-A}"; EV="$HOME/e6_${TAG}.log"
echo "$(date +%s) START tag=$TAG" > "$EV"
for i in $(seq 1 "$N"); do
  sleep "$INT"
  pid=$(pgrep -f "pyflink.fn_execution.beam.beam_[b]oot" | head -1)
  if [ -z "$pid" ]; then echo "$(date +%s) NOWORKER#$i" >> "$EV"; continue; fi
  echo "$(date +%s) TEARDOWN#$i pid=$pid" >> "$EV"
  kill -9 "$pid" 2>/dev/null
done
echo "$(date +%s) DONE" >> "$EV"
