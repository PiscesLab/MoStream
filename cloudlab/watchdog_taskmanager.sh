#!/usr/bin/env bash
# Watchdog: monitors the Flink TaskManager on apt063.
# If the TM Java process dies it kills orphaned Python beam_boot workers
# (which accumulate and OOM the machine) then restarts the TM.
#
# Usage: nohup bash watchdog_taskmanager.sh > /tmp/tm-watchdog.log 2>&1 &

PYFL=/users/NamSDSU/miniconda3/envs/mostream/lib/python3.9/site-packages/pyflink
CHECK_INTERVAL=30   # seconds between health checks

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

cleanup_and_restart() {
    log "TM process gone — killing orphaned Python UDF workers..."
    BEFORE=$(ps aux | grep -c "beam_boot")
    pkill -9 -f "beam_boot"
    pkill -9 -f "pyflink-udf-runner"
    sleep 3
    AFTER=$(ps aux | grep -c "beam_boot")
    log "beam_boot processes: $BEFORE -> $AFTER"
    log "Memory after cleanup:"
    free -h | grep Mem

    log "Starting TaskManager..."
    $PYFL/bin/taskmanager.sh start
    sleep 15

    if pgrep -f "TaskManagerRunner" > /dev/null; then
        log "TM restarted successfully."
    else
        log "WARNING: TM failed to start — check $PYFL/log/ for errors."
    fi
}

log "Watchdog started. Monitoring TM every ${CHECK_INTERVAL}s."
log "PYFL=$PYFL"

while true; do
    if ! pgrep -f "TaskManagerRunner" > /dev/null; then
        cleanup_and_restart
    fi
    sleep "$CHECK_INTERVAL"
done
