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
    ~/flink/bin/taskmanager.sh start
    sleep 15

    if pgrep -f "TaskManagerRunner" > /dev/null; then
        log "TM restarted successfully."
        # Restart socat metrics relay (Flink 2.0 binds metrics to 127.0.0.1; socat bridges to 10.10.1.2)
        pkill -f "socat.*9998" 2>/dev/null || true
        sleep 3  # wait for metrics actor to bind to 127.0.0.1:9998
        TM_IP=$(hostname -I | tr ' ' '\n' | grep '^10\.10\.1\.' | head -1)
        nohup socat TCP-LISTEN:9998,bind=${TM_IP},reuseaddr,fork TCP:127.0.0.1:9998 \
            > /tmp/socat-metrics.log 2>&1 &
        log "socat relay restarted: ${TM_IP}:9998 → 127.0.0.1:9998 (PID $!)"
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
