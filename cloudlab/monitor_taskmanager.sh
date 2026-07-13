#!/bin/bash
# Run ON the TaskManager node (node1).
# Usage: bash monitor_taskmanager.sh [interval_seconds]

INTERVAL=${1:-30}
LOGFILE="$HOME/tm_memory.log"
JM_REST="${JM_REST:-http://10.10.1.4:8081}"

echo "Monitoring TM memory every ${INTERVAL}s  ->  $LOGFILE"
echo "Press Ctrl+C to stop."

while true; do
    TS=$(date '+%Y-%m-%d %H:%M:%S')

    TM_PID=$(pgrep -f "TaskManagerRunner" | head -1)
    if [ -z "$TM_PID" ]; then
        echo "$TS  [WARN] TM process not found" | tee -a "$LOGFILE"
        sleep "$INTERVAL"; continue
    fi

    # --- RSS of TM Java process ---
    RSS_KB=$(grep VmRSS /proc/"$TM_PID"/status 2>/dev/null | awk '{print $2}')
    RSS_MB=$(( RSS_KB / 1024 ))

    # --- Direct memory: get TM metrics via Flink REST ---
    # Flink 2.0: GET /taskmanagers/<id>/metrics?get=Status.JVM.Memory.Direct.Used,...
    TM_ID=$(curl -sf "${JM_REST}/taskmanagers" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['taskmanagers'][0]['id'])" 2>/dev/null)

    if [ -n "$TM_ID" ]; then
        METRICS=$(curl -sf "${JM_REST}/taskmanagers/${TM_ID}/metrics?get=Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max,Status.JVM.Memory.Direct.MemoryUsed,Status.JVM.Memory.Direct.TotalCapacity" 2>/dev/null \
            | python3 -c "
import sys, json
try:
    data = {m['id']: int(float(m['value'])) // (1024*1024) for m in json.load(sys.stdin)}
    hu = data.get('Status.JVM.Memory.Heap.Used', 0)
    hm = data.get('Status.JVM.Memory.Heap.Max',  0)
    du = data.get('Status.JVM.Memory.Direct.MemoryUsed', 0)
    dm = data.get('Status.JVM.Memory.Direct.TotalCapacity', 0)
    print(f'jvm_heap={hu}/{hm}MB  direct={du}/{dm}MB')
except Exception as e:
    print(f'metrics_err={e}')
" 2>/dev/null)
    else
        METRICS="rest=unavailable"
    fi

    # --- Python worker memory (separate processes from JVM) ---
    PY_MB=$(ps aux | grep -E "python.*beam_sdk_worker|python.*pyflink" \
        | grep -v grep | awk '{sum+=$6} END {printf "%dMB", sum/1024}')
    PY_COUNT=$(ps aux | grep -E "python.*beam_sdk_worker|python.*pyflink" \
        | grep -v grep | wc -l)

    # --- Training metrics from TM log (model_id 0, latest + rolling avg of last 5) ---
    TM_LOG=$(ls ~/flink/log/flink-*-taskexecutor-0-*.log 2>/dev/null | grep -v '\.[0-9]*$' | head -1)
    if [ -n "$TM_LOG" ]; then
        M0_LOSS=$(grep "model_id:  0  train_loss:" "$TM_LOG" 2>/dev/null \
            | tail -1 | grep -oP '[\d.]+(?=\])' | awk '{printf "%.2f",$1}')
        M0_AVG=$(grep "model_id:  0  train_loss:" "$TM_LOG" 2>/dev/null \
            | tail -5 | grep -oP '[\d.]+(?=\])' \
            | awk '{s+=$1;n++} END{if(n>0) printf "%.2f",s/n}')
        M0_MAE=$(grep "model_id:  0  train_mae:" "$TM_LOG" 2>/dev/null \
            | tail -1 | grep -oP '[\d.]+(?=\])' | awk '{printf "%.2f",$1}')
        TRAIN="m0_loss=${M0_LOSS:-n/a}(avg5=${M0_AVG:-n/a})  m0_mae=${M0_MAE:-n/a}"
    else
        TRAIN="m0_loss=n/a(no_log)  m0_mae=n/a"
    fi

    # --- System memory ---
    SYS=$(free -h | awk '/^Mem:/{print "sys_used="$3"/"$2}')

    LINE="$TS  pid=$TM_PID  rss=${RSS_MB}MB  $METRICS  py_workers=${PY_COUNT}x${PY_MB}  $TRAIN  $SYS"
    echo "$LINE" | tee -a "$LOGFILE"

    sleep "$INTERVAL"
done
