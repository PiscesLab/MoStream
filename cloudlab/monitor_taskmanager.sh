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
    # Match `beam_boot` ONLY, via the [b] bracket trick so this pipeline does not self-match.
    #
    # The old pattern ("python.*beam_sdk_worker|python.*pyflink") also matched the 8
    # `pyflink-udf-runner.sh` shell WRAPPERS that launch the workers. Each wrapper holds ~3 MB,
    # so the memory SUM was unaffected, but the COUNT was inflated: it logged 18 where there
    # were 8 real workers (8 wrappers + 8 workers + 2 orphans from a killed job). That count is
    # what put "16 Python worker processes" into the paper. The real model is one worker per
    # task SLOT, not per operator: slot sharing puts a Train + Infer + Rank subtask in one slot,
    # so at P=8 there are 8 workers of ~2.2 GB each, not 16 of ~1.05 GB.
    #
    # Orphans (ppid=1) left by a killed job still match beam_boot and would be counted into a
    # later run's total, so exclude them: a live worker is parented by its wrapper, never by init.
    PY_PIDS=$(ps -eo pid,ppid,args --no-headers \
        | grep 'pyflink.fn_execution.beam.beam_[b]oot' \
        | awk '$2 != 1 {print $1}')
    if [ -n "$PY_PIDS" ]; then
        PY_MB=$(ps -o rss= -p "$(echo "$PY_PIDS" | tr '\n' ',' | sed 's/,$//')" \
            | awk '{sum+=$1} END {printf "%dMB", sum/1024}')
        PY_COUNT=$(echo "$PY_PIDS" | wc -l)
    else
        PY_MB="0MB"; PY_COUNT=0
    fi

    # --- Training metrics from TM log ---
    # Read the TRAINPROF line that NPMMModel.py actually emits. The old code grepped for
    # "model_id:  0  train_loss:", a print() format the job stopped producing; since the TM
    # log is cumulative, `tail -1` kept matching a line from a long-dead run and the reported
    # loss/mae were FROZEN at a stale value for hours. Aggregate over the last 16 records
    # (~one per model) rather than a single model_id, so one straggler cannot skew the view.
    TM_LOG=$(ls ~/flink/log/flink-*-taskexecutor-0-*.log 2>/dev/null | grep -v '\.[0-9]*$' | head -1)
    if [ -n "$TM_LOG" ]; then
        RECENT=$(grep "TRAINPROF subtask=" "$TM_LOG" 2>/dev/null | tail -16)
        LOSS=$(echo "$RECENT" | tail -1 | grep -oP 'loss=\K[\d.]+' | awk '{printf "%.2f",$1}')
        AVG=$(echo "$RECENT" | grep -oP 'loss=\K[\d.]+' \
            | awk '{s+=$1;n++} END{if(n>0) printf "%.2f",s/n}')
        MAE=$(echo "$RECENT" | grep -oP 'mae=\K[\d.]+' \
            | awk '{s+=$1;n++} END{if(n>0) printf "%.2f",s/n}')
        TRAIN="loss=${LOSS:-n/a}(avg16=${AVG:-n/a})  mae=${MAE:-n/a}"
    else
        TRAIN="loss=n/a(no_log)  mae=n/a"
    fi

    # --- System memory ---
    SYS=$(free -h | awk '/^Mem:/{print "sys_used="$3"/"$2}')

    LINE="$TS  pid=$TM_PID  rss=${RSS_MB}MB  $METRICS  py_workers=${PY_COUNT}x${PY_MB}  $TRAIN  $SYS"
    echo "$LINE" | tee -a "$LOGFILE"

    sleep "$INTERVAL"
done
