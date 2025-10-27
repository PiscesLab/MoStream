
export JOBNAME=$parsl.localprovider.1688749199.1208093
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
process_worker_pool.py   -a 192.168.1.6,128.110.96.16,node-5.jliu1721-161156.lsucloud-pg0.apt.emulab.net,127.0.0.1 -p 0 -c 1.0 -m None --poll 10 --task_port=54225 --result_port=54750 --logdir=/mnt/media/MDStream/WLGenerator/local/runinfo/005/HighThroughputExecutor --block_id=0 --hb_period=30  --hb_threshold=120 --cpu-affinity none --available-accelerators  --start-method fork
}
for COUNT in $(seq 1 1 $WORKERCOUNT); do
    [[ "1" == "1" ]] && echo "Launching worker: $COUNT"
    CMD $COUNT &
    PIDS="$PIDS $!"
done

ALLFAILED=1
ANYFAILED=0
for PID in $PIDS ; do
    wait $PID
    if [ "$?" != "0" ]; then
        ANYFAILED=1
    else
        ALLFAILED=0
    fi
done

[[ "1" == "1" ]] && echo "All workers done"
if [ "$FAILONANY" == "1" ]; then
    exit $ANYFAILED
else
    exit $ALLFAILED
fi
