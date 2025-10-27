
export JOBNAME=$parsl.localprovider.1688747463.950685
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
process_worker_pool.py   -a 127.0.0.1,128.110.96.17,node-1.jliu1721-161156.lsucloud-pg0.apt.emulab.net,192.168.1.2 -p 0 -c 1.0 -m None --poll 10 --task_port=54242 --result_port=54641 --logdir=/mnt/media/MDStream/WLGenerator/local/runinfo/003/HighThroughputExecutor --block_id=0 --hb_period=30  --hb_threshold=120 --cpu-affinity none --available-accelerators  --start-method fork
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
