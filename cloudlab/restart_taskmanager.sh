#!/bin/bash
# Restart the Flink TaskManager. Must run ON the TaskManager node (node1 / 10.10.1.2).
#
# NOTE: there is deliberately NO socat metrics relay here. The old version of this script
# started one on port 9998 -- the very port the TaskManager's own MetricQueryService must
# bind. socat grabbed it first and the TM died at startup with:
#   ERROR NettyTransport - failed to bind to host:0.0.0.0 port:9998, shutting down
#   Caused by: java.net.BindException: Could not start actor system on any port in range 9998
# With taskmanager.bind-host=0.0.0.0 the metrics actor binds 0.0.0.0:9998 itself and the
# JobManager reaches it directly. Verified on Flink 2.0.0: all 38 TM metrics resolve over
# REST, including the Heap.Used / Direct.MemoryUsed pair monitor_taskmanager.sh scrapes.
# Do not reintroduce socat.
set -e

echo "=== [1/3] Stop TaskManager ==="
~/flink/bin/taskmanager.sh stop 2>/dev/null || true
sleep 5
pkill -f TaskManagerRunner 2>/dev/null || true

# A TM that failed to start leaves a stale pid file behind, after which taskmanager.sh
# SILENTLY refuses to start (exit 0, no output, no process). Clear it every time.
rm -f /tmp/flink-*-taskexecutor.pid

echo "=== [2/3] Start TaskManager ==="
~/flink/bin/taskmanager.sh start

echo "=== [3/3] Verify ==="
for i in $(seq 1 15); do
    sleep 2
    if ss -tln 2>/dev/null | grep -q ':9998'; then
        echo "OK: TaskManager up, MetricQueryService listening on 9998"
        grep -E '^taskmanager\.memory\.' ~/flink/conf/config.yaml | sed 's/^/  /'
        exit 0
    fi
done

echo "WARN: TaskManager did not come up within 30s. Last log lines:" >&2
ls -t ~/flink/log/*taskexecutor*.log | head -1 | xargs tail -15 >&2
exit 1
