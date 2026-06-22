#!/bin/bash
# Restart TM and re-launch socat metrics relay.
# Must run ON the TaskManager node (node1 / 10.10.1.2).
# socat relay is required for Flink Web UI metrics to work -- TM binds
# MetricQueryService to 127.0.0.1 but advertises external IP to JM.
set -e

TM_IP=$(hostname -I | tr ' ' '\n' | grep '^10\.10\.1\.' | head -1)

echo "=== [1/3] Restart TaskManager ==="
~/flink/bin/taskmanager.sh stop
sleep 5
~/flink/bin/taskmanager.sh start

echo "=== [2/3] Restart socat metrics relay (${TM_IP}:9998 -> 127.0.0.1:9998) ==="
pkill -f "socat.*9998" 2>/dev/null || true
sleep 1
nohup socat TCP-LISTEN:9998,bind=${TM_IP},reuseaddr,fork TCP:127.0.0.1:9998 \
    > /tmp/socat-metrics.log 2>&1 &
echo "socat PID $!"

echo "=== [3/3] Verify ==="
sleep 3
ss -tlnp | grep 9998 && echo "OK: socat relay running" || echo "WARN: socat not listening"
