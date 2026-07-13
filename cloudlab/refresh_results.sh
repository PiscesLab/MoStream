#!/bin/bash
# Pull every experiment artifact off the cluster, rebuild the figures, and print a status
# line. One command, so monitoring is cheap and the paper's figures never go stale.
#
#   bash cloudlab/refresh_results.sh
#
# Reads node addresses from cloudlab/cluster.conf.
set -u
cd "$(dirname "$0")/.." || exit 1
source cloudlab/cluster.conf

S="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15"
U="${SSH_USER:-NamSDSU}"
JM="$U@$JM_HOST"; TM="$U@$TM_HOST"; KA="$U@$KAFKA_HOST"

mkdir -p results/e1 results/profile results/e0

echo "=== pulling artifacts ==="
timeout 90 scp -q $S "$TM:~/tm_memory.log" results/e1/tm_memory.log 2>/dev/null \
  && echo "  memory trace   : $(wc -l < results/e1/tm_memory.log) samples"
timeout 150 ssh $S "$TM" \
  "grep -h 'TRAINPROF subtask=[0-9]\|INFERPROF chunk=[0-9]' ~/flink/log/*taskexecutor*.log 2>/dev/null | grep -v print" \
  > results/profile/prof.txt 2>/dev/null \
  && echo "  cost profile   : $(wc -l < results/profile/prof.txt) samples"
timeout 90 scp -q $S "$JM:~/results/e0_tuned.csv" results/e0/e0_tuned.csv 2>/dev/null \
  && echo "  E0 latency     : $(( $(wc -l < results/e0/e0_tuned.csv) - 1 )) samples"

echo
echo "=== job status ==="
JID=$(timeout 40 ssh $S "$JM" "curl -s http://${JM_IP}:8081/jobs" 2>/dev/null \
      | grep -o '"id":"[^"]*","status":"RUNNING"' | cut -d'"' -f4 | head -1)
if [ -z "$JID" ]; then
    echo "  NO RUNNING JOB"
else
    timeout 40 ssh $S "$JM" "curl -s http://${JM_IP}:8081/jobs/$JID" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
v = d['vertices']
src = v[0]['metrics'].get('read-records', 0)
trn = v[1]['metrics'].get('read-records', 0)
rec = v[3]['metrics'].get('write-records', 0)
up  = d['duration'] // 1000
print(f\"  job {d['jid'][:8]}  {d['state']}  up {up//3600}h{(up%3600)//60:02d}m\")
print(f\"  source={src}  train={trn}  backlog={src-trn}  recommendations={rec}\")
warm = min(trn, 256)
print('  warm-up: ' + ('DONE' if trn >= 256 else f'{warm}/256 ({100*warm/256:.0f}%)'))
"
    n=$(timeout 40 ssh $S "$JM" "curl -s http://${JM_IP}:8081/jobs/$JID/exceptions" 2>/dev/null | grep -c exceptionName)
    echo "  restarts: $n"
fi

echo
echo "=== rebuilding figures ==="
source ~/miniconda3/etc/profile.d/conda.sh 2>/dev/null && conda activate mostream 2>/dev/null
python cloudlab/make_figures.py 2>&1 | grep -vi warning

echo
echo "=== unmeasured values still in the paper ==="
grep -o '\\NUM{[^}]*}' paper/workshop26-latest.tex 2>/dev/null | sort -u | sed 's/^/  /'
