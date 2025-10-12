#!/bin/bash

ssh jliu1721@node-3 "
exp_dir=\$(date '+%Y-%m%d-%H%M%S')
mkdir -p /mnt/media/monitor/exp-globus-moleculer-design/Streaming/\$exp_dir
cd /mnt/media/monitor/exp-globus-moleculer-design/Streaming/\$exp_dir

starttime=`date +%Y%m%d%H%M%S`
echo startTime=\$starttime >> Experiments_timestamp.log

collectl -i 0.05 -F1200 -scmdn -oTm  -f /tmp > /dev/null &
"

python simulator.py &

python compute_ip_controller.py &
