# Measurement data

The measured inputs behind every figure in the paper. This branch is the code branch
plus this directory, so a checkout regenerates all eight figures with no other setup.

```bash
conda activate mostream
python cloudlab/plot_scaling_tf.py
python cloudlab/make_figures.py --only utilisation
python cloudlab/make_figures.py --only latency
python cloudlab/plot_latency_model.py
python cloudlab/plot_footprint_combined.py
python cloudlab/plot_systems_panels.py checkpoint
python cloudlab/plot_systems_panels.py recovery
python cloudlab/plot_discovery.py
```

Figures are written to `paper/Figures/` as PDF and PNG.

## What each file is

| File | Produced by | Feeds |
| --- | --- | --- |
| `e0/e0_tuned.csv` | steering-latency probe on the tuned configuration | Steering-latency CDF |
| `e1/tm_memory.log` | `monitor_taskmanager.sh` over a 24 hour run | Worker memory footprint |
| `e3/metrics_p{1,2,4,8}.csv` | `e3_run.sh`, one arm per parallelism, sampled from the Flink REST API | Operator utilization |
| `e4trace/trace.json` | `e4_recovery_trace.py`, sink-side throughput through a TaskManager SIGKILL | Fault recovery |
| `e7_long.csv` | `e7_recycle_memory.sh`, worker memory under periodic recycling | Worker memory footprint |
| `latsweep/summary.json` | `latency_sweep.py`, deterministic arrivals | Latency vs load |
| `latsweep_hard_summary.json` | `latency_sweep.py --reps`, Poisson arrivals with per-load error bars | Latency vs load |
| `campaign/oracle_*.log` | the three simulator nodes during the closed-loop campaign | End-to-end discovery |

Two figures need no data. The throughput and bounded-state panels carry their measured
constants in the plotting scripts, which record the per-rep values they came from.
