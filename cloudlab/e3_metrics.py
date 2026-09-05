#!/usr/bin/env python3
"""E3 -- parallelism sweep: sample Flink REST metrics over a measurement window.

Runs ON the JobManager (or anywhere that can reach the REST endpoint, default
10.10.1.4:8081). For the RUNNING job it samples, per operator subtask and per interval:

  busyTimeMsPerSecond, backPressuredTimeMsPerSecond, idleTimeMsPerSecond   (0..1000 ms/s)
  numRecordsInPerSecond, numRecordsOutPerSecond                            (rate gauges)
  numRecordsIn, numRecordsOut                                              (cumulative)

and writes a LONG-format CSV -- one row per (ts, vertex, subtask, metric) -- so
make_figures.fig_scaling / fig_utilisation can pivot however they need:

  ts,parallelism,vertex,subtask,metric,value

WHY these metrics. The utilisation triple (busy / backpressured / idle) is the venue's
expected scaling figure: a scalar "80% CPU" cannot say WHERE capacity was lost, a state
decomposition can (EXPERIMENTS.md E3). It shows the `window_all` fix directly -- Rank
backpressured at P=1, backpressure gone at P=8. The record rates give throughput
(sum of numRecordsOutPerSecond across a vertex's subtasks). The cumulative counters are
kept as a cross-check: (last - first) / elapsed is a rate that ignores sampling jitter.

Only the Python standard library -- no extra deps on the JobManager.

Usage
-----
  # sample the running job for 10 min at 15 s cadence, tagging it as the P=8 arm
  python cloudlab/e3_metrics.py --jm 10.10.1.4:8081 --parallelism 8 \
      --duration 600 --interval 15 --out results/e3/metrics_p8.csv

  # let e3_run.sh drive it per arm; you rarely call this by hand
"""
import argparse
import csv
import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

# Task/operator metrics we ask Flink for. Anything the vertex does not expose is simply
# absent from the response and skipped -- we never assume a metric is present.
METRICS = [
    'busyTimeMsPerSecond',
    'backPressuredTimeMsPerSecond',
    'idleTimeMsPerSecond',
    'numRecordsInPerSecond',
    'numRecordsOutPerSecond',
    'numRecordsIn',
    'numRecordsOut',
]


def _get(base, path, tries=3):
    """GET base+path, parse JSON, retry a few times (the REST server hiccups under load)."""
    url = base + path
    for i in range(tries):
        try:
            with urlopen(url, timeout=10) as r:
                return json.loads(r.read().decode('utf-8'))
        except (URLError, HTTPError, TimeoutError, json.JSONDecodeError) as e:
            if i == tries - 1:
                print(f'[e3] GET {path} failed: {e}', file=sys.stderr)
                return None
            time.sleep(1)
    return None


def running_job(base):
    """Return the id of the single RUNNING job, or None. We cancel between arms, so at
    measurement time there should be exactly one; if several are running, take the first
    and warn -- an ambiguous cluster is the operator's problem to resolve, not ours."""
    d = _get(base, '/jobs')
    if not d:
        return None
    running = [j['id'] for j in d.get('jobs', []) if j.get('status') == 'RUNNING']
    if len(running) > 1:
        print(f'[e3] WARNING: {len(running)} RUNNING jobs; sampling {running[0]}',
              file=sys.stderr)
    return running[0] if running else None


def vertices(base, jid):
    """[(vertex_id, name, parallelism)] for the job, in dataflow order."""
    d = _get(base, f'/jobs/{jid}')
    if not d:
        return []
    return [(v['id'], v.get('name', v['id']), int(v.get('parallelism', 1)))
            for v in d.get('vertices', [])]


def sample_subtask(base, jid, vid, idx):
    """{metric: value} for one subtask; missing metrics are simply not in the dict."""
    q = ','.join(METRICS)
    d = _get(base, f'/jobs/{jid}/vertices/{vid}/subtasks/{idx}/metrics?get={q}')
    if not d:
        return {}
    out = {}
    for m in d:
        try:
            out[m['id']] = float(m['value'])
        except (KeyError, ValueError, TypeError):
            pass  # a metric can read '' before the subtask has produced anything
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--jm', default=os.environ.get('FLINK_REST', '10.10.1.4:8081'),
                    help='JobManager REST host:port (default 10.10.1.4:8081)')
    ap.add_argument('--jid', help='job id (default: the one RUNNING job)')
    ap.add_argument('--parallelism', type=int, required=True,
                    help='arm label P, written verbatim into every row')
    ap.add_argument('--duration', type=int, default=600, help='seconds to sample')
    ap.add_argument('--interval', type=int, default=15, help='seconds between samples')
    ap.add_argument('--out', required=True, help='output CSV path')
    a = ap.parse_args()

    base = f'http://{a.jm}'
    jid = a.jid or running_job(base)
    if not jid:
        print('[e3] no RUNNING job found -- is the arm submitted and warmed up?',
              file=sys.stderr)
        return 1
    verts = vertices(base, jid)
    if not verts:
        print(f'[e3] job {jid} has no vertices', file=sys.stderr)
        return 1
    print(f'[e3] job {jid}: {len(verts)} operators, P={a.parallelism}, '
          f'sampling {a.duration}s @ {a.interval}s -> {a.out}', flush=True)
    for _, name, vp in verts:
        print(f'[e3]   op "{name[:60]}" parallelism={vp}')

    os.makedirs(os.path.dirname(a.out) or '.', exist_ok=True)
    with open(a.out, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['ts', 'parallelism', 'vertex', 'subtask', 'metric', 'value'])
        deadline = time.time() + a.duration
        n_rows = 0
        while time.time() < deadline:
            t0 = time.time()
            ts = int(t0)
            for vid, vname, vp in verts:
                for idx in range(vp):
                    for metric, val in sample_subtask(base, jid, vid, idx).items():
                        w.writerow([ts, a.parallelism, vname, idx, metric, val])
                        n_rows += 1
            f.flush()
            # keep a steady cadence even when a sampling pass itself took a while
            time.sleep(max(0.0, a.interval - (time.time() - t0)))
    print(f'[e3] wrote {n_rows} rows to {a.out}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
