#!/usr/bin/env python3
"""E0 -- steering latency.

Measures the wall-clock delay from a simulation result entering the workflow (the
`Simulation` topic) to the first recommendation whose ranking incorporates it (the
`Recommend` topic).

The Flink job stamps each recommendation with `src_ts:` (the producer timestamp of the
simulation result the model had just trained on) and `latency_ms:` (emit_ts - src_ts).
This script consumes `Recommend`, extracts those fields, and reports the distribution.

Because TrainFunction fits one record and then emits, a recommendation carrying src_ts=T
is causally downstream of the result produced at T -- the attribution is sound, not a
correlation.

Usage
-----
  # collect for 1 hour, then write CSV + stats + CDF
  python cloudlab/e0_steering_latency.py --bootstrap 10.10.1.3:9092 --duration 3600 \
      --out results/e0

  # re-analyse an existing CSV without touching Kafka
  python cloudlab/e0_steering_latency.py --replay results/e0.csv --out results/e0

Colmena's published figure for the same application is a mean steering latency of
57 minutes (3_420_000 ms), with 15% of simulations dispatched against a stale ranking
[Ward et al., MLHPC'21]. That is the number this experiment is measured against.
"""
import argparse
import csv
import os
import re
import sys
import time

COLMENA_MEAN_LATENCY_MS = 57 * 60 * 1000  # published baseline: 57 minutes

# "smiles: CCO ucb: 0.42 est_ip: 9.1 timestamp: 1700 src_ts: 1200 latency_ms: 500"
FIELD_RE = re.compile(r'(\w+):\s*(\S+)')

PLACEHOLDERS = ('model_not_ready', 'search_space_empty', 'mol_dicts_empty', 'inference_error')


def parse_line(text):
    """Return (smiles, emit_ts, src_ts, latency_ms) or None if not a real recommendation."""
    if not text or any(p in text for p in PLACEHOLDERS):
        return None
    parts = dict(FIELD_RE.findall(text))
    try:
        latency = int(parts['latency_ms'])
        src_ts = int(parts['src_ts'])
    except (KeyError, ValueError):
        return None
    if latency < 0 or src_ts <= 0:
        return None  # producer did not stamp the record; no attribution possible
    return parts.get('smiles'), int(parts['timestamp']), src_ts, latency


def collect(bootstrap, topic, duration, out_csv):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap],
        auto_offset_reset='latest',
        group_id=f'e0-latency-{int(time.time())}',
        consumer_timeout_ms=5000,
        api_version=(0, 10, 0),
    )
    deadline = time.time() + duration
    rows, skipped = [], 0
    print(f'[e0] consuming {topic} at {bootstrap} for {duration}s ...', flush=True)
    while time.time() < deadline:
        for msg in consumer:
            val = msg.value.decode('utf-8', 'replace')
            rec = parse_line(val)
            if rec is None:
                skipped += 1
            else:
                rows.append(rec)
                if len(rows) % 50 == 0:
                    print(f'[e0] {len(rows)} recommendations, '
                          f'last latency {rec[3]/1000:.1f}s', flush=True)
            if time.time() >= deadline:
                break
    consumer.close()

    os.makedirs(os.path.dirname(out_csv) or '.', exist_ok=True)
    with open(out_csv, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['smiles', 'emit_ts_ms', 'src_ts_ms', 'latency_ms'])
        w.writerows(rows)
    print(f'[e0] wrote {len(rows)} rows to {out_csv} ({skipped} placeholder/unstamped skipped)')
    return rows


def replay(path):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append((r['smiles'], int(r['emit_ts_ms']),
                         int(r['src_ts_ms']), int(r['latency_ms'])))
    return rows


def percentile(sorted_vals, q):
    if not sorted_vals:
        return float('nan')
    k = (len(sorted_vals) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def report(rows, out_prefix):
    if not rows:
        print('[e0] NO ATTRIBUTABLE RECOMMENDATIONS.\n'
              '     Check: (a) the simulator is stamping "timestamp" in its JSON payload,\n'
              '            (b) the job is the instrumented build (Rank emits src_ts/latency_ms),\n'
              '            (c) the model is past the 16-record warm-up (else all model_not_ready).',
              file=sys.stderr)
        return 1

    lat = sorted(r[3] for r in rows)
    n = len(lat)
    stats = {
        'n': n,
        'min_ms': lat[0],
        'p50_ms': percentile(lat, 0.50),
        'p95_ms': percentile(lat, 0.95),
        'p99_ms': percentile(lat, 0.99),
        'max_ms': lat[-1],
        'mean_ms': sum(lat) / n,
    }

    print('\n=== E0: steering latency ===')
    print(f'  recommendations attributed : {n}')
    for k in ('min_ms', 'p50_ms', 'mean_ms', 'p95_ms', 'p99_ms', 'max_ms'):
        print(f'  {k:<12}               : {stats[k]/1000:10.2f} s')
    speedup = COLMENA_MEAN_LATENCY_MS / stats['mean_ms'] if stats['mean_ms'] else float('inf')
    print(f'\n  Colmena published mean     : {COLMENA_MEAN_LATENCY_MS/1000:10.2f} s  (57 min)')
    print(f'  MoStream mean              : {stats["mean_ms"]/1000:10.2f} s')
    print(f'  reduction                  : {speedup:10.1f}x')
    print('\n  -> paper: replace \\NUM{E0} with the p50, and \\NUM{factor} with the reduction.')

    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print('\n[e0] matplotlib not available; stats printed, CDF skipped.')
        return 0

    fig, ax = plt.subplots(figsize=(5, 3.2))
    xs = [v / 1000.0 for v in lat]
    ys = [(i + 1) / n for i in range(n)]
    ax.step(xs, ys, where='post', lw=2, label='MoStream')
    ax.axvline(COLMENA_MEAN_LATENCY_MS / 1000.0, ls='--', lw=1.5, color='0.35',
               label='Colmena mean (57 min)')
    ax.set_xscale('log')
    ax.set_xlabel('Steering latency (s, log scale)')
    ax.set_ylabel('CDF')
    ax.set_ylim(0, 1.02)
    ax.grid(alpha=0.3, which='both')
    ax.legend(loc='lower right', frameon=False, fontsize=8)
    fig.tight_layout()
    for ext in ('pdf', 'png'):
        fig.savefig(f'{out_prefix}_cdf.{ext}', dpi=200)
    print(f'  wrote {out_prefix}_cdf.pdf / .png')
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bootstrap', default=os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092'))
    ap.add_argument('--topic', default='Recommend')
    ap.add_argument('--duration', type=int, default=3600, help='seconds to collect')
    ap.add_argument('--out', default='results/e0', help='output prefix')
    ap.add_argument('--replay', help='analyse an existing CSV instead of consuming Kafka')
    args = ap.parse_args()

    rows = replay(args.replay) if args.replay else collect(
        args.bootstrap, args.topic, args.duration, f'{args.out}.csv')
    return report(rows, args.out)


if __name__ == '__main__':
    sys.exit(main())
