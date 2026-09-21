#!/usr/bin/env python3
"""Turn a baseline campaign into the discovery curve plot_discovery.py reads.

Two input shapes, because there are two ways to run the baseline.

Upstream Colmena, which writes a runs/<timestamp>/ directory:

    python cloudlab/colmena_baseline/make_curve.py \
        --upstream ~/colmena-upstream/molecular-design/runs/<timestamp> \
        --out results/campaign/colmena.csv

The local harness, which writes an oracle log in the simulator's format:

    python cloudlab/colmena_baseline/make_curve.py \
        --log results/colmena/oracle_colmena.log \
        --out results/campaign/colmena.csv

Either way a molecule counts once, at its first verification, and the curve steps the
cumulative number whose oracle ionization potential clears the hit threshold. That is
the same rule the streaming arm's curve uses, so the two are comparable.

THE CLOCK STARTS AT LAUNCH, not at the first result. A system that spends its first
hour training and scoring before simulating anything has to show that hour on the
curve, because that delay is the thing being compared. For upstream runs the launch
time is the first timestamp in runtime.log; pass --t0 to set it explicitly. Starting
from the first result instead would erase the startup cost from the figure.
"""
import argparse
import csv
import os
import re

HIT = 14.0
LINE = re.compile(r'^(?P<ts>\d+)\s+\[oracle\]\s+(?P<smiles>\S+)\s+est_ip=\S+\s+->\s+xtb_ip=(?P<ip>[-\d.]+)')


def read_oracle_log(path):
    """Our harness: one '[oracle]' line per molecule, the simulator's format."""
    rows, seen = [], set()
    for line in open(path):
        m = LINE.match(line)
        if not m:
            continue
        smiles = m.group('smiles')
        if smiles in seen:
            continue
        seen.add(smiles)
        rows.append((int(m.group('ts')), float(m.group('ip'))))
    return rows


def upstream_launch_epoch(run_dir):
    """Launch time of an upstream run: the first timestamp in its runtime.log.

    Python logging writes local wall-clock, and parsing it as naive local time gives a
    true epoch, the same base as the moldata-records timestamps on this machine.
    """
    from datetime import datetime
    with open(os.path.join(run_dir, 'runtime.log')) as f:
        first = f.readline()
    return datetime.strptime(first[:23], '%Y-%m-%d %H:%M:%S,%f').timestamp()


def read_upstream(run_dir, potential='xtb-vacuum'):
    """Upstream Colmena: moldata-records.json, one [timestamp, MoleculeData] per line.

    Each line is written when a molecule's calculation completes and the recipes have
    been applied, so the timestamp is its verification time and oxidation_potential
    carries the value under the requested level of theory.
    """
    import json as _json
    path = os.path.join(run_dir, 'moldata-records.json')
    if not os.path.exists(path):
        raise SystemExit(f'no moldata-records.json in {run_dir}')
    rows, seen = [], set()
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            ts, blob = _json.loads(line)
            data = _json.loads(blob) if isinstance(blob, str) else blob
        except Exception:
            continue
        smiles = data.get('identifier', {}).get('smiles') or data.get('smiles')
        pot = data.get('oxidation_potential') or {}
        if smiles is None or potential not in pot:
            continue
        if smiles in seen:
            continue
        seen.add(smiles)
        rows.append((int(float(ts)), float(pot[potential])))
    return rows


def main():
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument('--log', help="oracle log written by this repo's harness")
    src.add_argument('--upstream', help='an upstream Colmena runs/<timestamp> directory')
    ap.add_argument('--out', default='results/campaign/colmena.csv')
    ap.add_argument('--hit', type=float, default=HIT)
    ap.add_argument('--potential', default='xtb-vacuum',
                    help='which oxidation potential to read from upstream records')
    ap.add_argument('--t0', type=float, default=None,
                    help='launch time as a unix epoch; the curve is measured from here')
    ap.add_argument('--window-h', type=float, default=None,
                    help='drop molecules verified after this many hours from launch')
    args = ap.parse_args()

    if args.upstream:
        rows = read_upstream(args.upstream, args.potential)
        source = args.upstream
    else:
        rows = read_oracle_log(args.log)
        source = args.log

    if not rows:
        raise SystemExit(f'no molecules parsed from {source}')
    rows.sort()
    if args.t0 is not None:
        t0 = args.t0
    elif args.upstream:
        t0 = upstream_launch_epoch(args.upstream)
    else:
        t0 = rows[0][0]
        print('  warning: no --t0 given, so the clock starts at the first result and any '
              'startup cost is invisible on this curve')
    if args.window_h is not None:
        rows = [r for r in rows if (r[0] - t0) / 3600.0 <= args.window_h]

    os.makedirs(os.path.dirname(args.out) or '.', exist_ok=True)
    cumulative = 0
    with open(args.out, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['hours', 'cumulative'])
        w.writerow(['0.000000', 0])          # the launch, before anything was verified
        for ts, ip in rows:
            if ip > args.hit:
                cumulative += 1
            w.writerow([f'{(ts - t0) / 3600.0:.6f}', cumulative])
    first_h = (rows[0][0] - t0) / 3600.0
    print(f'  wrote {args.out}: {len(rows)} molecules, {cumulative} hits above {args.hit} V, '
          f'first result at +{first_h:.3f} h from launch')


if __name__ == '__main__':
    main()
