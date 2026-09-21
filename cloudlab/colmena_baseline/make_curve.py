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
    t0 = rows[0][0]

    os.makedirs(os.path.dirname(args.out) or '.', exist_ok=True)
    cumulative = 0
    with open(args.out, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['hours', 'cumulative'])
        for ts, ip in rows:
            if ip > args.hit:
                cumulative += 1
            w.writerow([f'{(ts - t0) / 3600.0:.6f}', cumulative])
    print(f'  wrote {args.out}: {len(rows)} molecules, {cumulative} hits above {args.hit} V')


if __name__ == '__main__':
    main()
