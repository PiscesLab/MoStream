#!/usr/bin/env python3
"""Turn the baseline's oracle log into the discovery curve plot_discovery.py reads.

    python cloudlab/colmena_baseline/make_curve.py \
        --log results/colmena/oracle_colmena.log \
        --out results/campaign/colmena.csv

Counts a molecule once, at its first verification, and steps the cumulative number
whose oracle ionization potential clears the hit threshold. Same rule the streaming
arm's curve uses, so the two are comparable.
"""
import argparse
import csv
import os
import re

HIT = 14.0
LINE = re.compile(r'^(?P<ts>\d+)\s+\[oracle\]\s+(?P<smiles>\S+)\s+est_ip=\S+\s+->\s+xtb_ip=(?P<ip>[-\d.]+)')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--log', default='results/colmena/oracle_colmena.log')
    ap.add_argument('--out', default='results/campaign/colmena.csv')
    ap.add_argument('--hit', type=float, default=HIT)
    args = ap.parse_args()

    rows = []
    seen = set()
    for line in open(args.log):
        m = LINE.match(line)
        if not m:
            continue
        smiles = m.group('smiles')
        if smiles in seen:
            continue
        seen.add(smiles)
        rows.append((int(m.group('ts')), float(m.group('ip'))))

    if not rows:
        raise SystemExit(f'no oracle lines parsed from {args.log}')
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
