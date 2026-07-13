#!/usr/bin/env python3
"""Parse TRAINPROF / INFERPROF lines from the TaskManager log into a per-record cost breakdown.

The point of this script is to answer ONE question:

    Is the loop slow because of COMPUTE, or because of DATA MOVEMENT?

It matters because the answers point in opposite directions:

  * If the time is in model.fit() and model.predict(), the bottleneck is arithmetic. A
    faster machine helps, roughly linearly. The architecture is fine.

  * If the time is in json.dumps / json.loads / set_weights over a ~13.6 MB payload, the
    bottleneck is that we serialize and ship the ENTIRE MODEL on EVERY RECORD. No machine
    fixes that -- you would only be doing the same wasted work faster. It needs an
    architectural change: Infer should hold the model in its own keyed state and receive a
    version token, reloading weights only when they actually change.

Usage:
    ssh tm 'grep -h "TRAINPROF\\|INFERPROF" ~/flink/log/*taskexecutor*.log' > prof.txt
    python cloudlab/parse_profile.py prof.txt
"""
import re
import statistics
import sys

KV = re.compile(r'(\w+)=([-\d.]+)')


def parse(path):
    train, infer = [], []
    for line in open(path, errors='replace'):
        if 'TRAINPROF' in line:
            train.append({k: float(v) for k, v in KV.findall(line)})
        elif 'INFERPROF' in line:
            infer.append({k: float(v) for k, v in KV.findall(line)})
    return train, infer


def table(rows, fields, title, total_key):
    if not rows:
        print(f"  {title}: no samples")
        return None
    print(f"\n  {title}  (n={len(rows)})")
    print(f"    {'phase':<12} {'median':>9} {'mean':>9} {'max':>9}   {'% of total':>10}")
    print(f"    {'-'*12} {'-'*9} {'-'*9} {'-'*9}   {'-'*10}")
    tot = statistics.median([r.get(total_key, 0) for r in rows]) or 1e-9
    for f in fields:
        vals = [r[f] for r in rows if f in r]
        if not vals:
            continue
        med = statistics.median(vals)
        print(f"    {f:<12} {med:>8.2f}s {statistics.mean(vals):>8.2f}s "
              f"{max(vals):>8.2f}s   {100*med/tot:>9.1f}%")
    print(f"    {'TOTAL':<12} {tot:>8.2f}s")
    return tot


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else 'prof.txt'
    train, infer = parse(path)

    print("=" * 66)
    print("  PER-RECORD COST BREAKDOWN")
    print("=" * 66)

    t_tot = table(train, ['fit', 'tolist', 'dumps', 'persist'],
                  'TRAIN  (per record)', 'total')
    i_tot = table(infer, ['build', 'loads', 'setw', 'prep', 'predict'],
                  'INFER  (per record; scores 500 molecules)', 'work')

    if train:
        mb = statistics.median([r['payload_mb'] for r in train if 'payload_mb' in r] or [0])
        print(f"\n  serialized model payload: {mb:.1f} MB, shipped ONCE PER RECORD")

    # The verdict.
    print("\n" + "=" * 66)
    print("  VERDICT: compute-bound or data-movement-bound?")
    print("=" * 66)

    compute = movement = 0.0
    if train:
        compute += statistics.median([r.get('fit', 0) for r in train])
        movement += statistics.median([r.get('tolist', 0) + r.get('dumps', 0)
                                       + r.get('persist', 0) for r in train])
    if infer:
        compute += statistics.median([r.get('prep', 0) + r.get('predict', 0) for r in infer])
        movement += statistics.median([r.get('build', 0) + r.get('loads', 0)
                                       + r.get('setw', 0) for r in infer])

    total = compute + movement
    if total <= 0:
        print("  no samples yet -- the model is still warming up.")
        return 1

    print(f"    COMPUTE  (fit + predict + prep)          : {compute:6.2f}s  "
          f"{100*compute/total:5.1f}%")
    print(f"    MOVEMENT (serialize/ship/parse/set_wts)  : {movement:6.2f}s  "
          f"{100*movement/total:5.1f}%")
    print()
    if movement > compute:
        print("    -> DATA-MOVEMENT bound. A faster machine buys a LINEAR speedup on work")
        print("       that should not be happening at all. The fix is architectural: stop")
        print("       shipping the whole model on every record. Expected gain: order of")
        print("       magnitude, not a constant factor.")
    else:
        print("    -> COMPUTE bound. A faster machine (or GPU for fit/predict) helps")
        print("       roughly linearly, and the architecture is sound.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
