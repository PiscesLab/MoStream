#!/usr/bin/env python3
"""E7 -- worker recycling bounds the footprint (the measured solution to fig:footprint).

Reads results/e7_mem.csv (t, n_workers, total_mb, event) and plots total Python-worker memory
over time. Without recycling the footprint grows unbounded (the +0.81 GB/h of fig:footprint,
shown as the dashed envelope); each induced recycle reclaims it back to baseline, so the trace is
a BOUNDED SAWTOOTH capped near the per-interval peak. Weight persistence (E6) makes each recycle
non-destructive to training.
"""
import argparse, csv, os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
OUT = 'paper/Figures'; COL = 3.3


def load(path):
    t, mb, ev = [], [], []
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                t.append(float(r['t'])); mb.append(float(r['total_mb'])); ev.append(r.get('event', ''))
            except ValueError:
                continue
    return np.array(t), np.array(mb), ev


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="results/e7_mem.csv")
    ap.add_argument("--process-size-gb", type=float, default=9.856, help="engine process.size")
    ap.add_argument("--leak-gbph", type=float, default=0.81, help="unbounded growth rate")
    ap.add_argument("--name", default="fig_recycle")
    args = ap.parse_args()
    plt.rcParams.update({
        'font.size': 8, 'axes.labelsize': 8, 'xtick.labelsize': 7, 'ytick.labelsize': 7,
        'legend.fontsize': 6.6, 'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
        'axes.spines.top': False, 'axes.spines.right': False, 'axes.edgecolor': MUTED,
        'axes.labelcolor': INK, 'text.color': INK, 'xtick.color': INK2, 'ytick.color': INK2,
        'lines.linewidth': 1.4, 'legend.frameon': False, 'figure.dpi': 200,
        'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02})

    t, mb, ev = load(args.csv)
    tm = t / 60.0; gb = mb / 1024.0
    recycles = [t[i]/60.0 for i, e in enumerate(ev) if e and 'recycle' in e]
    peak = gb.max(); plateau = 17.0

    fig, ax = plt.subplots(figsize=(COL, 1.95))
    # un-recycled reference: the 17 GB plateau of fig:footprint, then +0.81 GB/h (unbounded)
    ax.plot(tm, plateau + args.leak_gbph * (tm / 60.0), '--', color=RED, lw=1.2, alpha=0.85,
            label='un-recycled (Fig. 2): $+0.81$ GB/h')
    ax.plot(tm, gb, '-', color=BLUE, lw=1.5, label='recycled (measured)')
    for rc in recycles:
        ax.axvline(rc, color=MUTED, lw=0.7, ls=':')
    if recycles:
        ax.annotate('recycle', xy=(recycles[0], peak), xytext=(recycles[0]+0.6, peak+2.2),
                    fontsize=6.3, color=INK2, arrowprops=dict(arrowstyle='->', color=INK2, lw=0.7))
    ax.axhline(args.process_size_gb, color=INK2, lw=0.9, ls='-.')
    ax.text(tm.max(), args.process_size_gb-0.3, "engine process.size", color=INK2,
            fontsize=6.0, ha='right', va='top', style='italic')
    ax.set_xlabel('time (min)'); ax.set_ylabel('Python-worker memory (GB)')
    ax.set_xlim(0, tm.max()); ax.set_ylim(0, plateau * 1.28)
    ax.legend(loc='lower right', handlelength=1.7)
    os.makedirs(OUT, exist_ok=True)
    for extn in ('pdf', 'png'):
        fig.savefig(f'{OUT}/{args.name}.{extn}')
    print(f'  wrote {OUT}/{args.name}.pdf  (peak={peak:.1f} GB, {len(recycles)} recycles, min={gb.min():.1f} GB)')


if __name__ == "__main__":
    main()
