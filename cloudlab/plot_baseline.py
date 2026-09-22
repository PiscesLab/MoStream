#!/usr/bin/env python3
"""Live head-to-head discovery: the streaming system against Colmena, same host.

Every arm is read from the same kind of file, a launch-relative curve written by
cloudlab/colmena_baseline/make_curve.py, so all arms are measured identically:

  results/campaign/streaming.csv   the streaming system
  results/campaign/colmena.csv     upstream Colmena
  results/campaign/random.csv      uniform picks from the search space, no model

each with columns `hours,cumulative`, where hours counts from LAUNCH rather than from
the first result. A system that spends its first hour training and scoring before it
simulates anything has to show that hour here, because that delay is what the
comparison measures. An arm whose file is missing is left out, never approximated.

Run from the repository root in the mostream environment:

    python cloudlab/plot_baseline.py
"""
import argparse
import csv
import math
import os

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
OUT = 'paper/Figures'; COL = 3.3
plt.rcParams.update({
    'font.size': 8, 'axes.labelsize': 8, 'axes.titlesize': 8,
    'xtick.labelsize': 8, 'ytick.labelsize': 8, 'legend.fontsize': 8,
    'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.edgecolor': MUTED, 'axes.labelcolor': INK, 'text.color': INK,
    'xtick.color': INK, 'ytick.color': INK, 'lines.linewidth': 1.4,
    'legend.frameon': False, 'figure.dpi': 200, 'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02,
})

# (file, legend label, colour, line style). Grayscale-safe: identity never rests on hue.
ARMS = [
    ('streaming.csv', 'streaming', BLUE, '-'),
    ('colmena.csv', 'Colmena', RED, '--'),
    ('random.csv', 'random', MUTED, ':'),
]


def read_curve(path):
    hours, cum = [], []
    with open(path) as f:
        for row in csv.DictReader(f):
            hours.append(float(row['hours']))
            cum.append(int(row['cumulative']))
    return np.array(hours), np.array(cum)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', default='results/campaign')
    ap.add_argument('--window-h', type=float, default=6.0)
    ap.add_argument('--hit', type=float, default=14.0, help='only used in the axis label')
    ap.add_argument('--name', default='fig_baseline')
    args = ap.parse_args()

    fig, ax = plt.subplots(figsize=(COL, 2.1))
    ax.grid(True, color='0.85', linewidth=0.5)
    ax.set_axisbelow(True)

    ymax_seen, plotted = 0, []
    for fname, label, colour, style in ARMS:
        path = os.path.join(args.dir, fname)
        if not os.path.exists(path):
            print(f'  [skip] {label}: no {path}')
            continue
        h, c = read_curve(path)
        keep = h <= args.window_h
        h, c = h[keep], c[keep]
        # hold the last value to the end of the window, so a curve does not appear to
        # stop early just because its final molecule landed before the window closed
        h = np.append(h, args.window_h)
        c = np.append(c, c[-1] if len(c) else 0)
        ax.step(h, c, where='post', color=colour, ls=style, lw=1.5, label=label)
        ymax_seen = max(ymax_seen, int(c.max()))
        plotted.append((label, int(c[-1])))
        print(f'  {label:10s} {int(c[-1]):4d} hits by {args.window_h:g} h')

    if not plotted:
        raise SystemExit('no arms to plot')

    ystep = 20 if ymax_seen > 60 else 10
    ymax = max(ystep, math.ceil(ymax_seen / ystep) * ystep)
    ax.set_xlim(0, args.window_h); ax.set_xticks(np.arange(0, args.window_h + 1e-9, 1))
    ax.set_ylim(0, ymax); ax.set_yticks(np.arange(0, ymax + 1e-9, ystep))
    ax.set_xlabel('Elapsed time (h)')
    ax.set_ylabel(f'Molecules found, IP > {args.hit:g} V')
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1.0), ncol=1,
              handlelength=1.6, columnspacing=1.0, borderaxespad=0.2, fontsize=8)

    os.makedirs(OUT, exist_ok=True)
    for ext in ('pdf', 'png'):
        fig.savefig(f'{OUT}/{args.name}.{ext}')
    print(f'  wrote {OUT}/{args.name}.pdf')


if __name__ == '__main__':
    main()
