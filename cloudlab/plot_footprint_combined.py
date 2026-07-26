#!/usr/bin/env python3
"""Combined Fig 2: worker memory over ~1 day, un-recycled (problem) vs recycled (fix).

Un-recycled curve: measured 17.7 h trace (results/e1/tm_memory.log), extrapolated to 24 h at the
fitted +0.81 GB/h. Recycled curve: measured 4 h run (results/e7_new_4h.csv), tiled to 24 h since the
bounded sawtooth simply repeats. The un-recycled line runs away past process.size; the recycled
sawtooth stays in a bounded band.
"""
import csv, re
from datetime import datetime
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
OUT = 'paper/Figures'; COL = 3.3
plt.rcParams.update({
    'font.size': 8, 'axes.labelsize': 8, 'xtick.labelsize': 7, 'ytick.labelsize': 7,
    'legend.fontsize': 6.6, 'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False, 'axes.edgecolor': MUTED,
    'axes.labelcolor': INK, 'text.color': INK, 'xtick.color': INK2, 'ytick.color': INK2,
    'lines.linewidth': 1.4, 'legend.frameon': False, 'figure.dpi': 200,
    'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02})

HORIZON = 16.0; PROC = 9.856

# --- un-recycled (old) from the 17.7 h e1 trace ---
rx = re.compile(r'(?P<ts>[\d-]+ [\d:]+).*?py_workers=\d+x(?P<pm>\d+)MB')
old = []
for ln in open('results/e1/tm_memory.log'):
    m = rx.search(ln)
    if m:
        old.append((m.group('ts'), int(m.group('pm')) / 1024.0))
t0 = datetime.strptime(old[0][0], '%Y-%m-%d %H:%M:%S')
ot = np.array([(datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') - t0).total_seconds() / 3600 for ts, _ in old])
og = np.array([g for _, g in old])
keep = og > 0
ot, og = ot[keep], og[keep]
# clip the measured un-recycled trace to the horizon (the 17.7 h trace covers 15 h fully)
cm = ot <= HORIZON
otc, ogc = ot[cm], og[cm]

# --- recycled (new): measured across the full horizon, no tiling needed ---
nt, ng = [], []
for r in csv.DictReader(open('results/e7_long.csv')):
    nt.append(float(r['t']) / 3600); ng.append(float(r['total_mb']) / 1024)
nt = np.array(nt); ng = np.array(ng)
mm = nt <= HORIZON
nt, ng = nt[mm], ng[mm]
print(f'  recycled measured to {nt[-1]:.1f} h')

fig, ax = plt.subplots(figsize=(COL, 2.0))
# un-recycled (measured, clipped to the horizon)
ax.plot(otc, ogc, '-', color=RED, lw=1.5, label='no recycling')
ax.plot(nt, ng, '-', color=BLUE, lw=1.4, label='with recycling')
# engine memory budget -- labelled in the legend, not on the graph
ax.axhline(PROC, color=INK, lw=1.0, ls='-.', label=f'engine memory budget ({PROC:.1f} GB)')
# warm-up marker: the workers reach ~17 GB within the first hour, then creep up
wu_t, wu_g = 1.0, float(np.interp(1.0, otc, ogc))
ax.plot([wu_t], [wu_g], 'o', color=RED, ms=4.5, mec='white', mew=0.7, zorder=6)
ax.annotate('warm-up to $\\sim$17 GB', xy=(wu_t, wu_g), xytext=(3.0, 27.5),
            fontsize=6.6, color=RED, ha='left', arrowprops=dict(arrowstyle='->', color=RED, lw=0.7))
ax.set_xlabel('time (h)'); ax.set_ylabel('Python-worker memory (GB)')
ax.set_xlim(0, HORIZON); ax.set_ylim(0, max(ogc.max(), ng.max()) * 1.15)
ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1.0), ncol=2,
          handlelength=1.5, columnspacing=1.0, borderaxespad=0.2, fontsize=6.3)
import os
os.makedirs(OUT, exist_ok=True)
for e in ('pdf', 'png'):
    fig.savefig(f'{OUT}/fig_worker_footprint.{e}')
print(f'  wrote {OUT}/fig_worker_footprint.pdf')
print(f'  un-recycled max {ogc.max():.0f} GB at {HORIZON}h; recycled band {ng.min():.0f}-{ng.max():.0f} GB')
