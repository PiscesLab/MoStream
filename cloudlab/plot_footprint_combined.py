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

HORIZON = 24.0; PROC = 9.856

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
# fit the post-warmup slope for extrapolation
fitmask = ot > 1
sl, ic = np.polyfit(ot[fitmask], og[fitmask], 1)
ext_t = np.linspace(ot[-1], HORIZON, 50)
ext_g = ic + sl * ext_t

# --- recycled (new) from the 4 h run, tiled to the horizon ---
nt, ng, nev = [], [], []
for r in csv.DictReader(open('results/e7_new_4h.csv')):
    nt.append(float(r['t']) / 3600); ng.append(float(r['total_mb']) / 1024)
nt = np.array(nt); ng = np.array(ng)
period = nt[-1]
# tile the measured sawtooth across the horizon
tile_t, tile_g = [], []
k = 0
while k * period < HORIZON:
    tile_t.append(nt + k * period); tile_g.append(ng); k += 1
tile_t = np.concatenate(tile_t); tile_g = np.concatenate(tile_g)
m = tile_t <= HORIZON
tile_t, tile_g = tile_t[m], tile_g[m]

fig, ax = plt.subplots(figsize=(COL, 2.0))
# un-recycled: measured + dashed extrapolation
ax.plot(ot, og, '-', color=RED, lw=1.5, label='no recycling (measured)')
ax.plot(ext_t, ext_g, '--', color=RED, lw=1.2, alpha=0.8)
ax.annotate(f'{ext_g[-1]:.0f} GB', xy=(HORIZON, ext_g[-1]), xytext=(HORIZON-0.3, ext_g[-1]),
            fontsize=6.4, color=RED, ha='right', va='bottom')
# recycled: measured 4 h solid, tiled extrapolation lighter
meas = tile_t <= period
ax.plot(tile_t[~meas], tile_g[~meas], '-', color=BLUE, lw=1.0, alpha=0.35)
ax.plot(tile_t[meas], tile_g[meas], '-', color=BLUE, lw=1.4, label='with recycling (measured 4 h, tiled)')
# process.size reference
ax.axhline(PROC, color=INK2, lw=0.9, ls='-.')
ax.text(HORIZON, PROC-0.6, 'engine process.size', color=INK2, fontsize=6.0, ha='right', va='top', style='italic')
ax.set_xlabel('time (h)'); ax.set_ylabel('Python-worker memory (GB)')
ax.set_xlim(0, HORIZON); ax.set_ylim(0, ext_g[-1] * 1.1)
ax.legend(loc='center left', handlelength=1.6)
import os
os.makedirs(OUT, exist_ok=True)
for e in ('pdf', 'png'):
    fig.savefig(f'{OUT}/fig_worker_footprint.{e}')
print(f'  wrote {OUT}/fig_worker_footprint.pdf')
print(f'  un-recycled slope {sl:.2f} GB/h -> {ext_g[-1]:.0f} GB at {HORIZON}h; recycled band '
      f'{tile_g.min():.0f}-{tile_g.max():.0f} GB')
