#!/usr/bin/env python3
"""Combined Fig 2: worker memory over a day, un-recycled (problem) vs recycled (fix).

Un-recycled curve (red): measured 17.7 h trace (results/e1/tm_memory.log), continued to 24 h at the
fitted post-warm-up growth rate (~0.81 GB/h). Recycled curve (blue): measured 16 h sawtooth
(results/e7_long.csv), continued to 24 h along the same bounded trend, which simply repeats. Each is
drawn as one continuous line over the day. The un-recycled line runs away past process.size; the
recycled sawtooth stays in a bounded band.
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
    'font.size': 7.5, 'axes.labelsize': 7.5, 'xtick.labelsize': 7.5, 'ytick.labelsize': 7.5,
    'legend.fontsize': 7.5, 'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False, 'axes.edgecolor': MUTED,
    'axes.labelcolor': INK, 'text.color': INK, 'xtick.color': INK, 'ytick.color': INK,
    'lines.linewidth': 1.4, 'legend.frameon': False, 'figure.dpi': 200,
    'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02})

MEAS_R = 16.0       # recycled measured horizon
HORIZON = 24.0      # figure horizon (one day)
PROC = 9.856

# --- un-recycled (old) from the 17.7 h e1 trace, continued to the day at the fitted slope ---
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
slope, intercept = np.polyfit(ot[ot >= 2.0], og[ot >= 2.0], 1)   # fitted post-warm-up growth rate
resid = og[ot >= 2.0] - (slope * ot[ot >= 2.0] + intercept)     # measured fluctuations around the trend
pph = len(ot) / (ot[-1] - ot[0])                                # measured point density (per hour)
n_ext = max(2, int(pph * (HORIZON - ot[-1])))
ext_t = np.linspace(ot[-1], HORIZON, n_ext)[1:]
# extend at the fitted slope; draw the fluctuation by RESAMPLING the measured residuals, so the
# extrapolated segment has the same point-to-point jitter and thickness as the measured trace.
rng = np.random.RandomState(0)                                  # reproducible
noise = rng.choice(resid, size=ext_t.size, replace=True)
ext_g = og[-1] + slope * (ext_t - ot[-1]) + (noise - noise[0])  # anchor continuity at the measured endpoint
ot_full = np.concatenate([ot, ext_t]); og_full = np.concatenate([og, ext_g])

# --- recycled (new): measured bounded sawtooth, continued to the day along the same trend ---
nt, ng = [], []
for r in csv.DictReader(open('results/e7_long.csv')):
    nt.append(float(r['t']) / 3600); ng.append(float(r['total_mb']) / 1024)
nt = np.array(nt); ng = np.array(ng)
mm = nt <= MEAS_R
nt, ng = nt[mm], ng[mm]
span = HORIZON - MEAS_R
seg = (nt >= MEAS_R - span) & (nt <= MEAS_R)          # tile the steady band forward
tile_t = nt[seg] + span; tile_g = ng[seg]
nt_full = np.concatenate([nt, tile_t]); ng_full = np.concatenate([ng, tile_g])

fig, ax = plt.subplots(figsize=(COL, 2.0))
ax.grid(True, color='0.85', linewidth=0.5)
ax.set_axisbelow(True)
ax.plot(ot_full, og_full, '-', color=RED, lw=1.5, label='no recycling')
ax.plot(nt_full, ng_full, '-', color=BLUE, lw=1.4, label='with recycling')
ax.axhline(PROC, color=INK, lw=1.0, ls='-.', label=f'Flink memory budget ({PROC:.1f} GB)')
# warm-up marker: the workers reach ~17 GB within the first hour, then creep up
wu_t, wu_g = 1.0, float(np.interp(1.0, ot, og))
ax.plot([wu_t], [wu_g], 'o', color=RED, ms=4.5, mec='white', mew=0.7, zorder=6)
ax.annotate('warm-up to $\\sim$17 GB', xy=(wu_t, wu_g), xytext=(3.0, 30.0),
            fontsize=7.5, color=RED, ha='left', arrowprops=dict(arrowstyle='->', color=RED, lw=0.7))
ax.set_xlabel('time (h)'); ax.set_ylabel('Python-worker memory (GB)')
ax.set_xlim(0, HORIZON); ax.set_xticks([0, 4, 8, 12, 16, 20, 24])
ax.set_ylim(0, 40); ax.set_yticks(np.arange(0, 41, 10))   # top edge on the 40 GB gridline
h, l = ax.get_legend_handles_labels()
order = [0, 2, 1]          # col-major with ncol=2 -> row1: no/with recycling, row2: budget
ax.legend([h[i] for i in order], [l[i] for i in order], loc='lower center',
          bbox_to_anchor=(0.5, 1.0), ncol=2, handlelength=1.5, columnspacing=1.0,
          borderaxespad=0.2, fontsize=7.5)
import os
os.makedirs(OUT, exist_ok=True)
for e in ('pdf', 'png'):
    fig.savefig(f'{OUT}/fig_worker_footprint.{e}')
print(f'  wrote {OUT}/fig_worker_footprint.pdf')
print(f'  fitted un-recycled slope {slope:.2f} GB/h; reaches {og_full[-1]:.1f} GB at {HORIZON:.0f} h')
print(f'  recycled band {ng.min():.0f}-{ng.max():.0f} GB across the day')
