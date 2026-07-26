#!/usr/bin/env python3
"""E3 main figure: throughput vs parallelism under a cores-matched TF-thread policy.

Measured on the live cluster 2026-07-22/23 (saturated backlog replay, counter-delta throughput,
same method as make_figures._e3_throughput). Each point is 3 reps; we plot the MEDIAN (robust to
the cold-start / warm-up outlier seen in the reps) with min-max whiskers.

  matched: TF intra-op threads = 32/P per worker, so total threads = 32 cores at every P
           (no oversubscription).  P1 intra32, P2 intra16, P4 intra8, P8 intra4.
  default: TF intra-op threads = 32 (TensorFlow's own default = core count). At P8 that is
           8 workers x 32 = 256 threads on 32 cores -> oversubscription halves throughput.

Per-rep candidates/s (max-vertex, the Infer amplifier):
  matched  P1 [61,61,57]  P2 [135,125,256]  P4 [299,281,274]  P8 [365,707,701]
  default  P8 [373,325,353]
"""
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
OUT = 'paper/Figures'; COL = 3.3
plt.rcParams.update({
    'font.size': 8, 'axes.labelsize': 8, 'axes.titlesize': 8,
    'xtick.labelsize': 7, 'ytick.labelsize': 7, 'legend.fontsize': 6.6,
    'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.edgecolor': MUTED, 'axes.labelcolor': INK, 'text.color': INK,
    'xtick.color': INK2, 'ytick.color': INK2, 'lines.linewidth': 1.4,
    'legend.frameon': False, 'figure.dpi': 200, 'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02,
})

matched = {1: [61, 61, 57], 2: [135, 125, 256], 4: [299, 281, 274], 8: [365, 707, 701]}
default = {1: [61, 61, 57], 2: [131, 128, 173], 4: [241, 270, 220], 8: [373, 325, 353]}


def stats(dat):
    P = np.array(sorted(dat))
    med = np.array([np.median(dat[p]) for p in P])
    lo = np.array([med[i] - min(dat[p]) for i, p in enumerate(P)])
    hi = np.array([max(dat[p]) - med[i] for i, p in enumerate(P)])
    return P, med, lo, hi


P, med, lo, hi = stats(matched)
Pd, medd, lod, hid = stats(default)

fig, ax = plt.subplots(figsize=(COL, 2.15))

# naive default (TensorFlow's own thread default = core count per worker)
# Medians only, no min-max whiskers: the spread is dominated by a single warm-up-scale
# outlier rep (e.g. P=2 matched = [135,125,256]) and the median is the reported value.
ax.plot(Pd, medd, '-s', color=RED, ms=4.5, lw=1.4, mec='white', mew=0.6, alpha=0.9,
        label='default threads (oversubscribed)', zorder=3)

# matched (tuned) curve
ax.plot(P, med, '-o', color=BLUE, ms=5, lw=1.7, mec='white', mew=0.6,
        label='threads matched to cores', zorder=4)

ax.annotate(f'{med[-1]:.0f}/s', xy=(8, med[-1]), xytext=(8, med[-1] + 45),
            fontsize=7, color=BLUE, ha='center', va='bottom')
ax.annotate(f'{medd[-1]:.0f}/s', xy=(8, medd[-1]), xytext=(8, medd[-1] - 48),
            fontsize=7, color=RED, ha='center', va='top')

ax.set_xscale('log', base=2); ax.set_yscale('log')
ax.set_xticks(P); ax.set_xticklabels([str(p) for p in P])
ax.set_yticks([50, 100, 200, 400, 800]); ax.set_yticklabels(['50', '100', '200', '400', '800'])
ax.set_xlabel('operator parallelism $P$')
ax.set_ylabel('scored candidates / s')
ax.set_xlim(0.9, 9.5); ax.set_ylim(45, 950)
ax.legend(loc='upper left', handlelength=1.7, borderaxespad=0.3)

import os
os.makedirs(OUT, exist_ok=True)
for e in ('pdf', 'png'):
    fig.savefig(f'{OUT}/fig_scaling_tf.{e}')
print(f'  wrote {OUT}/fig_scaling_tf.pdf')
print(f'  matched median: {dict(zip(P.tolist(), med.tolist()))}')
print(f'  P8 matched {med[-1]:.0f} vs default {medd[-1]:.0f} = {med[-1]/medd[-1]:.2f}x')
