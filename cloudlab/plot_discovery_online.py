#!/usr/bin/env python3
"""Cumulative high-IP discovery over wall-clock hours: MoStream (online) vs Colmena vs static
vs random. Reads insearch_discovery_results.json (per-seed cumulative-discovery curves) and
plots the mean across seeds. Matches the paper figure style (serif, brand palette, one distinct
line style per arm so the figure survives grayscale). Threshold = top decile of the real
search-space xTB distribution."""
import json, os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import os as _os
REPO = _os.environ.get("MOSTREAM_REPO") or _os.path.abspath(
    _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))
SCR = _os.environ.get("MOSTREAM_DISCOVERY_DIR") or _os.path.join(REPO, "results", "discovery")
OUT_REPO = os.path.join(REPO, "paper", "Figures")
J = os.path.join(SCR, "insearch_discovery_results.json")

BLUE, RED, GREY, INK, INK2, MUTED = '#2a78d6', '#e34948', '#8a8985', '#0b0b0b', '#52514e', '#b7b6b3'
plt.rcParams.update({
    'font.size': 8, 'axes.labelsize': 8, 'axes.titlesize': 8,
    'xtick.labelsize': 8, 'ytick.labelsize': 8, 'legend.fontsize': 8,
    'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.edgecolor': MUTED, 'axes.labelcolor': INK, 'text.color': INK,
    'xtick.color': INK, 'ytick.color': INK, 'lines.linewidth': 1.4,
    'legend.frameon': False, 'figure.dpi': 200, 'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02,
})

d = json.load(open(J))
cfg = d['config']; curves = d['curves']
seeds = sorted(curves.keys(), key=int)
HRS = cfg['HRS_PER_EVAL']; B = cfg['BATCH']; THR = cfg['HIGH_THR']
L = min(len(curves[s]['online']) for s in seeds)                 # align seed lengths
x = np.arange(L) * B * HRS                                        # elapsed hours

def band(method):
    M = np.array([curves[s][method][:L] for s in seeds], float)
    return M.mean(0), M.min(0), M.max(0)

fig, ax = plt.subplots(figsize=(3.3, 2.15))
ax.grid(True, color='0.85', linewidth=0.5)
ax.set_axisbelow(True)

# One distinct line style per arm: solid / dashed / dash-dot / dotted, so colour is never the
# only cue. Colmena is the red dashed line.
series = [('online',  BLUE, '-',            'online',  1.8),
          ('colmena', RED,  (0, (4, 2)),    'Colmena', 1.5),
          ('static',  INK2, (0, (5, 1.5, 1, 1.5)), 'static', 1.4),
          ('random',  GREY, (0, (1, 2)),    'random',  1.3)]
series = [s for s in series if s[0] in curves[seeds[0]]]
for key, col, ls, lab, lw in series:
    ax.plot(x, band(key)[0], color=col, ls=ls, lw=lw, label=lab)

# 6-hour campaign marker
ax.axvline(6.0, color=INK2, lw=0.7, ls=(0, (1, 2)), alpha=0.6)
ax.text(6.0, 140 * 0.03, ' 6 h', color=INK2, fontsize=8, va='bottom', ha='left')

ax.set_xlabel('Elapsed time (h)')
ax.set_ylabel('Molecules found, IP $\\geq$ %.1f V' % THR)
ax.set_xlim(0, 12); ax.set_xticks(np.arange(0, 13, 2))
ax.set_ylim(0, 140); ax.set_yticks(np.arange(0, 141, 20))
ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1.0), ncol=1,
          handlelength=1.6, columnspacing=1.0, borderaxespad=0.2, fontsize=8)

os.makedirs(OUT_REPO, exist_ok=True)
for base in (os.path.join(SCR, 'insearch_discovery'), os.path.join(OUT_REPO, 'fig_discovery_online')):
    for e in ('pdf', 'png'):
        fig.savefig(f'{base}.{e}')

keys = [k for k, *_ in series]
print(f"wrote {OUT_REPO}/fig_discovery_online.pdf/.png  (and scratchpad/insearch_discovery.pdf/.png)")
print(f"threshold={THR:.2f}V  seeds={seeds}  aligned_rounds={L-1}  span={x[-1]:.1f}h")
print("FINAL mean discoveries -> " + "  ".join(f"{k}={band(k)[0][-1]:.1f}" for k in keys))
i6 = int(np.argmin(np.abs(x - 6.0)))
print(f"AT 6h (~{i6*B} evals) -> " + "  ".join(f"{k}={band(k)[0][i6]:.1f}" for k in keys))
