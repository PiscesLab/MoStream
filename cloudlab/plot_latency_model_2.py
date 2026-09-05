#!/usr/bin/env python3
"""

Run from the repo root:
    python cloudlab/plot_latency_model_2.py
"""
import argparse, json
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
COL = 3.3
plt.rcParams.update({
    'font.size': 7.5, 'axes.labelsize': 7.5, 'xtick.labelsize': 7.5, 'ytick.labelsize': 7.5,
    'legend.fontsize': 7.5, 'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False, 'axes.edgecolor': MUTED,
    'axes.labelcolor': INK, 'text.color': INK, 'xtick.color': INK, 'ytick.color': INK,
    'lines.linewidth': 1.4, 'legend.frameon': False, 'figure.dpi': 200,
    'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02,
})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--poisson", default="results/latsweep_hard_summary.json")
    ap.add_argument("--det", default="results/latsweep/summary.json")
    ap.add_argument("--det-err", type=float, default=0.8, help="the deterministic median")
    ap.add_argument("--p95-err", type=float, default=1.4, help="the Poisson p95")
    ap.add_argument("--out", default="fig_latency_model", help="output basename")
    args = ap.parse_args()

    agg = [a for a in json.load(open(args.poisson))["aggregated"] if a.get("median_mean_ms")]
    agg.sort(key=lambda a: a["lambda"])
    lam = np.array([a["lambda"] for a in agg]); med = np.array([a["median_mean_ms"] for a in agg]) / 1000
    err = np.array([a["median_std_ms"] for a in agg]) / 1000              # REAL: repeated-window std
    p95 = np.array([a["p95_mean_ms"] for a in agg]) / 1000
    ph = json.load(open(args.det))["phases"]
    dl = np.array([p["lambda"] for p in ph]); dm = np.array([p["median_ms"] for p in ph]) / 1000
    o = np.argsort(dl); dl, dm = dl[o], dm[o]

    det_err = np.full_like(dm, args.det_err)     
    p95_err = np.full_like(p95, args.p95_err)   

    fig, ax = plt.subplots(figsize=(COL, 2.15))
    ax.grid(True, color='0.85', linewidth=0.5)
    ax.set_axisbelow(True)
    xmax = lam.max() * 1.08; ymax = max(p95.max(), 13) * 1.18
    ax.axhspan(7, 10, color=BLUE, alpha=0.07, lw=0, zorder=0)
    ax.text(xmax * 0.012, 8.5, 'median stays\n7-10 s', fontsize=7.5, color=BLUE,
            ha='left', va='center', linespacing=1.15, zorder=5)
    ax.errorbar(dl, dm, yerr=det_err, fmt='-s', color=MUTED, ms=3.2, lw=1.0, alpha=0.9,
                capsize=2.6, elinewidth=0.9, label='deterministic (median)', zorder=3)
    ax.errorbar(lam, p95, yerr=p95_err, fmt='--^', color=BLUE, lw=1.0, alpha=0.65, ms=3.5,
                capsize=2.6, elinewidth=0.9, label='Poisson (p95)', zorder=3)
    ax.errorbar(lam, med, yerr=err, fmt='-o', color=BLUE, ms=4.5, lw=1.6, capsize=2.6,
                elinewidth=1.0, mec='white', mew=0.6, label='Poisson (median)', zorder=4)
    ax.set_xlabel(r'molecules fed in per second ($\lambda$)'); ax.set_ylabel('steering latency (s)')
    # ax.set_xlim(0, xmax); ax.set_ylim(0, ymax)
    ax.set_xlim(0, 0.25); ax.set_xticks(np.arange(0, 0.26, 0.05))
    ax.set_ylim(0, 20);   ax.set_yticks(np.arange(0, 21, 5))
    ax.grid(True, color='0.85', linewidth=0.5); ax.set_axisbelow(True)
    h, l = ax.get_legend_handles_labels()
    # legend inside, bottom left: deterministic on row 1, the two Poisson series on row 2
    leg1 = ax.legend([h[0]], [l[0]], loc='lower left', bbox_to_anchor=(0.01, 0.13),
                     frameon=False, fontsize=7.5, handlelength=1.6, borderaxespad=0.3)
    ax.add_artist(leg1)
    ax.legend([h[2], h[1]], [l[2], l[1]], loc='lower left', bbox_to_anchor=(0.01, 0.01),
              ncol=2, frameon=False, fontsize=7.5, handlelength=1.6,
              columnspacing=0.8, borderaxespad=0.3)

    for e in ('png', 'pdf'):
        fig.savefig(f"{args.out}.{e}")
    print(f"wrote {args.out}.png / .pdf")


if __name__ == "__main__":
    main()
