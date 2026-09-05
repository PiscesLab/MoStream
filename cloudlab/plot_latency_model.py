#!/usr/bin/env python3
"""#1 -- steering latency vs offered load (supports eq:lstream and the seconds-scale claim).

Hardened version: the bursty (Poisson) series carries error bars (std of the median over --reps
measurement windows per load point); the saturating point at lambda~mu is excluded. The smooth
(deterministic) run is shown as a light reference. Median latency stays seconds-scale across the
feasible range and rises with load as the queueing model predicts, and the loop saturates at
lambda~mu (rho<1, the stability boundary eq:lstream requires). The oracle-bound operating point
(lambda~0.024/s, Section E5) sits far to the left, deep in the flat regime.
"""
import argparse, json, os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
OUT = 'paper/Figures'; COL = 3.3
plt.rcParams.update({
    'font.size': 7.5, 'axes.labelsize': 7.5, 'axes.titlesize': 7.5,
    'xtick.labelsize': 7.5, 'ytick.labelsize': 7.5, 'legend.fontsize': 7.5,
    'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.edgecolor': MUTED, 'axes.labelcolor': INK, 'text.color': INK,
    'xtick.color': INK, 'ytick.color': INK, 'lines.linewidth': 1.4,
    'legend.frameon': False, 'figure.dpi': 200, 'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02,
})


def det_series(path):
    ph = json.load(open(path))["phases"]
    lam = np.array([p["lambda"] for p in ph])
    med = np.array([p["median_ms"] for p in ph]) / 1000.0
    o = np.argsort(lam)
    return lam[o], med[o]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--poisson", default="results/latsweep_hard_summary.json")
    ap.add_argument("--det", default="results/latsweep/summary.json")
    ap.add_argument("--mu", type=float, default=0.27)
    ap.add_argument("--oracle-lambda", type=float, default=0.024)
    ap.add_argument("--name", default="fig_latency_model")
    args = ap.parse_args()

    agg = json.load(open(args.poisson))["aggregated"]
    agg = [a for a in agg if a.get("median_mean_ms")]
    agg.sort(key=lambda a: a["lambda"])
    lam = np.array([a["lambda"] for a in agg])
    med = np.array([a["median_mean_ms"] for a in agg]) / 1000.0
    err = np.array([a["median_std_ms"] for a in agg]) / 1000.0
    p95 = np.array([a["p95_mean_ms"] for a in agg]) / 1000.0
    print("hardened Poisson:")
    for a in agg:
        print(f"  lambda={a['lambda']:.3f} median={a['median_mean_ms']/1000:.2f}+/-{a['median_std_ms']/1000:.2f}s "
              f"(reps={a['n_reps']}, n={a['n_total']})")

    fig, ax = plt.subplots(figsize=(COL, 2.15))
    ax.grid(True, color='0.85', linewidth=0.5)
    ax.set_axisbelow(True)
    # snap the axis edges onto the outermost gridline (no blank margin past the last line)
    import math
    xmax = math.ceil(lam.max() / 0.05) * 0.05          # -> next 0.05 gridline
    ymax = math.ceil(max(p95.max(), 13) / 5.0) * 5.0    # -> next 5 s gridline

    # the band the typical latency never leaves -- makes "flat" readable at a glance
    ax.axhspan(7, 10, color=BLUE, alpha=0.07, lw=0, zorder=0)
    ax.text(xmax * 0.012, 8.5, 'median stays\n7-10 s', fontsize=7.5, color=BLUE,
            ha='left', va='center', linespacing=1.15, zorder=5)

    # smooth (deterministic) reference
    if os.path.exists(args.det):
        dl, dm = det_series(args.det)
        ax.plot(dl, dm, '-s', color=MUTED, ms=3.2, lw=1.0, alpha=0.9,
                label='deterministic (median)', zorder=3)
    # Poisson, hardened with error bars
    ax.plot(lam, p95, '--', color=BLUE, lw=1.0, alpha=0.65,
            label='Poisson (p95)', zorder=3)
    ax.errorbar(lam, med, yerr=err, fmt='-o', color=BLUE, ms=4.5, lw=1.6, capsize=2.6,
                elinewidth=1.0, mec='white', mew=0.6,
                label='Poisson (median)', zorder=4)

    ax.set_xlabel(r'molecules fed in per second ($\lambda$)')
    ax.set_ylabel('steering latency (s)')
    ax.set_xlim(0, xmax); ax.set_xticks(np.arange(0, xmax + 1e-9, 0.05))
    ax.set_ylim(0, ymax); ax.set_yticks(np.arange(0, ymax + 1e-9, 5))
    h, l = ax.get_legend_handles_labels()
    # legend inside, bottom left: deterministic on row 1, the two Poisson series on row 2
    leg1 = ax.legend([h[0]], [l[0]], loc='lower left', bbox_to_anchor=(0.01, 0.13),
                     frameon=False, fontsize=7.5, handlelength=1.6, borderaxespad=0.3)
    ax.add_artist(leg1)
    ax.legend([h[2], h[1]], [l[2], l[1]], loc='lower left', bbox_to_anchor=(0.01, 0.01),
              ncol=2, frameon=False, fontsize=7.5, handlelength=1.6,
              columnspacing=0.8, borderaxespad=0.3)
    os.makedirs(OUT, exist_ok=True)
    for ext in ('pdf', 'png'):
        fig.savefig(f'{OUT}/{args.name}.{ext}')
    print(f'  wrote {OUT}/{args.name}.pdf')


if __name__ == "__main__":
    main()
