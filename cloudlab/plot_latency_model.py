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
    'font.size': 8, 'axes.labelsize': 8, 'axes.titlesize': 8,
    'xtick.labelsize': 7, 'ytick.labelsize': 7, 'legend.fontsize': 6.6,
    'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.edgecolor': MUTED, 'axes.labelcolor': INK, 'text.color': INK,
    'xtick.color': INK2, 'ytick.color': INK2, 'lines.linewidth': 1.4,
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
    xmax = args.mu * 1.10
    ymax = max(p95.max(), 13) * 1.18

    # the band the typical latency never leaves -- makes "flat" readable at a glance
    ax.axhspan(7, 10, color=BLUE, alpha=0.07, lw=0, zorder=0)
    ax.text(xmax * 0.012, 8.5, 'typical stays\n7-10 s', fontsize=6.0, color=BLUE,
            ha='left', va='center', linespacing=1.15, zorder=5)

    # past the service rate the loop cannot keep up: shade it rather than draw a bare line
    ax.axvspan(args.mu, xmax, color=RED, alpha=0.09, lw=0, zorder=0)
    ax.axvline(args.mu, color=RED, ls=':', lw=1.1, zorder=2)
    ax.text(args.mu, ymax * 0.99, ' cannot\n keep up', color=RED, fontsize=6.0,
            va='top', ha='left', linespacing=1.15, zorder=5)

    # smooth (deterministic) reference
    if os.path.exists(args.det):
        dl, dm = det_series(args.det)
        ax.plot(dl, dm, '-s', color=MUTED, ms=3.2, lw=1.0, alpha=0.9,
                label='steady arrivals: typical', zorder=3)
    # bursty (Poisson), hardened with error bars
    ax.plot(lam, p95, '--', color=BLUE, lw=1.0, alpha=0.65,
            label='bursty arrivals: slowest 5%', zorder=3)
    ax.errorbar(lam, med, yerr=err, fmt='-o', color=BLUE, ms=4.5, lw=1.6, capsize=2.6,
                elinewidth=1.0, mec='white', mew=0.6,
                label='bursty arrivals: typical', zorder=4)

    ax.set_xlabel(r'molecules fed in per second ($\lambda$)')
    ax.set_ylabel('steering latency (s)')
    ax.set_xlim(0, xmax)
    ax.set_ylim(0, ymax)
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1.0), ncol=2,
              handlelength=1.6, columnspacing=1.0, borderaxespad=0.2, fontsize=6.3)
    os.makedirs(OUT, exist_ok=True)
    for ext in ('pdf', 'png'):
        fig.savefig(f'{OUT}/{args.name}.{ext}')
    print(f'  wrote {OUT}/{args.name}.pdf')


if __name__ == "__main__":
    main()
