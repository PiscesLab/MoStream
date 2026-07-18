#!/usr/bin/env python3
"""#1 -- steering latency vs offered load (supports eq:lstream and the seconds-scale claim).

Plots measured steering latency against offered load lambda for both a smooth (deterministic)
and a bursty (Poisson) arrival process. The median stays seconds-scale across the whole feasible
range; under bursty load the tail (p95) widens as lambda approaches the service rate mu, and the
loop saturates at lambda ~ mu -- the stability boundary rho<1 the model requires. The oracle-bound
operating point (lambda ~ 0.024/s, Section E5) sits far to the left, deep in the flat regime.
"""
import argparse, json, os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, AQUA, RED, INK, INK2, MUTED = '#2a78d6', '#1baf7a', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
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


def series(path):
    ph = json.load(open(path))["phases"]
    lam = np.array([p["lambda"] for p in ph])
    med = np.array([p["median_ms"] for p in ph]) / 1000.0
    p95 = np.array([p["p95_ms"] for p in ph]) / 1000.0
    return lam, med, p95


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--poisson", default="results/latsweep_poisson_summary.json")
    ap.add_argument("--det", default="results/latsweep/summary.json")
    ap.add_argument("--mu", type=float, default=0.27)
    ap.add_argument("--oracle-lambda", type=float, default=0.024)
    ap.add_argument("--name", default="fig_latency_model")
    args = ap.parse_args()

    lam_p, med_p, p95_p = series(args.poisson)
    lam_d, med_d, p95_d = series(args.det)

    fig, ax = plt.subplots(figsize=(COL, 1.95))
    # bursty (Poisson): median line + shaded median..p95 band (the tail that queueing inflates)
    order = np.argsort(lam_p)
    lp, mp, qp = lam_p[order], med_p[order], p95_p[order]
    ax.fill_between(lp, mp, qp, color=BLUE, alpha=0.15, lw=0)
    ax.plot(lp, mp, '-o', color=BLUE, ms=4.5, mec='white', mew=0.6, label='bursty load: median')
    ax.plot(lp, qp, '--', color=BLUE, lw=1.0, alpha=0.8, label='bursty load: p95')
    # smooth (deterministic) median for contrast
    od = np.argsort(lam_d)
    ax.plot(lam_d[od], med_d[od], '-s', color=MUTED, ms=3.5, lw=1.1, label='smooth load: median')
    # saturation boundary lambda = mu
    ax.axvline(args.mu, color=RED, ls=':', lw=1.1)
    ax.text(args.mu, ax.get_ylim()[1]*0.98, r' $\lambda=\mu$', color=RED, fontsize=6.6, va='top', ha='left')
    # oracle operating point
    ax.annotate('oracle-bound\noperating point', xy=(args.oracle_lambda, 8.0),
                xytext=(args.oracle_lambda + 0.01, 13.2), fontsize=6.2, color=INK2,
                arrowprops=dict(arrowstyle='->', color=INK2, lw=0.8))
    ax.set_xlabel(r'offered load $\lambda$ (rec/s)')
    ax.set_ylabel('steering latency (s)')
    ax.set_xlim(0, args.mu * 1.06)
    ax.set_ylim(0, max(qp.max(), p95_d.max()) * 1.12)
    ax.legend(loc='lower left', handlelength=1.7, ncol=1)
    os.makedirs(OUT, exist_ok=True)
    for ext in ('pdf', 'png'):
        fig.savefig(f'{OUT}/{args.name}.{ext}')
    print(f'  wrote {OUT}/{args.name}.pdf')


if __name__ == "__main__":
    main()
