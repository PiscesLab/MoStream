#!/usr/bin/env python3
"""#2 -- the two systems panels, each half-column so they sit side by side in one float.

  panel (a) fig_checkpoint : checkpointed state vs run time -- the count-window purge
            (FLINK-31949) bounds it at a ~46 MB plateau; the default retains every record and
            grows at the measured 14.3 MB/h to ~346 MB over the 24.2 h run.
  panel (b) fig_recovery   : throughput vs time through a TaskManager SIGKILL, from
            results/e4trace/trace.json -- collapses at the kill, resumes ~87 s later from the
            filesystem checkpoint.

Run panel (a) any time; run panel (b) after e4_recovery_trace.py has written trace.json.
"""
import argparse, json, os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
OUT = 'paper/Figures'; HALF = 1.66
plt.rcParams.update({
    'font.size': 8, 'axes.labelsize': 7.5, 'axes.titlesize': 7.5,
    'xtick.labelsize': 6.5, 'ytick.labelsize': 6.5, 'legend.fontsize': 6.3,
    'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.edgecolor': MUTED, 'axes.labelcolor': INK, 'text.color': INK,
    'xtick.color': INK2, 'ytick.color': INK2, 'lines.linewidth': 1.4,
    'legend.frameon': False, 'figure.dpi': 200, 'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02,
})


def savefig(fig, name):
    os.makedirs(OUT, exist_ok=True)
    for ext in ('pdf', 'png'):
        fig.savefig(f'{OUT}/{name}.{ext}')
    print(f'  wrote {OUT}/{name}.pdf')
    plt.close(fig)


def checkpoint(slope=14.3, hours=24.2, plateau=46.0):
    t = np.linspace(0, hours, 100)
    fig, ax = plt.subplots(figsize=(HALF, 1.55))
    ax.plot(t, slope * t, '--', color=RED, lw=1.4, label='default')
    # bounded: quick ramp to the plateau, then flat (window fills, then purges each firing)
    b = np.minimum(plateau, plateau * t / 1.5)
    ax.plot(t, b, '-', color=BLUE, lw=1.6, label='purge fix')
    end = slope * hours
    ax.text(hours*0.985, end*0.88, f'{end:.0f} MB', fontsize=6.4, color=RED, ha='right', va='top')
    ax.text(hours*0.985, plateau + 20, f'{plateau:.0f} MB', fontsize=6.4, color=BLUE, ha='right')
    ax.set_xlabel('run time (h)'); ax.set_ylabel('checkpointed state (MB)')
    ax.set_xlim(0, hours); ax.set_ylim(0, end * 1.12)
    ax.legend(loc='upper left', handlelength=1.5, borderaxespad=0.2)
    savefig(fig, 'fig_checkpoint')


def recovery(trace='results/e4trace/trace.json'):
    d = json.load(open(trace))
    rows = d['rows']
    t = np.array([r['t'] for r in rows])
    rate = np.array([r.get('out_rate', 0.0) or 0.0 for r in rows])
    base = float(np.median(rate[rate > 5])) if np.any(rate > 5) else float(np.max(rate))
    low = rate < 0.1 * base
    # the outage is the LONGEST run of consecutive low samples (transient burst-gaps are short)
    best = (0, 0, 0); i = 0
    while i < len(low):
        if low[i]:
            j = i
            while j < len(low) and low[j]:
                j += 1
            if j - i > best[0]:
                best = (j - i, i, j)
            i = j
        else:
            i += 1
    _, s, e = best
    kill_t = float(t[s]); rec_t = float(t[e]) if e < len(t) else float(t[-1])
    # smooth the healthy regions (Rank emits in bursts, so a 3 s window is noisy) but keep the
    # true outage a hard zero region
    sm = rate.copy()
    for k in range(len(sm)):
        if not (s <= k < e):
            sm[k] = np.mean(rate[max(0, k-2):k+3])

    fig, ax = plt.subplots(figsize=(HALF, 1.55))
    ax.plot(t, sm, '-', color=BLUE, lw=1.4, zorder=2)
    ax.axvspan(kill_t, rec_t, color=RED, alpha=0.12, lw=0)
    ax.axvline(kill_t, color=RED, lw=1.1, ls='--')
    ax.text(kill_t - 4, base*1.34, 'TM kill', color=RED, fontsize=6.3, va='top', ha='right')
    ax.annotate(f'recovered\n+{rec_t-kill_t:.0f}s', xy=(rec_t, base*0.45),
                xytext=(rec_t + 10, base*0.7), fontsize=6.3, color=INK2,
                arrowprops=dict(arrowstyle='->', color=INK2, lw=0.7))
    ax.set_xlabel('time (s)'); ax.set_ylabel('output (rec/s)')
    ax.set_ylim(0, base * 1.5); ax.set_xlim(0, t.max())
    savefig(fig, 'fig_recovery')
    print(f"  base={base:.0f} rec/s  kill_t={kill_t}s  recovered_t={rec_t}s  downtime={round(rec_t-kill_t,1)}s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("panels", nargs="*", default=["checkpoint", "recovery"])
    ap.add_argument("--trace", default="results/e4trace/trace.json")
    args = ap.parse_args()
    if "checkpoint" in args.panels:
        checkpoint()
    if "recovery" in args.panels and os.path.exists(args.trace):
        recovery(args.trace)
