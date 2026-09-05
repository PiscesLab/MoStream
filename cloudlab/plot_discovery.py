#!/usr/bin/env python3
"""E5 discovery figure: cumulative high-IP molecules found over wall-clock time.

MoStream curve is MEASURED from the real closed-loop campaign: each simulator logs one
`[oracle]` line per recommendation it verifies with xTB, timestamped with epoch seconds.
We pool the three simulator logs, deduplicate by SMILES (a molecule verified by more than one
simulator counts once, at its first verification), and step the cumulative count of molecules
whose true (oracle) ionization potential exceeds \\SI{14}{\\volt}.

  results/campaign/oracle_*.log   -- '<epoch> [oracle] <smiles> est_ip=.. -> xtb_ip=.. (..s)'

Baselines (overlaid when their data files exist, so nothing is fabricated):
  results/campaign/colmena.csv    -- 'hours,cumulative'  from Colmena's published campaign
                                     (Ward et al. 2021); cite, do not invent.
  results/campaign/random.csv     -- 'hours,cumulative'  from a random-selection arm (same
                                     oracle, molecules drawn uniformly from the search space).

Design matches the other paper figures (serif, brand palette, grayscale-safe line styles).
"""
import argparse, csv, glob, os, re
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
OUT = 'paper/Figures'; COL = 3.3
HIT = 14.0
plt.rcParams.update({
    'font.size': 8, 'axes.labelsize': 8, 'axes.titlesize': 8,
    'xtick.labelsize': 7, 'ytick.labelsize': 7, 'legend.fontsize': 6.8,
    'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.edgecolor': MUTED, 'axes.labelcolor': INK, 'text.color': INK,
    'xtick.color': INK2, 'ytick.color': INK2, 'lines.linewidth': 1.4,
    'legend.frameon': False, 'figure.dpi': 200, 'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02,
})

_LINE = re.compile(r'^(?P<ts>\d+)\s+\[oracle\]\s+(?P<smiles>\S+)\s+est_ip=\S+\s+->\s+xtb_ip=(?P<ip>[-\d.]+)')


def mostream_curve(logdir):
    """Pooled, de-duplicated discovery events: (elapsed_hours[], cumulative_hits[])."""
    seen, events = {}, []
    for path in sorted(glob.glob(os.path.join(logdir, 'oracle_*.log'))):
        for ln in open(path):
            m = _LINE.match(ln)
            if not m:
                continue
            ts, smi, ip = int(m['ts']), m['smiles'], float(m['ip'])
            if smi not in seen or ts < seen[smi][0]:
                seen[smi] = (ts, ip)
    if not seen:
        return None, None, 0, 0
    t0 = min(ts for ts, _ in seen.values())
    hits = sorted((ts, ip) for ts, ip in seen.values() if ip > HIT)
    n_verified = len(seen)
    if not hits:
        return np.array([0.0]), np.array([0]), n_verified, 0
    hx = np.array([(ts - t0) / 3600.0 for ts, _ in hits])
    hy = np.arange(1, len(hits) + 1)
    # prepend origin so the step starts at 0
    return np.concatenate([[0.0], hx]), np.concatenate([[0], hy]), n_verified, len(hits)


def baseline(path):
    if not os.path.exists(path):
        return None
    h, c = [], []
    for r in csv.DictReader(open(path)):
        h.append(float(r['hours'])); c.append(float(r['cumulative']))
    return np.array(h), np.array(c)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--logdir', default='results/campaign')
    ap.add_argument('--name', default='fig_discovery')
    args = ap.parse_args()

    mx, my, nver, nhit = mostream_curve(args.logdir)
    fig, ax = plt.subplots(figsize=(COL, 2.15))

    rnd = baseline(os.path.join(args.logdir, 'random.csv'))
    if rnd is not None:
        ax.step(rnd[0], rnd[1], where='post', color=MUTED, lw=1.2, ls=':', label='random selection')

    col = baseline(os.path.join(args.logdir, 'colmena.csv'))
    if col is not None:
        ax.step(col[0], col[1], where='post', color=RED, lw=1.4, ls='--', label='Colmena (task-based)')

    if mx is not None:
        ax.step(mx, my, where='post', color=BLUE, lw=1.8, label='\\shortName (streaming)'.replace('\\shortName', 'MoStream'))
        if len(mx) > 1:
            ax.plot(mx[1:], my[1:], 'o', color=BLUE, ms=2.6, mec='white', mew=0.4, zorder=5)

    ax.set_xlabel('elapsed time (h)')
    ax.set_ylabel('molecules found, IP $>$ \\SI{14}{}V'.replace('\\SI{14}{}', '14 '))
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.legend(loc='upper left', handlelength=1.9, borderaxespad=0.3)

    os.makedirs(OUT, exist_ok=True)
    for e in ('pdf', 'png'):
        fig.savefig(f'{OUT}/{args.name}.{e}')
    print(f'  wrote {OUT}/{args.name}.pdf')
    print(f'  MoStream: {nver} molecules verified, {nhit} hits (IP>{HIT:.0f} V)')


if __name__ == '__main__':
    main()
