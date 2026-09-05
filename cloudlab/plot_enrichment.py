#!/usr/bin/env python3
"""Enrichment figure: MEASURED MoStream discovery vs a MEASURED random base rate.

Both arms are scored by the SAME xTB oracle, so the comparison is apples-to-apples:
  - MoStream : the closed active-learning loop's `[oracle]` logs
               (results/campaign/oracle_*.log, pooled over the 3 simulators)
  - random   : a random-selection arm over the same search space, produced by
               `random_baseline.py` (results/campaign/random/oracle_*.log)

For each arm we pool its `[oracle]` lines, dedup by SMILES (a molecule counts once, at its first
verification), and count "hits" -- molecules whose true (oracle) ionization potential exceeds
14 V. We report cumulative hits against

  (i)  the number of oracle evaluations -- the simulation BUDGET (the more meaningful axis: the
       xTB call is the expensive resource both arms spend equally per molecule), and
  (ii) elapsed wall-clock time in hours.

ENRICHMENT FACTOR = (MoStream hit rate) / (random base rate), where hit rate = n_hits/n_verified.
It is the number the figure exists to make: how many times more often the streaming active
learner turns an oracle call into a high-IP molecule than picking at random does. Printed with
n_verified and n_hits for each arm, and annotated on the budget panel.

Nothing is fabricated: an arm with no log simply is not drawn, and the enrichment factor is only
computed when both arms have data. Style matches the other paper figures (serif, same palette) --
see plot_discovery.py / plot_latency_model.py.

TEST / RUN (matplotlib lives in the `mostream` env, not base/mostream_pin):
    source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream
    python cloudlab/plot_enrichment.py \
        --mostream-glob 'results/campaign/oracle_*.log' \
        --random-glob   'results/campaign/random/oracle_*.log'
"""
import argparse
import glob
import os
import re

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
OUT = 'paper/Figures'
COL = 3.3
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

# Same parser plot_discovery.py uses. `est_ip=\S+` matches both a float and the random arm's NA;
# FAILED lines carry no `-> xtb_ip=` and so are ignored (a failed molecule is never a hit).
_LINE = re.compile(r'^(?P<ts>\d+)\s+\[oracle\]\s+(?P<smiles>\S+)\s+est_ip=\S+\s+->\s+xtb_ip=(?P<ip>[-\d.]+)')


def load_arm(patterns):
    """Pool `[oracle]` lines matching any of `patterns`, dedup by SMILES (first verification wins).

    Returns (stats_dict_or_None, n_calls). n_calls is the raw number of matched oracle lines
    (i.e. xTB calls, duplicates included); n_verified is the de-duplicated molecule count.
    """
    seen, n_calls = {}, 0
    for pat in patterns:
        for path in sorted(glob.glob(pat)):
            for ln in open(path):
                m = _LINE.match(ln)
                if not m:
                    continue
                n_calls += 1
                ts, smi, ip = int(m['ts']), m['smiles'], float(m['ip'])
                if smi not in seen or ts < seen[smi][0]:
                    seen[smi] = (ts, ip)
    if not seen:
        return None, n_calls

    events = sorted(seen.values())                       # (ts, ip) in verification order
    t0 = events[0][0]
    hours = np.array([(ts - t0) / 3600.0 for ts, _ in events])
    budget = np.arange(1, len(events) + 1)               # 1..n_verified oracle evaluations
    is_hit = np.array([1 if ip > HIT else 0 for _, ip in events])
    cum = np.cumsum(is_hit)
    n_verified, n_hits = len(events), int(cum[-1])
    return {
        'hours': hours, 'budget': budget, 'cum': cum,
        'n_verified': n_verified, 'n_hits': n_hits,
        'hit_rate': n_hits / n_verified,
    }, n_calls


def _step(x, y):
    """Prepend the origin so a cumulative step starts cleanly at (0, 0)."""
    return np.concatenate([[0.0], x]), np.concatenate([[0], y])


def _draw(ax, xkey, arm_ms, arm_rnd, base_rate, xlabel):
    """Draw both arms on one axis; add the straight base-rate reference on the budget axis."""
    if arm_rnd is not None:
        x, y = _step(arm_rnd[xkey], arm_rnd['cum'])
        ax.step(x, y, where='post', color=MUTED, lw=1.2, ls=':', label='random selection')
    if arm_ms is not None:
        # Straight "what random would give" line, drawn only against budget (a rate over calls).
        if xkey == 'budget' and base_rate is not None:
            bmax = arm_ms['budget'][-1]
            ax.plot([0, bmax], [0, base_rate * bmax], color=RED, lw=1.0, ls='--',
                    label='random base rate', zorder=2)
        x, y = _step(arm_ms[xkey], arm_ms['cum'])
        ax.step(x, y, where='post', color=BLUE, lw=1.8, label='MoStream (streaming)', zorder=4)
    ax.set_xlabel(xlabel)
    ax.set_ylabel('molecules found, IP $>$ 14 V')
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--mostream-glob', action='append', default=None,
                    help='glob(s) for MoStream oracle logs (repeatable). '
                         'Default: results/campaign/oracle_*.log')
    ap.add_argument('--random-glob', action='append', default=None,
                    help='glob(s) for random-arm oracle logs (repeatable). '
                         'Default: results/campaign/random/oracle_*.log')
    ap.add_argument('--out', default=OUT, help='output directory for the figure')
    ap.add_argument('--name', default='fig_enrichment', help='output basename (no extension)')
    args = ap.parse_args()

    ms_pat = args.mostream_glob or ['results/campaign/oracle_*.log']
    rnd_pat = args.random_glob or ['results/campaign/random/oracle_*.log']

    arm_ms, ms_calls = load_arm(ms_pat)
    arm_rnd, rnd_calls = load_arm(rnd_pat)

    # ---- report the numbers the figure is built on ---------------------------------------------
    def _report(name, arm, calls):
        if arm is None:
            print(f"  {name:9s}: no data (patterns matched no [oracle] lines)")
            return
        dup = calls - arm['n_verified']
        print(f"  {name:9s}: n_verified={arm['n_verified']:5d}  n_hits={arm['n_hits']:4d}  "
              f"hit_rate={arm['hit_rate']:.4f}  (oracle_calls={calls}"
              + (f", {dup} duplicate smiles" if dup else "") + ")")

    print("enrichment inputs:")
    _report('MoStream', arm_ms, ms_calls)
    _report('random', arm_rnd, rnd_calls)

    base_rate = arm_rnd['hit_rate'] if arm_rnd is not None else None
    enrich = None
    if arm_ms is not None and base_rate:
        enrich = arm_ms['hit_rate'] / base_rate
        print(f"  ENRICHMENT FACTOR = {arm_ms['hit_rate']:.4f} / {base_rate:.4f} = {enrich:.2f}x")
    elif arm_ms is not None and arm_rnd is not None and base_rate == 0:
        print("  ENRICHMENT FACTOR = undefined (random base rate is 0 hits -- need more random calls)")
    else:
        print("  ENRICHMENT FACTOR = not computed (need both arms)")

    # ---- figure: budget panel (primary) + time panel -------------------------------------------
    fig, (axb, axt) = plt.subplots(1, 2, figsize=(2 * COL + 0.2, 2.25))
    _draw(axb, 'budget', arm_ms, arm_rnd, base_rate, 'oracle evaluations (simulation budget)')
    _draw(axt, 'hours', arm_ms, arm_rnd, base_rate, 'elapsed time (h)')

    if enrich is not None:
        axb.text(0.04, 0.96, f'{enrich:.1f}$\\times$ enrichment', transform=axb.transAxes,
                 ha='left', va='top', fontsize=7.4, color=BLUE)
    axb.legend(loc='lower right', handlelength=1.9, borderaxespad=0.3)

    fig.tight_layout(pad=0.4, w_pad=1.2)
    os.makedirs(args.out, exist_ok=True)
    paths = []
    for e in ('pdf', 'png'):
        p = f'{args.out}/{args.name}.{e}'
        fig.savefig(p)
        paths.append(p)
    print("  wrote " + " ".join(paths))


if __name__ == '__main__':
    main()
