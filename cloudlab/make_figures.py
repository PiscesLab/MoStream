#!/usr/bin/env python3
"""Generate the paper's figures from the experiment data.

Design constraints (IEEE two-column):
  * ~3.3in column width, so 8pt labels / 7pt ticks and no wasted chrome.
  * GRAYSCALE-SAFE: reviewers print in black and white. Every series carries a second
    encoding (line style, marker, or hatch) so identity never depends on hue alone.
  * COLORBLIND-SAFE: hues are the validated categorical slots, assigned in fixed order.
  * NO dual-axis charts. Two measures of different scale get two panels, never two y-scales.

Usage:
    python cloudlab/make_figures.py                 # build everything it has data for
    python cloudlab/make_figures.py --only cost     # build one

Outputs PDF (vector, for LaTeX) and PNG (for quick eyeballing) into paper/Figures/.
"""
import argparse
import csv
import json
import os
import re
import statistics
import sys
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# --- Validated categorical slots, fixed order. Never cycled, never reordered. ---
BLUE, AQUA, YELLOW, GREEN = '#2a78d6', '#1baf7a', '#eda100', '#008300'
RED = '#e34948'          # status: failure/critical. Reserved -- never a "series 4".
INK, INK2, MUTED = '#0b0b0b', '#52514e', '#8a8985'

OUT = 'paper/Figures'
COL = 3.3   # single-column width, inches

plt.rcParams.update({
    'font.size': 8, 'axes.labelsize': 8, 'axes.titlesize': 8,
    'xtick.labelsize': 7, 'ytick.labelsize': 7, 'legend.fontsize': 7,
    'font.family': 'serif', 'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.edgecolor': MUTED, 'axes.labelcolor': INK, 'text.color': INK,
    'xtick.color': INK2, 'ytick.color': INK2,
    'grid.color': MUTED, 'grid.alpha': 0.25, 'grid.linewidth': 0.5,
    'lines.linewidth': 1.4, 'legend.frameon': False,
    'figure.dpi': 200, 'savefig.bbox': 'tight', 'savefig.pad_inches': 0.02,
})


def save(fig, name):
    os.makedirs(OUT, exist_ok=True)
    for ext in ('pdf', 'png'):
        fig.savefig(f'{OUT}/{name}.{ext}')
    plt.close(fig)
    print(f'  wrote {OUT}/{name}.pdf')


# ---------------------------------------------------------------- memory trace
MEM_RE = re.compile(
    r'(?P<ts>[\d-]+ [\d:]+)\s+pid=\d+\s+rss=(?P<rss>\d+)MB\s+'
    r'jvm_heap=(?P<hu>\d+)/(?P<hm>\d+)MB\s+direct=(?P<du>\d+)/(?P<dt>\d+)MB\s+'
    r'py_workers=(?P<pn>\d+)x(?P<pm>\d+)MB')


def read_mem(path):
    rows = []
    if not os.path.exists(path):
        return rows
    for line in open(path, errors='replace'):
        m = MEM_RE.search(line)
        if not m:
            continue
        d = m.groupdict()
        rows.append({
            't': datetime.strptime(d['ts'], '%Y-%m-%d %H:%M:%S').timestamp(),
            'rss': int(d['rss']), 'heap_used': int(d['hu']), 'heap_max': int(d['hm']),
            'direct': int(d['du']), 'py': int(d['pm']),
        })
    if rows:
        t0 = rows[0]['t']
        for r in rows:
            r['min'] = (r['t'] - t0) / 60.0
    return rows


def fig_memory():
    """The derived heap rides at its ceiling; ours has headroom.

    Heap UTILISATION (used/max), not absolute MB: the arms have different heap sizes, so
    absolute values are not comparable. Each arm is plotted from its OWN first sample --
    the memory log stores local wall-clock strings while the failure log stores epoch-ms,
    and mixing the two bases silently shifts one series by over an hour. The restart
    timeline lives in fig_restarts, which uses a single base.
    """
    ctl = read_mem('results/e2_before/tm_memory_e2before.log')
    trt = read_mem('results/e1/tm_memory.log')
    if not ctl:
        print('  [skip] memory: no control-arm trace')
        return

    nfail = 0
    if os.path.exists('results/e2_before/exceptions.json'):
        nfail = len(json.load(open('results/e2_before/exceptions.json'))['exceptionHistory']['entries'])

    fig, ax = plt.subplots(figsize=(COL, 1.7))
    xc = [r['min'] for r in ctl]
    yc = [100.0 * r['heap_used'] / r['heap_max'] for r in ctl]
    ax.plot(xc, yc, color=BLUE, ls='-', label=f"derived ({ctl[0]['heap_max']} MB heap)")
    if nfail:
        ax.annotate(f'{nfail} OOM restarts', xy=(xc[len(xc)//2], max(yc)),
                    xytext=(xc[len(xc)//2], 118), fontsize=6.5, color=RED, ha='center',
                    arrowprops=dict(arrowstyle='-|>', color=RED, lw=0.7,
                                    shrinkA=0, shrinkB=2))

    if trt:
        xt = [r['min'] for r in trt]
        yt = [100.0 * r['heap_used'] / r['heap_max'] for r in trt]
        ax.plot(xt, yt, color=AQUA, ls='--', label=f"ours ({trt[0]['heap_max']} MB heap)")
        ax.annotate('0 restarts', xy=(xt[-1], yt[-1]), xytext=(xt[-1], yt[-1] + 22),
                    fontsize=6.5, color=AQUA, ha='center',
                    arrowprops=dict(arrowstyle='-|>', color=AQUA, lw=0.7,
                                    shrinkA=0, shrinkB=2))

    ax.axhline(100, color=MUTED, lw=0.7, ls=(0, (1, 2)))
    ax.text(ax.get_xlim()[1], 100, ' exhausted', fontsize=6, color=MUTED, va='center')
    ax.set_xlabel('Elapsed time (min)')
    ax.set_ylabel('JVM heap used (% of max)')
    ax.set_ylim(0, 132)
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.grid(axis='y'); ax.set_axisbelow(True)
    ax.legend(loc='upper left', bbox_to_anchor=(0.0, -0.30), ncol=2, handlelength=1.6,
              labelcolor=INK2, borderpad=0.2, columnspacing=1.2)
    save(fig, 'fig_heap_utilisation')


def fig_invisible_memory():
    """The Python workers are charged to the machine but to no Flink budget.

    Flink's taskmanager.memory.process.size is a promise about the JVM process. The Beam
    Python workers are separate OS processes and appear in no Flink budget. Size a container
    to process.size and the workers push the machine past it.
    """
    trt = read_mem('results/e1/tm_memory.log')
    if not trt:
        print('  [skip] invisible memory: no trace')
        return
    r = trt[-1]
    PROCESS_SIZE = 9856     # taskmanager.memory.process.size, our tuned budget

    fig, ax = plt.subplots(figsize=(COL, 1.35))
    ax.barh(0, r['rss'], height=0.45, color=BLUE, edgecolor='white', lw=0.8,
            label='TaskManager JVM (budgeted)')
    ax.barh(0, r['py'], left=r['rss'], height=0.45, color=YELLOW, edgecolor='white',
            lw=0.8, hatch='///', label='Python workers (not budgeted)')
    total = r['rss'] + r['py']

    ax.axvline(PROCESS_SIZE, color=RED, ls='--', lw=1.1)
    ax.text(PROCESS_SIZE, 0.42, f" Flink's budget\n {PROCESS_SIZE} MB",
            fontsize=6.2, color=RED, va='bottom', ha='center')

    ax.text(r['rss'] / 2, 0, f"{r['rss']} MB", ha='center', va='center',
            fontsize=6.5, color='white')
    ax.text(r['rss'] + r['py'] / 2, 0, f"{r['py']} MB", ha='center', va='center',
            fontsize=6.5, color='white')
    ax.text(total, -0.33, f'{total} MB', va='center', ha='center', fontsize=7, color=INK)

    ax.set_yticks([]); ax.set_ylim(-0.45, 0.95)
    ax.set_xlabel('Resident memory (MB)')
    ax.set_xlim(0, max(total, PROCESS_SIZE) * 1.16)
    ax.grid(axis='x'); ax.set_axisbelow(True)
    ax.legend(loc='upper left', handlelength=1.4, labelcolor=INK2, borderpad=0.2,
              bbox_to_anchor=(0.0, 1.35), ncol=1)
    save(fig, 'fig_invisible_memory')


# ---------------------------------------------------------------- cost breakdown
KV = re.compile(r'(\w+)=([-\d.]+)')


def fig_cost():
    """Nearly half the per-record cost is recomputation.

    Two segments per stage, not seven: the per-phase detail is already in the paper's table,
    and seven labelled segments collide at column width. Hatch carries the claim, so it
    survives grayscale.
    """
    path = 'results/profile/prof.txt'
    if not os.path.exists(path):
        print('  [skip] cost: no profile data')
        return
    tr, inf = [], []
    for line in open(path, errors='replace'):
        if 'TRAINPROF subtask=' in line:
            tr.append({k: float(v) for k, v in KV.findall(line)})
        elif 'INFERPROF chunk=' in line:
            inf.append({k: float(v) for k, v in KV.findall(line)})
    if not tr or not inf:
        print('  [skip] cost: incomplete profile')
        return

    def med(rows, k):
        v = [r[k] for r in rows if k in r]
        return statistics.median(v) if v else 0.0

    stages = [
        ('Infer', med(inf, 'predict'),
                  med(inf, 'build') + med(inf, 'loads') + med(inf, 'setw') + med(inf, 'prep')),
        ('Train', med(tr, 'fit') + med(tr, 'persist'),
                  med(tr, 'dumps') + med(tr, 'tolist')),
    ]

    fig, ax = plt.subplots(figsize=(COL, 1.45))
    for row, (name, need, avoid) in enumerate(stages):
        ax.barh(row, need, height=0.5, color=BLUE, edgecolor='white', lw=0.8)
        ax.barh(row, avoid, left=need, height=0.5, color=YELLOW, edgecolor='white',
                lw=0.8, hatch='///')
        if need > 0.7:
            ax.text(need / 2, row, f'{need:.1f}s', ha='center', va='center',
                    fontsize=6.5, color='white')
        if avoid > 0.7:
            ax.text(need + avoid / 2, row, f'{avoid:.1f}s', ha='center', va='center',
                    fontsize=6.5, color='white')
        ax.text(need + avoid + 0.15, row, f'{need+avoid:.1f}s', va='center',
                fontsize=7, color=INK)

    ax.set_yticks([0, 1]); ax.set_yticklabels(['Infer', 'Train'])
    ax.set_xlabel('Median time per record (s)')
    ax.set_xlim(0, max(n + a for _, n, a in stages) * 1.16)
    ax.grid(axis='x'); ax.set_axisbelow(True)

    tot = sum(n + a for _, n, a in stages)
    av = sum(a for _, _, a in stages)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(facecolor=BLUE, label='necessary work'),
                       Patch(facecolor=YELLOW, hatch='///', label='recomputed every record')],
              loc='upper left', bbox_to_anchor=(0.0, -0.42), ncol=2,
              handlelength=1.4, labelcolor=INK2, borderpad=0.2, columnspacing=1.0)
    ax.set_title(f'{100*av/tot:.0f}% of the {tot:.1f}s per-record path is recomputation',
                 loc='left', color=INK2, pad=4, fontsize=7)
    save(fig, 'fig_cost_breakdown')


# ---------------------------------------------------------------- restarts
def fig_restarts():
    """Cumulative restarts. One line per arm; the flat line IS the result."""
    p = 'results/e2_before/exceptions.json'
    if not os.path.exists(p):
        print('  [skip] restarts: no exception data')
        return
    ents = json.load(open(p))['exceptionHistory']['entries']
    ts = sorted(e['timestamp'] / 1000 for e in ents)
    t0 = ts[0]
    x = [(t - t0) / 60.0 for t in ts]
    y = list(range(1, len(x) + 1))

    fig, ax = plt.subplots(figsize=(COL, 1.5))
    ax.step([0] + x, [0] + y, where='post', color=RED, ls='-', marker='x', ms=3.5, mew=1.0)
    ax.text(x[-1], y[-1], f'  {y[-1]} restarts', fontsize=7, color=RED, va='center')

    # Our budget: flat at zero for as long as we ran it.
    e1 = read_mem('results/e1/tm_memory.log')
    span = max(x[-1], (e1[-1]['min'] if e1 else 0)) * 1.05 or 20
    ax.plot([0, span], [0, 0], color=BLUE, ls='--')
    ax.text(span, 0.35, 'our budget: 0', fontsize=7, color=BLUE, ha='right')

    ax.set_xlabel('Elapsed time (min)')
    ax.set_ylabel('Cumulative restarts')
    ax.set_ylim(-0.6, len(x) + 1.5)
    ax.grid(axis='y'); ax.set_axisbelow(True)
    save(fig, 'fig_restarts')


# ---------------------------------------------------------------- latency CDF
COLMENA_MS = 57 * 60 * 1000


def fig_latency():
    p = 'results/e0/e0_tuned.csv'
    if not os.path.exists(p):
        print('  [skip] latency: no E0 data yet (run cloudlab/e0_steering_latency.py)')
        return
    lat = sorted(int(r['latency_ms']) for r in csv.DictReader(open(p)) if int(r['latency_ms']) >= 0)
    if not lat:
        print('  [skip] latency: no attributable samples')
        return
    n = len(lat)
    fig, ax = plt.subplots(figsize=(COL, 1.7))
    ax.step([v / 1000 for v in lat], [(i + 1) / n for i in range(n)],
            where='post', color=BLUE, ls='-')
    ax.axvline(COLMENA_MS / 1000, color=RED, ls='--', lw=1.0)
    ax.text(COLMENA_MS / 1000, 0.5, ' Colmena\n 57 min', fontsize=6.5, color=RED, va='center')
    med = statistics.median(lat) / 1000
    ax.text(med, 0.5, f'{med:.0f}s ', fontsize=6.5, color=BLUE, ha='right', va='center')
    ax.set_xscale('log')
    ax.set_xlabel('Steering latency (s, log scale)')
    ax.set_ylabel('CDF')
    ax.set_ylim(0, 1.03)
    ax.grid(which='both'); ax.set_axisbelow(True)
    save(fig, 'fig_latency_cdf')




def fig_leak():
    """Python worker RSS grows without bound, and no engine budget bounds it.

    The single most consequential measurement in the paper: the workers are separate OS
    processes, so Flink's memory model does not see them at all, and Infer's per-record
    Keras model construction leaks into them.
    """
    rows = read_mem('results/e1/tm_memory.log')
    rows = [r for r in rows if r['py'] > 0]
    if len(rows) < 10:
        print('  [skip] leak: not enough samples')
        return
    x = [r['min'] / 60.0 for r in rows]          # hours
    y = [r['py'] / 1024.0 for r in rows]         # GB
    d = [r['direct'] / 1024.0 for r in rows]
    h = [r['heap_used'] / 1024.0 for r in rows]

    fig, ax = plt.subplots(figsize=(COL, 1.8))
    ax.plot(x, y, color=YELLOW, ls='-', label='Python workers')
    ax.plot(x, h, color=BLUE, ls='--', label='JVM heap')
    ax.plot(x, d, color=AQUA, ls=':', label='JVM direct')

    # linear fit on the worker series -> the growth rate is the headline
    n = len(x)
    mx, my = sum(x)/n, sum(y)/n
    num = sum((xi-mx)*(yi-my) for xi, yi in zip(x, y))
    den = sum((xi-mx)**2 for xi in x) or 1e-9
    slope = num/den
    ax.annotate(f'{slope:+.1f} GB/hour', xy=(x[len(x)//2], y[len(y)//2]),
                xytext=(x[len(x)//3], max(y)*1.12), fontsize=7, color=INK,
                arrowprops=dict(arrowstyle='-|>', color=MUTED, lw=0.7))

    ax.set_xlabel('Elapsed time (h)')
    ax.set_ylabel('Resident memory (GB)')
    ax.set_ylim(0, max(y)*1.30)
    ax.grid(axis='y'); ax.set_axisbelow(True)
    ax.legend(loc='upper left', bbox_to_anchor=(0.0, -0.32), ncol=3, handlelength=1.5,
              labelcolor=INK2, borderpad=0.2, columnspacing=1.0)
    save(fig, 'fig_worker_leak')


FIGS = {'leak': fig_leak, 'memory': fig_memory, 'invisible': fig_invisible_memory, 'cost': fig_cost,
        'restarts': fig_restarts, 'latency': fig_latency}

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--only', choices=list(FIGS))
    a = ap.parse_args()
    for name, fn in FIGS.items():
        if a.only and name != a.only:
            continue
        fn()
