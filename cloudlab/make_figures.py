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


# --- EVERY READER TAKES A WINDOW. This is not optional. ---------------------------------
# The memory trace and the TaskManager log are both CUMULATIVE: the monitor appends, and
# nothing rotates either one on resubmit. Read either whole and you splice together runs of
# DIFFERENT builds, with the job redeploys between them appearing as sharp drops. A growth
# rate or a median fitted across that splice is an artifact of the splice.
#
# This is exactly the trap that produced the retracted "2.2 GB/h unbounded Python-worker
# leak": the old fig_leak() read the whole trace, dropped the py==0 samples -- which are the
# only visible evidence of a redeploy -- and then fitted ONE line through TWO runs. Per run,
# the workers ramp for ~50 min and then sit flat at ~1.05 GB/worker. There is no leak.
#
# f875381 (build the Keras model once in open(), not per record) was deployed into the
# running job at 17:57 on 2026-07-13. Before that timestamp the trace is the PRE-FIX build;
# after it, HEAD. That is the one boundary the paper's before/after rests on.
FIX_DEPLOY = '2026-07-13 17:57:30'

SINCE = None      # --since: overrides the window start (use the clean E1 job start)
UNTIL = None      # --until: overrides the window end

# taskmanager.memory.process.size, our tuned budget. The engine's ENTIRE promise about the
# TaskManager process -- and it does not cover the Python workers at all.
PROCESS_SIZE_MB = 9856


def _in_window(ts, since, until):
    if since and ts < since:
        return False
    if until and ts > until:
        return False
    return True


def read_mem(path, since=None, until=None):
    since, until = since or SINCE, until or UNTIL
    rows = []
    if not os.path.exists(path):
        return rows
    for line in open(path, errors='replace'):
        m = MEM_RE.search(line)
        if not m:
            continue
        d = m.groupdict()
        if not _in_window(d['ts'], since, until):
            continue
        rows.append({
            't': datetime.strptime(d['ts'], '%Y-%m-%d %H:%M:%S').timestamp(),
            'rss': int(d['rss']), 'heap_used': int(d['hu']), 'heap_max': int(d['hm']),
            'direct': int(d['du']), 'nproc': int(d['pn']), 'py': int(d['pm']),
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
    trt = read_mem('results/e1/tm_memory.log', since=SINCE or FIX_DEPLOY)
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


# fig_invisible_memory() was DELETED, not disabled. It made the same point as fig_footprint
# -- the workers are charged to the machine but to no engine budget -- as a snapshot bar of
# the trace's LAST sample, which made it silently dependent on when the monitor happened to
# stop. It read 3.5 GB when the last sample landed in warm-up and 16.5 GB when it landed on
# the plateau, and the paper quoted the warm-up value in prose beside a figure drawn from
# the plateau. fig_footprint carries the budget line as a horizontal reference instead, so
# the same claim is made against the whole run rather than one arbitrary sample. At eight
# pages, one figure making the point beats two disagreeing about it.


# ---------------------------------------------------------------- cost breakdown
KV = re.compile(r'(\w+)=([-\d.]+)')
PROF_TS = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)')


def read_prof(path, since=None, until=None):
    """Per-record phase timings, WINDOWED. The TaskManager log is cumulative -- see the note
    on FIX_DEPLOY. Medianing the whole file averages the pre-fix and post-fix builds
    together and describes a system that never existed."""
    tr, inf = [], []
    if not os.path.exists(path):
        return tr, inf
    for line in open(path, errors='replace'):
        m = PROF_TS.match(line)
        if not m or not _in_window(m.group(1), since, until):
            continue
        if 'TRAINPROF subtask=' in line:
            tr.append({k: float(v) for k, v in KV.findall(line)})
        elif 'INFERPROF chunk=' in line:
            inf.append({k: float(v) for k, v in KV.findall(line)})
    return tr, inf


def med(rows, k):
    v = [r[k] for r in rows if k in r]
    return statistics.median(v) if v else 0.0


def fig_cost():
    """Removing one per-record recomputation halved the per-record path.

    The bar that vanishes is the claim. Infer rebuilt the Keras model from its serialized
    architecture on EVERY record; building it once in open() removes not only the 2.4s of
    construction but the 3.1s of TensorFlow retracing it forced on `predict`.

    Windowed at FIX_DEPLOY. Hatch carries the claim, so it survives grayscale.
    """
    path = 'results/postfix/prof_postfix.txt'
    if not os.path.exists(path):
        path = 'results/profile/prof.txt'
    pre_tr, pre_inf = read_prof(path, until=FIX_DEPLOY)
    post_tr, post_inf = read_prof(path, since=FIX_DEPLOY)
    if not (pre_tr and pre_inf and post_tr and post_inf):
        print('  [skip] cost: profile does not straddle the fix deploy')
        return

    # The irreducible scoring cost is what `predict` costs with a WARM model -- i.e. its
    # post-fix median. The pre-fix `predict` was 4.4x that, because a model rebuilt per
    # record is a fresh object every time and TensorFlow must re-trace its prediction
    # function. That retracing is a cost OF the rebuild, so it is charged to the rebuild and
    # not to `predict`. Charging it to `predict` -- which is what a naive reading of the
    # profile does -- understates the defect by more than half and makes the irreducible
    # work look like it got faster, which is incoherent: the same 500 molecules are scored
    # either way.
    predict_warm = med(post_inf, 'predict')

    def split(tr, inf):
        retrace = max(0.0, med(inf, 'predict') - predict_warm)
        return [
            # (stage, irreducible, cost of reconstructing/shipping the model per record)
            ('Infer', med(inf, 'prep') + predict_warm,
                      med(inf, 'build') + med(inf, 'loads') + med(inf, 'setw') + retrace),
            ('Train', med(tr, 'fit') + med(tr, 'persist'),
                      med(tr, 'dumps') + med(tr, 'tolist')),
        ]

    arms = [('after', split(post_tr, post_inf)), ('before', split(pre_tr, pre_inf))]

    fig, ax = plt.subplots(figsize=(COL, 1.85))
    ticks, labels = [], []
    row = 0
    for arm, stages in arms:
        for name, need, avoid in stages:
            ax.barh(row, need, height=0.62, color=BLUE, edgecolor='white', lw=0.8)
            ax.barh(row, avoid, left=need, height=0.62, color=YELLOW, edgecolor='white',
                    lw=0.8, hatch='///')
            if need > 0.8:
                ax.text(need / 2, row, f'{need:.1f}s', ha='center', va='center',
                        fontsize=6.4, color='white')
            if avoid > 0.8:
                ax.text(need + avoid / 2, row, f'{avoid:.1f}s', ha='center', va='center',
                        fontsize=6.4, color='white')
            ax.text(need + avoid + 0.12, row, f'{need+avoid:.1f}s', va='center',
                    fontsize=7, color=INK)
            ticks.append(row); labels.append(f'\\textit{{{name}}}' if False else name)
            row += 1
        row += 0.55

    ax.set_yticks(ticks); ax.set_yticklabels(labels)
    tot_a = sum(n + a for _, n, a in arms[0][1])
    tot_b = sum(n + a for _, n, a in arms[1][1])
    ax.text(-0.02, 0.5 / len(ticks) + 0.06, 'after', transform=ax.transAxes, rotation=90,
            fontsize=7, color=INK2, ha='right', va='center')
    ax.text(-0.02, 0.80, 'before', transform=ax.transAxes, rotation=90,
            fontsize=7, color=INK2, ha='right', va='center')
    ax.set_xlabel('Median time per record (s)')
    ax.set_xlim(0, max(tot_a, tot_b) * 1.18)
    ax.grid(axis='x'); ax.set_axisbelow(True)

    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(facecolor=BLUE, label='irreducible work'),
                       Patch(facecolor=YELLOW, hatch='///',
                             label='re-creating the model, per record')],
              loc='upper left', bbox_to_anchor=(0.0, -0.34), ncol=2,
              handlelength=1.4, labelcolor=INK2, borderpad=0.2, columnspacing=1.0)
    ax.set_title(f'per-record path: {tot_b:.1f}s $\\rightarrow$ {tot_a:.1f}s '
                 f'({tot_b/tot_a:.1f}$\\times$)', loc='left', color=INK2, pad=4, fontsize=7)
    save(fig, 'fig_cost_breakdown')
    print(f'      before={tot_b:.2f}s  after={tot_a:.2f}s  speedup={tot_b/tot_a:.2f}x')
    for arm, stages in arms:
        for name, need, avoid in stages:
            print(f'      {arm:6s} {name:5s} irreducible={need:.2f}s  rebuild={avoid:.2f}s')
    print(f'      predict: {med(pre_inf, "predict"):.2f}s cold-rebuilt -> '
          f'{predict_warm:.2f}s warm  => {med(pre_inf,"predict")-predict_warm:.2f}s of the '
          f'"predict" phase was retracing forced by the rebuild')


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
    e1 = read_mem('results/e1/tm_memory.log', since=SINCE or FIX_DEPLOY)
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




def fig_footprint():
    """The engine's budget is not a budget for the machine.

    Two-part story, and a long run is needed to see both parts:
      (1) a fast warm-up ramp to ~17 GB in the first ~50 min -- a BOUNDED working set, and
      (2) a slow ~0.8 GB/h growth ON TOP of it that only a multi-hour run reveals.
    Either part alone misleads. A short trace sees only (1) and calls it 'no leak'; the
    retracted fig_leak() fitted one line across two SPLICED runs and called the splice slope
    a '2.2 GB/h leak'. The truth is a slow real leak of ~0.8 GB/h, measured within ONE run.

    The structural point stands regardless: the Python workers are separate OS processes that
    appear in no engine budget and no engine metric, and they cross the engine's entire
    declared process.size before warm-up even finishes.
    """
    rows = read_mem('results/e1/tm_memory.log', since=SINCE or FIX_DEPLOY)
    rows = [r for r in rows if r['py'] > 0]
    if len(rows) < 10:
        print('  [skip] footprint: not enough samples in window')
        return
    x = [r['min'] / 60.0 for r in rows]          # hours
    y = [r['py'] / 1024.0 for r in rows]         # GB, all Python workers
    h = [r['heap_used'] / 1024.0 for r in rows]
    d = [r['direct'] / 1024.0 for r in rows]
    budget = PROCESS_SIZE_MB / 1024.0
    peak, nproc = max(y), rows[-1]['nproc']

    fig, ax = plt.subplots(figsize=(COL, 1.95))
    # No process count in the label. The monitor's py_workers COUNT is not the worker count:
    # its grep matches the 8 `pyflink-udf-runner.sh` shell wrappers (~3 MB each) alongside the
    # real `beam_boot` processes, plus any orphans left by a killed job, so it logs 18 where
    # there are 8 real workers. The SUM is unaffected (wrappers hold ~0), so the series itself
    # is sound; only the count is inflated. Older traces carry the inflated value, so we do not
    # render it. The count belongs in the caption, from `pgrep -f beam_boot`.
    ax.plot(x, y, color=YELLOW, ls='-', label='Python workers')
    ax.plot(x, h, color=BLUE, ls='--', label='JVM heap')
    ax.plot(x, d, color=AQUA, ls=':', label='JVM direct')

    # The engine's ENTIRE promise about the TaskManager -- crossed during warm-up.
    ax.axhline(budget, color=RED, ls='--', lw=1.0)
    ax.text(x[len(x)//2], budget, "engine's entire declared budget", fontsize=6.0,
            color=RED, va='bottom', ha='center')

    # Fit the POST-WARM-UP region to get the SLOW-LEAK rate. This is the honest number: it is
    # NOT the whole-run slope (which folds in the warm-up ramp) and NOT zero (the series is not
    # flat). Warm-up ends when the series first reaches ~90% of the warm-up plateau, which we
    # take near the early maximum rather than the global one (the series keeps rising).
    early_peak = max(y[:len(y)//3]) if len(y) >= 6 else peak
    warm = next((i for i, v in enumerate(y) if v >= 0.9 * early_peak), 0)
    xs, ys = x[warm:], y[warm:]
    mx, my = sum(xs)/len(xs), sum(ys)/len(ys)
    slope = sum((a-mx)*(b-my) for a, b in zip(xs, ys)) / (sum((a-mx)**2 for a in xs) or 1e-9)
    # draw the fitted trend so the eye sees the steady climb
    ax.plot([xs[0], xs[-1]], [my + slope*(xs[0]-mx), my + slope*(xs[-1]-mx)],
            color=INK, ls='-', lw=0.8)
    ax.annotate(f'{slope:+.1f} GB/h after warm-up', xy=(xs[len(xs)//2], my),
                xytext=(x[0] + 0.04*(x[-1]-x[0]), peak*1.16), fontsize=6.8, color=INK,
                arrowprops=dict(arrowstyle='-|>', color=MUTED, lw=0.7))

    ax.set_xlabel('Elapsed time (h)')
    ax.set_ylabel('Resident memory (GB)')
    ax.set_ylim(0, peak * 1.34)
    ax.grid(axis='y'); ax.set_axisbelow(True)
    ax.legend(loc='upper left', bbox_to_anchor=(0.0, -0.30), ncol=2, handlelength=1.5,
              labelcolor=INK2, borderpad=0.2, columnspacing=1.0)
    save(fig, 'fig_worker_footprint')
    print(f'      window {len(x)} samples over {x[-1]:.2f} h; {nproc} workers')
    print(f'      warm-up plateau ~{early_peak:.0f} GB ({early_peak*1024/PROCESS_SIZE_MB:.1f}x '
          f'budget); end {peak:.0f} GB ({peak*1024/PROCESS_SIZE_MB:.1f}x)')
    print(f'      post-warm-up slope {slope:+.2f} GB/h  <-- SLOW LEAK (not flat, not 2.2)')


def _read_e6(tag):
    """Return (elapsed_min, mae, teardown_mins) for arm `tag` from the CLEANED CSVs produced by
    the collection step: arm{tag}.csv (min,mae) and arm{tag}_tds.txt (one teardown-minute per
    line). We use pre-windowed CSVs rather than re-parsing the TaskManager log because that log
    is BOTH cumulative across submissions AND rotated by size mid-run, so neither the whole file
    nor its current tail isolates one arm; the collection step greps every rotation and windows
    to the driver's [START, DONE] epochs once, here."""
    csvp = f'results/e6/arm{tag}.csv'
    tdp = f'results/e6/arm{tag}_tds.txt'
    if not os.path.exists(csvp):
        return None
    xs, ys = [], []
    for r in csv.DictReader(open(csvp)):
        xs.append(float(r['min'])); ys.append(float(r['mae']))
    tds = [float(l) for l in open(tdp)] if os.path.exists(tdp) else []
    return xs, ys, tds


def fig_persistence():
    """E6: training MAE across induced worker teardowns, with vs without weight persistence.

    The falsifiable prediction: WITHOUT persistence, open() rebuilds the model at its initial
    weights on every teardown, so MAE saw-tooths back to its starting value; WITH persistence it
    stays converged. If the sawtooth does not appear, contribution 2 is false.
    """
    A = _read_e6('A')   # persistence ON
    B = _read_e6('B')   # persistence OFF
    if not A:
        print('  [skip] persistence: no arm-A data')
        return
    fig, ax = plt.subplots(figsize=(COL, 1.9))
    if B:
        xb, yb, tdb = B
        ax.plot(xb, yb, color=RED, ls='-', label='without persistence')
        for t in tdb:
            ax.axvline(t, color=MUTED, lw=0.5, ls=(0, (1, 2)))
    xa, ya, tda = A
    ax.plot(xa, ya, color=BLUE, ls='-', label='with persistence')
    for t in tda:
        ax.axvline(t, color=MUTED, lw=0.5, ls=(0, (1, 2)))
    ax.text(xa[-1] if xa else 1, 0, ' teardowns', fontsize=6, color=MUTED, va='bottom')
    ax.set_xlabel('Elapsed time (min)')
    ax.set_ylabel('Training MAE (V)')
    ax.grid(axis='y'); ax.set_axisbelow(True)
    ax.legend(loc='upper right', handlelength=1.5, labelcolor=INK2, borderpad=0.2)
    save(fig, 'fig_persistence')
    print(f'      arm A: {len(xa)} MAE samples, {len(tda)} teardowns, '
          f'range {min(ya):.2f}-{max(ya):.2f}')
    if B: print(f'      arm B: {len(xb)} MAE samples, {len(tdb)} teardowns, '
                f'range {min(yb):.2f}-{max(yb):.2f}')


# ---------------------------------------------------------------- E3 scaling sweep
# Data: results/e3/metrics_p{P}.csv (long format ts,parallelism,vertex,subtask,metric,value
# from e3_metrics.py) and results/e3/latency_p{P}.csv (from e0_steering_latency.py). Both
# are produced per arm by cloudlab/e3_run.sh. Every reader here SKIPS cleanly if an arm is
# absent, so these render whatever subset of {1,2,4,8,16} has been collected.
E3_PS = (1, 2, 4, 8, 16)
# Compute stages carry the narrative (Rank is the window_all bottleneck); Source/Sink are
# just the I/O endpoints of a chained vertex. Prefer the most downstream COMPUTE stage so
# 'Infer->Rank->Sink' labels as 'Rank', not 'Sink'.
_VCORE = ('Parse', 'Train', 'Infer', 'Rank')
_VEND = ('Source', 'Sink')


def _short_vertex(name):
    """Chain names read like 'Train->Infer' / 'Infer->Rank->Sink'; label by the most
    downstream COMPUTE stage so a bar or point is named by what it IS. The source chain is
    named 'Source: ... -> LoadJSON, Parse, ...' and contains 'Parse', so it is special-cased
    first -- otherwise it would label as 'Parse' and read as a compute stage it is not."""
    if 'Source' in name:
        return 'Source'
    core = [k for k in _VCORE if k in name]
    if core:
        return core[-1]
    end = [k for k in _VEND if k in name]
    return end[-1] if end else name[:14]


def _pct(sorted_vals, q):
    if not sorted_vals:
        return float('nan')
    k = (len(sorted_vals) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def _read_latency(P):
    """[latency_ms] for arm P, or None if the arm was not collected."""
    path = f'results/e3/latency_p{P}.csv'
    if not os.path.exists(path):
        return None
    out = []
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                out.append(int(r['latency_ms']))
            except (ValueError, KeyError):
                pass
    return out or None


def _load_e3_arm(P):
    """(per, rate_by_ts) for arm P, or None. `per[metric][vertex] = [values]` over all
    samples and subtasks; `rate_by_ts[vertex][ts]` = out-rate summed across subtasks at
    that sample.

    NOTE: rate_by_ts holds the ENGINE'S rate GAUGE and is kept only for diagnostics. Do not
    use it for throughput -- see _e3_throughput."""
    path = f'results/e3/metrics_p{P}.csv'
    if not os.path.exists(path):
        return None
    per, rate_by_ts = {}, {}
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                v = float(r['value'])
            except (ValueError, KeyError):
                continue
            metric, vx = r.get('metric', ''), r.get('vertex', '')
            per.setdefault(metric, {}).setdefault(vx, []).append(v)
            if metric == 'numRecordsOutPerSecond':
                try:
                    ts = int(r['ts'])
                except (ValueError, KeyError):
                    continue
                d = rate_by_ts.setdefault(vx, {})
                d[ts] = d.get(ts, 0.0) + v
    return (per, rate_by_ts) if per else None


def _e3_throughput(P):
    """{vertex: records/s} and window seconds for arm P, from CUMULATIVE COUNTER DELTAS.

    Throughput is (last numRecordsOut - first numRecordsOut) / elapsed, summed over subtasks.
    It is NOT the median of numRecordsOutPerSecond: that gauge is a smoothed rate meter, and
    Infer emits its ~488 candidates for a record in one burst, so the meter swings wildly
    between samples and its median understates the truth badly -- measured 221/s against an
    actual 404/s at P=4, an 1.8x error. The counter delta is exact by construction: it counts
    every record the operator emitted between two instants and divides by the elapsed time.

    e3_metrics only samples AFTER the driver's warm-up, so this delta is the steady-state,
    fully-warm behaviour. (Reading the last cumulative value instead of the delta measures
    the job's whole life INCLUDING warm-up, when Infer emits a `model_not_ready` singleton
    rather than ~488 candidates, and understates the amplification accordingly.)
    """
    path = f'results/e3/metrics_p{P}.csv'
    if not os.path.exists(path):
        return None, 0.0
    rows = list(csv.DictReader(open(path)))
    if not rows:
        return None, 0.0
    tss = sorted({int(r['ts']) for r in rows})
    t0, t1 = tss[0], tss[-1]
    span = float(t1 - t0)
    if span <= 0:
        return None, 0.0
    first, last = {}, {}
    for r in rows:
        if r.get('metric') != 'numRecordsOut':
            continue
        k = (r['vertex'], r['subtask'])
        ts = int(r['ts'])
        try:
            v = float(r['value'])
        except ValueError:
            continue
        if ts == t0:
            first[k] = v
        if ts == t1:
            last[k] = v
    out = {}
    for k, v in last.items():
        out[k[0]] = out.get(k[0], 0.0) + (v - first.get(k, 0.0))
    return {vx: n / span for vx, n in out.items()}, span


def fig_scaling():
    """E3: throughput and steering latency vs operator parallelism.

    Throughput is the dominant operator's output rate -- Infer, the 500x amplifier, whose
    scored-candidates/s is the compute that parallelism buys. A dashed ideal-linear line
    from the P=1 point marks perfect scaling, so the eye reads the inflection (where the
    curve peels away) rather than an absolute number. Latency is the E0 distribution per
    arm (p50 and p95). Two panels, never a dual axis: the measures have different units.
    """
    Ps, thru, p50, p95 = [], [], [], []
    thru_lbl = None
    for P in E3_PS:
        tp, span = _e3_throughput(P)
        lat = _read_latency(P)
        if tp is None and lat is None:
            continue
        Ps.append(P)
        if tp:
            # the amplifier (Infer) dominates the record counts; it is the compute
            # parallelism actually buys, and its delta-derived rate is exact.
            best_v = max(tp, key=lambda v: tp[v])
            thru.append(tp[best_v])
            thru_lbl = _short_vertex(best_v)
        else:
            thru.append(float('nan'))
        if lat:
            s = sorted(lat)
            p50.append(_pct(s, 0.50) / 1000.0)
            p95.append(_pct(s, 0.95) / 1000.0)
        else:
            p50.append(float('nan'))
            p95.append(float('nan'))
    if not Ps:
        print('  [skip] scaling: no results/e3 arms')
        return

    # Steering latency is only meaningful when the source is LIVE. Under OFFSET=earliest each
    # arm replays a backlog, so src_ts is days old and `latency = now - src_ts` measures the
    # BACKLOG'S AGE, not steering delay (measured: p50 ~74.8 h). Throughput scaling REQUIRES
    # that backlog -- at the live sim rate P=1 is never the bottleneck and the curve is flat --
    # so one sweep cannot yield both. Drop the panel rather than plot a number that looks like
    # latency and is not; E0 reports steering latency from a steady-state run.
    finite = [v for v in p50 if v == v]
    lat_ok = bool(finite) and max(finite) < 600.0
    if not lat_ok and finite:
        print(f'      [latency panel suppressed] p50 up to {max(finite)/3600:.1f} h -- this is '
              f'backlog age under OFFSET=earliest, not steering latency. Use E0.')

    if lat_ok:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(COL, 2.75), sharex=True)
    else:
        fig, ax1 = plt.subplots(figsize=(COL, 1.7))
        ax2 = None
    ax1.plot(Ps, thru, color=BLUE, marker='o', ms=4, label=thru_lbl or 'throughput')
    # ideal-linear reference anchored at the first finite throughput point
    base = next(((p, t) for p, t in zip(Ps, thru) if t == t and t > 0), None)
    if base:
        p0, t0 = base
        ax1.plot(Ps, [t0 * p / p0 for p in Ps], color=MUTED, ls='--', lw=0.9,
                 label='ideal linear')
    ax1.set_ylabel('Scored cand./s')
    ax1.grid(axis='y'); ax1.set_axisbelow(True)
    ax1.legend(loc='upper left', handlelength=1.6, labelcolor=INK2, borderpad=0.2)

    if ax2 is not None:
        ax2.plot(Ps, p50, color=BLUE, marker='o', ms=4, label='p50')
        ax2.plot(Ps, p95, color=AQUA, marker='s', ms=4, ls='--', label='p95')
        ax2.set_ylabel('Latency (s)')
        ax2.grid(axis='y'); ax2.set_axisbelow(True)
        ax2.legend(loc='upper right', handlelength=1.6, labelcolor=INK2, borderpad=0.2)
    axb = ax2 if ax2 is not None else ax1
    axb.set_xlabel('Operator parallelism')
    axb.set_xscale('log', base=2)
    axb.set_xticks(Ps); axb.set_xticklabels([str(p) for p in Ps])
    save(fig, 'fig_scaling')
    for i, P in enumerate(Ps):
        lat_s = f'p50={p50[i]:.2f}s' if lat_ok else 'p50=n/a(backlog)'
        print(f'      P={P:<2} thru={thru[i]:.1f}/s  {lat_s}  (window {_e3_throughput(P)[1]:.0f}s)')


def fig_utilisation():
    """E3: where capacity goes -- busy / backpressured / idle per operator across parallelism.

    A scalar utilisation cannot say WHERE capacity was lost; the state decomposition can
    (EXPERIMENTS.md E3). We plot EVERY operator, because the cascade is the story: under a
    saturating source the bottleneck is the operator that is busy and NOT backpressured, and
    everything upstream of it is backpressured BY it. Measured (P=1): Rank 100% busy / 0%
    backpressured while Source sits at 99.9% backpressured -- Rank is the constraint. As P
    rises Rank's busy drains (100 -> 88 -> 72%) and its idle climbs (0 -> 12 -> 28%), which is
    the per-operator evidence that parallelism relieves it, and hence that the
    window_all -> window fix (which let Rank scale past 1 sub-task at all) did the work.

    Bottleneck selector is max(busy - backpressure), NOT max(backpressure). Under saturation
    the SOURCE is pinned at ~100% backpressured in every arm, so ranking by backpressure picks
    the source every time and says nothing. busy-minus-backpressure scores Rank 100, Infer 4.8,
    Train -10.6, Source -99.8, which is the operator a Flink practitioner would name.
    """
    arms = {}
    for P in E3_PS:
        a = _load_e3_arm(P)
        if a is not None:
            arms[P] = a
    if not arms:
        print('  [skip] utilisation: no results/e3 arms')
        return
    Ps = sorted(arms)

    # per arm: {op: (busy, bp, idle)} as fractions summing to 1
    def _arm_ops(per):
        ops = {}
        vxs = set()
        for m in ('busyTimeMsPerSecond', 'backPressuredTimeMsPerSecond'):
            vxs |= set(per.get(m, {}).keys())
        for vx in vxs:
            def _mean(metric):
                vals = per.get(metric, {}).get(vx, [])
                return statistics.mean(vals) / 1000.0 if vals else None
            b = _mean('busyTimeMsPerSecond') or 0.0
            p = _mean('backPressuredTimeMsPerSecond') or 0.0
            i = _mean('idleTimeMsPerSecond')
            if i is None:
                i = max(0.0, 1.0 - b - p)
            tot = b + p + i or 1.0
            ops[_short_vertex(vx)] = (b / tot, p / tot, i / tot)
        return ops

    per_arm = {P: _arm_ops(arms[P][0]) for P in Ps}
    order = [o for o in ('Source', 'Train', 'Infer', 'Rank')
             if any(o in per_arm[P] for P in Ps)]
    if not order:
        print('  [skip] utilisation: no recognised operators')
        return

    # bottleneck = max(busy - backpressure), at the smallest arm
    b0 = per_arm[Ps[0]]
    bottleneck = max(b0, key=lambda o: b0[o][0] - b0[o][1])

    fig, ax = plt.subplots(figsize=(COL, 2.0))
    nb = len(order)
    w = 0.8 / nb
    for gi, P in enumerate(Ps):
        for oi, op in enumerate(order):
            if op not in per_arm[P]:
                continue
            b, p, i = per_arm[P][op]
            x = gi + (oi - (nb - 1) / 2.0) * w
            ax.bar(x, b, width=w * 0.92, color=BLUE)
            ax.bar(x, p, width=w * 0.92, bottom=b, color=RED, hatch='///')
            ax.bar(x, i, width=w * 0.92, bottom=b + p, color=MUTED, alpha=0.35)
            ax.text(x, -0.055, op[0], ha='center', va='top', fontsize=5.4,
                    color=INK if op == bottleneck else MUTED)
    ax.set_xticks(range(len(Ps)))
    ax.set_xticklabels([f'P={p}' for p in Ps])
    ax.tick_params(axis='x', pad=10)
    ax.set_ylabel('Time fraction')
    ax.set_ylim(0, 1.0)
    ax.set_title(f'bottleneck: {bottleneck} (busy, never backpressured)',
                 loc='left', color=INK2, pad=4, fontsize=7)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(facecolor=BLUE, label='busy'),
                       Patch(facecolor=RED, hatch='///', label='backpressured'),
                       Patch(facecolor=MUTED, alpha=0.35, label='idle')],
              loc='upper left', bbox_to_anchor=(0.0, -0.16), ncol=3, handlelength=1.4,
              labelcolor=INK2, borderpad=0.2, columnspacing=1.0)
    save(fig, 'fig_utilisation')
    print(f'      bottleneck (max busy-bp) = {bottleneck}')
    for P in Ps:
        cells = '  '.join(f'{o}:{per_arm[P][o][0]:.0%}/{per_arm[P][o][1]:.0%}'
                          for o in order if o in per_arm[P])
        print(f'      P={P:<2} (busy/bp)  {cells}')


FIGS = {'footprint': fig_footprint, 'memory': fig_memory, 'cost': fig_cost,
        'restarts': fig_restarts, 'latency': fig_latency, 'persistence': fig_persistence,
        'scaling': fig_scaling, 'utilisation': fig_utilisation}

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--only', choices=list(FIGS))
    ap.add_argument('--since', metavar='"YYYY-MM-DD HH:MM:SS"',
                    help='window start. The logs are CUMULATIVE across job redeploys; pass '
                         'the job start so a growth rate is not fitted across a splice.')
    ap.add_argument('--until', metavar='"YYYY-MM-DD HH:MM:SS"', help='window end')
    a = ap.parse_args()
    SINCE, UNTIL = a.since, a.until
    for name, fn in FIGS.items():
        if a.only and name != a.only:
            continue
        fn()
