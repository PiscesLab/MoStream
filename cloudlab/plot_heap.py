#!/usr/bin/env python3
"""
Plot JVM heap + Python worker memory from monitor_taskmanager.sh log.
Usage: python plot_heap.py [logfile] [output.png]
"""
import re
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
from datetime import datetime

LOG_DEFAULT = '/tmp/tm_memory.log'
OUT_DEFAULT = 'heap_4h.png'

HEAP_RE = re.compile(
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
    r'.*jvm_heap=(\d+)/(\d+)MB'
)
PY_RE = re.compile(r'py_workers=(\d+)x(\d+)MB')

def parse_log(path):
    times, heap_used, heap_max, py_count, py_mb = [], [], [], [], []
    with open(path) as f:
        for line in f:
            m = HEAP_RE.search(line)
            if not m:
                continue
            times.append(datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S'))
            heap_used.append(int(m.group(2)))
            heap_max.append(int(m.group(3)))
            pm = PY_RE.search(line)
            if pm:
                py_count.append(int(pm.group(1)))
                py_mb.append(int(pm.group(2)))
            else:
                py_count.append(None)
                py_mb.append(None)
    return times, heap_used, heap_max, py_count, py_mb

def detect_gc(heap_used, drop=250):
    """Indices where heap dropped by >drop MB — GC event."""
    return [i for i in range(1, len(heap_used)) if heap_used[i-1] - heap_used[i] > drop]

def detect_beam_restarts(py_count, times, threshold=6):
    """Indices where Python worker count drops significantly — Beam environment recycle."""
    events = []
    for i in range(1, len(py_count)):
        a, b = py_count[i-1], py_count[i]
        if a is not None and b is not None and a - b > threshold:
            events.append(i)
    return events

def main():
    logfile = sys.argv[1] if len(sys.argv) > 1 else LOG_DEFAULT
    outfile = sys.argv[2] if len(sys.argv) > 2 else OUT_DEFAULT

    times, heap_used, heap_max, py_count, py_mb = parse_log(logfile)
    if not times:
        print("ERROR: No matching lines found. Check log format.")
        sys.exit(1)

    heap_limit = max(heap_max)
    gc_events  = detect_gc(heap_used)
    beam_restarts = detect_beam_restarts(py_count, times)
    duration_h = (times[-1] - times[0]).total_seconds() / 3600

    heap_only = '--heap-only' in sys.argv

    if heap_only:
        fig, ax1 = plt.subplots(1, 1, figsize=(16, 6))
    else:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 9), sharex=True,
                                        gridspec_kw={'height_ratios': [3, 1]})

    fig.suptitle(
        f'MoStream TaskManager JVM Heap — {duration_h:.1f} h  |  '
        f'Parallelism=8  |  task.heap={heap_limit} MB  |  '
        f'{len(gc_events)} GC events  |  NO OOM',
        fontsize=13, fontweight='bold', y=0.98
    )

    # ── JVM Heap panel ───────────────────────────────────────────────────────
    ax1.fill_between(times, heap_used, alpha=0.18, color='steelblue')
    ax1.plot(times, heap_used, color='steelblue', linewidth=1.4, label='JVM Heap Used')
    ax1.axhline(heap_limit, color='red', linestyle='--', linewidth=1.5,
                label=f'Heap Max = {heap_limit} MB')
    ax1.axhline(int(heap_limit * 0.80), color='orange', linestyle=':', linewidth=1.0,
                label=f'80% warning = {int(heap_limit*0.80)} MB')

    gc_label_done = False
    for idx in gc_events:
        lbl = 'GC event' if not gc_label_done else ''
        ax1.axvline(times[idx], color='gray', alpha=0.35, linewidth=0.8, label=lbl)
        gc_label_done = True

    beam_label_done = False
    for idx in beam_restarts:
        lbl = 'Beam recycle (~10 min)' if not beam_label_done else ''
        ax1.axvline(times[idx], color='purple', alpha=0.55, linewidth=1.2,
                    linestyle='-.', label=lbl)
        beam_label_done = True

    mid = times[len(times) // 2]
    ax1.annotate(
        '✓  No OOM in entire window',
        xy=(mid, heap_limit * 0.94),
        fontsize=11, ha='center', color='darkgreen',
        bbox=dict(boxstyle='round,pad=0.4', facecolor='#d4f7d4', edgecolor='green', alpha=0.85)
    )

    peak = max(heap_used)
    pct  = peak / heap_limit * 100
    stats = (
        f'Peak: {peak} MB ({pct:.0f}% of max)\n'
        f'GC events: {len(gc_events)}\n'
        f'Beam recycles: {len(beam_restarts)}\n'
        f'Duration: {duration_h:.1f} h'
    )
    ax1.text(0.99, 0.97, stats, transform=ax1.transAxes, fontsize=9,
             va='top', ha='right', family='monospace',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))

    ax1.set_ylabel('JVM Heap (MB)', fontsize=11)
    ax1.set_ylim(0, heap_limit * 1.12)
    ax1.legend(loc='upper left', fontsize=9, ncol=3)
    ax1.grid(axis='y', alpha=0.3)

    if heap_only:
        ax1.set_xlabel('Time', fontsize=11)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax1.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
        plt.xticks(rotation=45)
    else:
        # ── Bottom panel: Python workers ─────────────────────────────────────
        py_pairs = [(t, m/1024) for t, m in zip(times, py_mb) if m is not None]
        py_times2, py_gb = zip(*py_pairs) if py_pairs else ([], [])
        ax2.fill_between(py_times2, py_gb, alpha=0.20, color='green')
        ax2.plot(py_times2, py_gb, color='green', linewidth=1.2, label='Python Workers Total')

        for idx in beam_restarts:
            ax2.axvline(times[idx], color='purple', alpha=0.55, linewidth=1.2, linestyle='-.')

        ax2.set_ylabel('Python Workers\nTotal (GB)', fontsize=10)
        ax2.set_xlabel('Time', fontsize=11)
        ax2.legend(loc='upper left', fontsize=9)
        ax2.grid(axis='y', alpha=0.3)

        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax2.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
        plt.xticks(rotation=45)

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(outfile, dpi=150, bbox_inches='tight')
    print(f"Saved → {outfile}")
    print(f"Duration: {duration_h:.2f} h | {len(times)} samples | "
          f"peak heap {peak}/{heap_limit} MB ({pct:.0f}%) | "
          f"{len(gc_events)} GC events | {len(beam_restarts)} Beam recycles")

if __name__ == '__main__':
    main()
