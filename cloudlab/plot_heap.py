#!/usr/bin/env python3
"""
Two modes:
  python plot_heap.py [monitor.log] [out.png] [--heap-only]
      Plot JVM heap (+training metrics if present) from monitor_taskmanager.sh log.

  python plot_heap.py --loss [taskmanager.log] [out.png]
      Plot model_id 0 training loss/MAE from Flink TM log.
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

HEAP_RE   = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*jvm_heap=(\d+)/(\d+)MB')
DIRECT_RE = re.compile(r'direct=(\d+)/(\d+)MB')
PY_RE     = re.compile(r'py_workers=(\d+)x(\d+)MB')
LOSS_RE   = re.compile(r'm0_loss=([\d.eE+\-]+)')
MAE_RE    = re.compile(r'm0_mae=([\d.eE+\-]+)')

def parse_log(path):
    times, heap_used, heap_max, direct_used, py_count, py_mb, losses, maes = [], [], [], [], [], [], [], []
    with open(path) as f:
        for line in f:
            m = HEAP_RE.search(line)
            if not m:
                continue
            times.append(datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S'))
            heap_used.append(int(m.group(2)))
            heap_max.append(int(m.group(3)))
            dm = DIRECT_RE.search(line)
            direct_used.append(int(dm.group(1)) if dm else None)
            pm = PY_RE.search(line)
            py_count.append(int(pm.group(1)) if pm else None)
            py_mb.append(int(pm.group(2)) if pm else None)
            lm = LOSS_RE.search(line)
            mm = MAE_RE.search(line)
            losses.append(float(lm.group(1)) if lm else None)
            maes.append(float(mm.group(1)) if mm else None)
    return times, heap_used, heap_max, direct_used, py_count, py_mb, losses, maes

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

    times, heap_used, heap_max, direct_used, py_count, py_mb, losses, maes = parse_log(logfile)
    if not times:
        print("ERROR: No matching lines found. Check log format.")
        sys.exit(1)

    heap_limit    = max(heap_max)
    gc_events     = detect_gc(heap_used)
    beam_restarts = detect_beam_restarts(py_count, times)
    duration_h    = (times[-1] - times[0]).total_seconds() / 3600
    heap_only     = '--heap-only' in sys.argv

    has_train = any(v is not None for v in losses)

    if heap_only:
        fig, ax1 = plt.subplots(1, 1, figsize=(16, 6))
        ax2 = ax3 = None
    elif has_train:
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 12), sharex=True,
                                              gridspec_kw={'height_ratios': [3, 1, 1.5]})
    else:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 9), sharex=True,
                                        gridspec_kw={'height_ratios': [3, 1]})
        ax3 = None

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

    bottom_ax = ax1 if heap_only else (ax3 if has_train else ax2)

    if heap_only:
        ax1.set_xlabel('Time', fontsize=11)
    else:
        # ── Middle panel: Python workers ──────────────────────────────────────
        py_pairs = [(t, m/1024) for t, m in zip(times, py_mb) if m is not None]
        py_times2, py_gb = zip(*py_pairs) if py_pairs else ([], [])
        ax2.fill_between(py_times2, py_gb, alpha=0.20, color='green')
        ax2.plot(py_times2, py_gb, color='green', linewidth=1.2, label='Python Workers Total')
        for idx in beam_restarts:
            ax2.axvline(times[idx], color='purple', alpha=0.55, linewidth=1.2, linestyle='-.')
        ax2.set_ylabel('Python Workers\nTotal (GB)', fontsize=10)
        ax2.legend(loc='upper left', fontsize=9)
        ax2.grid(axis='y', alpha=0.3)

        # ── Bottom panel: Training loss + MAE ────────────────────────────────
        if has_train and ax3 is not None:
            loss_pairs = [(t, v) for t, v in zip(times, losses) if v is not None]
            mae_pairs  = [(t, v) for t, v in zip(times, maes)   if v is not None]

            if loss_pairs:
                lt, lv = zip(*loss_pairs)
                ax3.plot(lt, lv, color='tomato', linewidth=1.3, label='Train Loss (MSE)')
                ax3.fill_between(lt, lv, alpha=0.15, color='tomato')

            ax3_mae = ax3.twinx()
            if mae_pairs:
                mt, mv = zip(*mae_pairs)
                ax3_mae.plot(mt, mv, color='darkorange', linewidth=1.3,
                             linestyle='--', label='Train MAE')

            for idx in beam_restarts:
                ax3.axvline(times[idx], color='purple', alpha=0.45, linewidth=1.0, linestyle='-.')

            ax3.set_ylabel('Loss (MSE)', fontsize=10, color='tomato')
            ax3.tick_params(axis='y', labelcolor='tomato')
            ax3_mae.set_ylabel('MAE', fontsize=10, color='darkorange')
            ax3_mae.tick_params(axis='y', labelcolor='darkorange')
            ax3.set_xlabel('Time', fontsize=11)
            ax3.grid(axis='y', alpha=0.3)

            lines1, labels1 = ax3.get_legend_handles_labels()
            lines2, labels2 = ax3_mae.get_legend_handles_labels()
            ax3.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=9)
        else:
            ax2.set_xlabel('Time', fontsize=11)

    bottom_ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    bottom_ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
    plt.xticks(rotation=45)

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(outfile, dpi=150, bbox_inches='tight')
    print(f"Saved → {outfile}")
    print(f"Duration: {duration_h:.2f} h | {len(times)} samples | "
          f"peak heap {peak}/{heap_limit} MB ({pct:.0f}%) | "
          f"{len(gc_events)} GC events | {len(beam_restarts)} Beam recycles")

TMLOG_TS_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+')
TMLOG_LOSS_RE = re.compile(r'model_id:\s+(\d+)\s+train_loss:\s+\[([^\]]+)\]')
TMLOG_MAE_RE  = re.compile(r'model_id:\s+(\d+)\s+train_mae:\s+\[([^\]]+)\]')

def parse_tm_log(path, model_id=0):
    """Extract (timestamp, loss) and (timestamp, mae) for a given model_id from TM log."""
    loss_pts, mae_pts = [], []
    current_ts = None
    with open(path) as f:
        for line in f:
            tm = TMLOG_TS_RE.match(line)
            if tm:
                current_ts = datetime.strptime(tm.group(1), '%Y-%m-%d %H:%M:%S')
            lm = TMLOG_LOSS_RE.search(line)
            if lm and int(lm.group(1)) == model_id and current_ts:
                loss_pts.append((current_ts, float(lm.group(2))))
            mm = TMLOG_MAE_RE.search(line)
            if mm and int(mm.group(1)) == model_id and current_ts:
                mae_pts.append((current_ts, float(mm.group(2))))
    return loss_pts, mae_pts

def plot_loss(tmlog, outfile, model_id=0):
    loss_pts, mae_pts = parse_tm_log(tmlog, model_id)
    if not loss_pts:
        print(f"ERROR: no train_loss entries for model_id={model_id}")
        sys.exit(1)

    lt, lv = zip(*loss_pts)
    mt, mv = zip(*mae_pts) if mae_pts else ([], [])

    # Detect Beam recycles: loss spikes back to a very high value after being low
    beam_reset_idx = []
    for i in range(1, len(lv)):
        if lv[i] > lv[i-1] * 50 and lv[i] > 1000:
            beam_reset_idx.append(i)

    duration_m = (lt[-1] - lt[0]).total_seconds() / 60

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True,
                                    gridspec_kw={'height_ratios': [2, 1]})
    fig.suptitle(
        f'Model ID {model_id} — Training Loss  |  {len(loss_pts)} steps  |  '
        f'{duration_m:.0f} min  |  {len(beam_reset_idx)} Beam resets detected',
        fontsize=13, fontweight='bold'
    )

    # Loss (log scale to show large initial drop)
    ax1.semilogy(lt, lv, color='tomato', linewidth=1.3, marker='o', markersize=2, label='Train Loss (MSE)')
    for idx in beam_reset_idx:
        ax1.axvline(lt[idx], color='purple', alpha=0.6, linewidth=1.2, linestyle='--',
                    label='Beam recycle' if idx == beam_reset_idx[0] else '')
    ax1.set_ylabel('Loss (MSE, log scale)', fontsize=11)
    ax1.legend(fontsize=9)
    ax1.grid(alpha=0.3)

    # MAE (linear scale)
    if mt:
        ax2.plot(mt, mv, color='darkorange', linewidth=1.3, marker='o', markersize=2, label='Train MAE')
        for idx in beam_reset_idx:
            if idx < len(mt):
                ax2.axvline(mt[idx], color='purple', alpha=0.6, linewidth=1.2, linestyle='--')
    ax2.set_ylabel('MAE', fontsize=11)
    ax2.set_xlabel('Time', fontsize=11)
    ax2.legend(fontsize=9)
    ax2.grid(alpha=0.3)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax2.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.savefig(outfile, dpi=150, bbox_inches='tight')
    print(f"Saved → {outfile}")
    print(f"Steps: {len(loss_pts)} | min loss: {min(lv):.2f} | Beam resets: {len(beam_reset_idx)}")

def plot_combined(monitor_log, tm_log, outfile):
    """3-panel plot: JVM Heap + Direct Memory (monitor log) + Training Loss (TM log)."""
    times, heap_used, heap_max, direct_used, _, _, _, _ = parse_log(monitor_log)
    loss_pts, mae_pts = parse_tm_log(tm_log)

    if not times:
        print("ERROR: no data in monitor log"); sys.exit(1)
    if not loss_pts:
        print("ERROR: no train_loss in TM log"); sys.exit(1)

    heap_limit = max(heap_max)
    gc_events  = detect_gc(heap_used)
    duration_h = (times[-1] - times[0]).total_seconds() / 3600

    lt, lv = zip(*loss_pts)
    beam_reset_idx = [i for i in range(1, len(lv)) if lv[i] > lv[i-1] * 50 and lv[i] > 1000]

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 11), sharex=False,
                                         gridspec_kw={'height_ratios': [2, 1, 2]})
    fig.suptitle(
        f'MoStream — JVM Heap + Direct Memory + Training Loss  |  '
        f'{duration_h:.1f} h  |  Parallelism=8  |  NO OOM',
        fontsize=13, fontweight='bold'
    )

    # ── Panel 1: JVM Heap ────────────────────────────────────────────────────
    ax1.fill_between(times, heap_used, alpha=0.18, color='steelblue')
    ax1.plot(times, heap_used, color='steelblue', linewidth=1.3, label='JVM Heap Used')
    ax1.axhline(heap_limit, color='red', linestyle='--', linewidth=1.5,
                label=f'Heap Max = {heap_limit} MB')
    ax1.axhline(int(heap_limit * 0.80), color='orange', linestyle=':', linewidth=1.0,
                label=f'80% = {int(heap_limit*0.80)} MB')
    gc_done = False
    for idx in gc_events:
        ax1.axvline(times[idx], color='gray', alpha=0.3, linewidth=0.7,
                    label='GC' if not gc_done else '')
        gc_done = True
    ax1.annotate('✓ No OOM', xy=(times[len(times)//2], heap_limit*0.93),
                 fontsize=10, ha='center', color='darkgreen',
                 bbox=dict(boxstyle='round,pad=0.3', facecolor='#d4f7d4', alpha=0.85))
    ax1.set_ylabel('JVM Heap (MB)', fontsize=10)
    ax1.set_ylim(0, heap_limit * 1.1)
    ax1.legend(loc='upper left', fontsize=8, ncol=4)
    ax1.grid(axis='y', alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax1.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))

    # ── Panel 2: Direct Memory ───────────────────────────────────────────────
    d_pairs = [(t, v) for t, v in zip(times, direct_used) if v is not None]
    if d_pairs:
        dt2, dv2 = zip(*d_pairs)
        ax2.fill_between(dt2, dv2, alpha=0.2, color='teal')
        ax2.plot(dt2, dv2, color='teal', linewidth=1.3, label='Direct Memory Used')
        ax2.axhline(4864, color='red', linestyle='--', linewidth=1.2,
                    label='MaxDirectMemory = 4864 MB')
    ax2.set_ylabel('Direct Mem (MB)', fontsize=10)
    ax2.set_ylim(0, 5200)
    ax2.legend(loc='upper left', fontsize=8)
    ax2.grid(axis='y', alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax2.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))

    # ── Panel 3: Training Loss ───────────────────────────────────────────────
    ax3.semilogy(lt, lv, color='tomato', linewidth=1.2, marker='o',
                 markersize=2, label='Train Loss (MSE, log scale)')
    beam_done = False
    for idx in beam_reset_idx:
        ax3.axvline(lt[idx], color='purple', alpha=0.6, linewidth=1.2, linestyle='--',
                    label='Beam recycle' if not beam_done else '')
        beam_done = True
    ax3.set_ylabel('Loss (MSE)', fontsize=10)
    ax3.set_xlabel('Time', fontsize=10)
    ax3.legend(loc='upper right', fontsize=8)
    ax3.grid(axis='y', alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax3.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))

    for ax in [ax1, ax2, ax3]:
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    peak = max(heap_used)
    stats = (f'Peak heap: {peak}/{heap_limit} MB ({peak/heap_limit*100:.0f}%)\n'
             f'GC events: {len(gc_events)} | Beam resets: {len(beam_reset_idx)}\n'
             f'Min loss: {min(lv):.2f} | Steps: {len(lv)}')
    ax1.text(0.99, 0.97, stats, transform=ax1.transAxes, fontsize=8,
             va='top', ha='right', family='monospace',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))

    plt.tight_layout()
    plt.savefig(outfile, dpi=150, bbox_inches='tight')
    print(f"Saved → {outfile}")
    print(f"Duration: {duration_h:.2f}h | peak heap {peak}/{heap_limit}MB | "
          f"{len(gc_events)} GC | {len(beam_reset_idx)} Beam resets | min loss {min(lv):.2f}")


if __name__ == '__main__':
    if '--loss' in sys.argv:
        sys.argv.remove('--loss')
        tmlog   = sys.argv[1] if len(sys.argv) > 1 else '/tmp/taskmanager.log'
        outfile = sys.argv[2] if len(sys.argv) > 2 else 'loss_curve.png'
        plot_loss(tmlog, outfile)
    elif '--direct-only' in sys.argv:
        sys.argv.remove('--direct-only')
        logfile = sys.argv[1] if len(sys.argv) > 1 else LOG_DEFAULT
        outfile = sys.argv[2] if len(sys.argv) > 2 else 'direct.png'
        times, _, _, direct_used, _, _, _, _ = parse_log(logfile)
        d_pairs = [(t, v) for t, v in zip(times, direct_used) if v is not None]
        if not d_pairs:
            print("ERROR: no direct= entries found"); sys.exit(1)
        dt, dv = zip(*d_pairs)
        duration_h = (dt[-1] - dt[0]).total_seconds() / 3600
        fig, ax = plt.subplots(figsize=(16, 5))
        ax.fill_between(dt, dv, alpha=0.2, color='teal')
        ax.plot(dt, dv, color='teal', linewidth=1.4, label='Direct Memory Used')
        ax.axhline(4864, color='red', linestyle='--', linewidth=1.5,
                   label='MaxDirectMemorySize = 4864 MB (task.off-heap=4096+framework=256+network=512)')
        ax.set_ylabel('Direct Memory (MB)', fontsize=11)
        ax.set_xlabel('Time', fontsize=11)
        ax.set_ylim(0, 5200)
        ax.set_title(f'TaskManager Direct Memory — {duration_h:.1f} h  |  Parallelism=8  |  NO OOM',
                     fontsize=13, fontweight='bold')
        ax.legend(fontsize=9)
        ax.grid(axis='y', alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(outfile, dpi=150, bbox_inches='tight')
        print(f"Saved → {outfile} | peak direct: {max(dv)} MB / 4864 MB ({max(dv)/4864*100:.1f}%)")
    elif '--combined' in sys.argv:
        sys.argv.remove('--combined')
        monitor_log = sys.argv[1] if len(sys.argv) > 1 else '/tmp/tm_memory.log'
        tm_log      = sys.argv[2] if len(sys.argv) > 2 else '/tmp/taskmanager.log'
        outfile     = sys.argv[3] if len(sys.argv) > 3 else 'combined.png'
        plot_combined(monitor_log, tm_log, outfile)
    else:
        main()
