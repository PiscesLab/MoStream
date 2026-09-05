#!/usr/bin/env python3
"""Fig 2 redesign: how long the job stays up under each memory budget (E2).
Default budget: mean 81 s between out-of-memory crashes (16 in 20 min, then killed).
Our budget: 24.2 h with zero crashes. A log axis makes the ~1000x gap immediate.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, RED, INK, INK2, MUTED = '#2a78d6', '#e34948', '#0b0b0b', '#52514e', '#8a8985'
OUT='paper/Figures'; COL=3.3
plt.rcParams.update({
    'font.size':8,'axes.labelsize':8,'xtick.labelsize':7.5,'ytick.labelsize':7,
    'font.family':'serif','font.serif':['Times New Roman','DejaVu Serif'],
    'axes.spines.top':False,'axes.spines.right':False,'axes.edgecolor':MUTED,
    'axes.labelcolor':INK,'text.color':INK,'xtick.color':INK2,'ytick.color':INK2,
    'figure.dpi':200,'savefig.bbox':'tight','savefig.pad_inches':0.02})

# continuous run before a crash, in seconds
default_s = 81.0            # mean inter-failure time (16 crashes in 20 min)
ours_s    = 24.2*3600       # 24.2 h with zero crashes

fig, ax = plt.subplots(figsize=(COL, 1.95))
bars = ax.bar([0,1], [default_s, ours_s], width=0.55, color=[RED, BLUE],
              edgecolor='white', linewidth=0.8, zorder=3)
ax.set_yscale('log')
ax.set_ylim(10, ours_s*3)
ax.set_xticks([0,1])
ax.set_xticklabels(["engine default\n(16 crashes, then killed)", "our budget\n(0 crashes)"])
ax.set_ylabel('uninterrupted run\nbefore a crash')
# readable value above each bar
ax.text(0, default_s*1.5, '81 s', ha='center', va='bottom', fontsize=9, color=RED, fontweight='bold')
ax.text(1, ours_s*1.15, '24.2 h', ha='center', va='bottom', fontsize=9, color=BLUE, fontweight='bold')
# make y ticks human: seconds/minutes/hours
import matplotlib.ticker as mt
ax.yaxis.set_major_locator(mt.LogLocator(base=10, numticks=6))
ax.set_yticklabels([])  # values are annotated on the bars; hide raw log ticks for clarity
ax.tick_params(axis='y', length=0)
ax.set_xlim(-0.6, 1.9)
import os; os.makedirs(OUT, exist_ok=True)
for e in ('pdf','png'): fig.savefig(f'{OUT}/fig_restarts.{e}')
print('  wrote', OUT+'/fig_restarts.pdf')
