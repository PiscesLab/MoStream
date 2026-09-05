#!/usr/bin/env python3
"""E4 fault-recovery probe. Run on the JobManager. Two modes:

  e4_probe.py state <jid>      # one-shot snapshot: job state, throughput, latest checkpoint
  e4_probe.py throughput <jid> # numRecordsIn on Source over a 15 s window -> records/s

Throughput is a COUNTER DELTA over a wall-clock window, not the engine's rate gauge, for the
same reason as E3: the gauge is smoothed and lags a step change like a fault, while the delta is
exact. This is what makes "time to recovered throughput" measurable: sample the delta repeatedly
and watch it return to its pre-fault value.

Only stdlib; the JobManager has no extra packages.
"""
import json
import sys
import time
from urllib.request import urlopen

JM = "http://10.10.1.4:8081"


def get(path, tries=3):
    for i in range(tries):
        try:
            with urlopen(JM + path, timeout=8) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if i == tries - 1:
                return {"_err": str(e)}
            time.sleep(0.5)


def source_records(jid):
    """cumulative numRecordsIn summed over the Source vertex's subtasks, or None."""
    d = get(f"/jobs/{jid}")
    if not d or "_err" in d:
        return None
    for v in d.get("vertices", []):
        if "Source" in v.get("name", ""):
            try:
                return int(v["metrics"]["read-records"])
            except (KeyError, ValueError, TypeError):
                return None
    return None


def snapshot(jid):
    d = get(f"/jobs/{jid}")
    if not d or "_err" in d:
        print(f"job {jid}: UNREACHABLE ({d.get('_err') if d else 'no data'})")
        return
    print(f"state={d.get('state')} duration={d.get('duration',0)/1000.0:.0f}s")
    for v in d.get("vertices", []):
        m = v.get("metrics", {})
        print(f"  {v['name'][:40]:40s} {v.get('status'):11s} in={m.get('read-records')} out={m.get('write-records')}")
    c = get(f"/jobs/{jid}/checkpoints")
    if c and "_err" not in c:
        cc = c.get("counts", {})
        latest = (c.get("latest") or {}).get("completed") or {}
        restored = c.get("latest", {}).get("restored") or {}
        print(f"  checkpoints: completed={cc.get('completed')} failed={cc.get('failed')} "
              f"restored={cc.get('restored')}")
        print(f"  latest completed id={latest.get('id')}  restored_from={restored.get('id')} "
              f"(external={restored.get('is_savepoint')})")


def throughput(jid, window=15):
    a = source_records(jid)
    t0 = time.time()
    if a is None:
        print("throughput=NA (source metric unavailable; job restarting?)")
        return
    time.sleep(window)
    b = source_records(jid)
    dt = time.time() - t0
    if b is None:
        print("throughput=NA (source metric unavailable at t1)")
        return
    print(f"throughput={ (b-a)/dt :.3f} rec/s  (delta {b-a} over {dt:.0f}s, cum={b})")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "state"
    jid = sys.argv[2] if len(sys.argv) > 2 else ""
    if mode == "throughput":
        throughput(jid)
    else:
        snapshot(jid)
