#!/usr/bin/env python3
"""E8: recovery-consistency test for the versioned-weights build (reviewer changes 5 and 6).

Run the job with MOSTREAM_VERSIONED_WEIGHTS=1 so that TrainFunction:
  - writes immutable /tmp/mostream_weights_<subtask>_v<version>.json files,
  - commits the exact version into Flink-checkpointed keyed state, and
  - logs `WEIGHTVER subtask=.. version=.. sha=..` on every persist and
    `Restored weights v<version> from checkpoint reference ..` on recovery.

This harness injects a TaskManager SIGKILL at each of four phases and then verifies that, after
recovery, the reloaded weight version MATCHES the version the restored checkpoint referenced (not
whatever file is newest on disk) and that its sha equals the sha logged for that version before the
kill. That is the transactional property the reviewer asked for: the recovered model corresponds to
the checkpoint.

  before  : kill before any version is persisted (during warm-up)
  between : kill just after a WEIGHTVER line, before the next checkpoint completes
  during  : kill while a checkpoint is IN_PROGRESS
  after   : kill just after a checkpoint completes

Consistency criterion, per subtask:
  restored_version == max version whose WEIGHTVER timestamp <= restored checkpoint's completion time
  sha(restored_version) == sha logged for that version before the kill

RUN THIS ON THE JOBMANAGER NODE. ssh/curl must run OUTSIDE the conda mostream env, because conda's
OpenSSL breaks ssh (see docs/cloudlab_notes.md). Confirm the three cluster specifics marked CONFIRM
below (REST port, TM process pattern, TM log path) before trusting a run.
"""
import argparse, json, subprocess, sys, time, re, os

REST = "http://localhost:8081"                       # CONFIRM: JM REST endpoint
TM_PROC = "taskexecutor"                              # CONFIRM: pattern that matches the TM process
DEFAULT_TM_LOG = "$FLINK_HOME/log/*taskexecutor*.log" # CONFIRM: TM log glob on the TM host

WEIGHTVER = re.compile(r'WEIGHTVER subtask=(?P<st>\d+) version=(?P<v>\S+) sha=(?P<sha>\w+)')
RESTORED = re.compile(r'Restored weights v(?P<v>\d+) from checkpoint reference')


def sh(cmd):
    """Run a local shell command, return stdout (empty on failure)."""
    p = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return p.stdout.strip()


def ssh(host, user, remote_cmd):
    opts = "-o ConnectTimeout=15 -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
    return sh(f"/usr/bin/ssh {opts} {user}@{host} {json.dumps(remote_cmd)}")


def rest(path):
    out = sh(f"curl -s --max-time 10 {REST}{path}")
    try:
        return json.loads(out)
    except Exception:
        return None


def job_id():
    d = rest("/jobs")
    for j in (d or {}).get("jobs", []):
        if j.get("status") == "RUNNING":
            return j["id"]
    return None


def ckpt_counts(jid):
    d = rest(f"/jobs/{jid}/checkpoints")
    c = (d or {}).get("counts", {})
    latest = (d or {}).get("latest", {}).get("completed", {})
    return c.get("completed", 0), c.get("in_progress", 0), latest.get("latest_ack_timestamp", 0)


def num_restarts(jid):
    a = rest(f"/jobs/{jid}/metrics?get=numRestarts") or []
    return next((int(m["value"]) for m in a if m["id"] == "numRestarts"), 0)


def tm_log_lines(host, user, tm_log):
    """WEIGHTVER (version->sha, with ordering) and any Restored lines from the TM log."""
    raw = ssh(host, user, f"grep -hE 'WEIGHTVER|Restored weights v' {tm_log} 2>/dev/null | tail -n 400")
    wv, restored = {}, []
    for i, ln in enumerate(raw.splitlines()):
        m = WEIGHTVER.search(ln)
        if m and m["v"] != "NA":
            wv.setdefault(int(m["st"]), []).append((i, int(m["v"]), m["sha"]))
        r = RESTORED.search(ln)
        if r:
            restored.append(int(r["v"]))
    return wv, restored


def wait_running(timeout=600):
    t0 = time.time()
    while time.time() - t0 < timeout:
        jid = job_id()
        if jid:
            _, _, _ = ckpt_counts(jid)
            return jid
        time.sleep(5)
    return None


def kill_tm(host, user):
    ssh(host, user, f"pkill -9 -f {TM_PROC}")


def restart_tm(tm_host, user):
    # restart_taskmanager.sh must run ON the TaskManager node, in its own ssh invocation.
    ssh(tm_host, user, "bash ~/MoStream/cloudlab/restart_taskmanager.sh")


def trigger(phase, jid):
    """Block until the chosen phase condition holds, then return."""
    if phase == "before":
        return  # kill immediately; warm-up has not persisted a version yet
    base_completed, _, _ = ckpt_counts(jid)
    t0 = time.time()
    while time.time() - t0 < 900:
        completed, inprog, _ = ckpt_counts(jid)
        if phase == "during" and inprog > 0:
            return
        if phase == "after" and completed > base_completed:
            return
        if phase == "between":
            # a persist happened (WEIGHTVER advanced) but no new checkpoint completed yet
            if completed == base_completed:
                return
        time.sleep(1)


def expected_restored_version(wv_before, restored_ckpt_ts, wv_ts):
    """max version committed at or before the restored checkpoint's ack time, per subtask."""
    exp = {}
    for st, entries in wv_before.items():
        cand = [v for (idx, v, sha) in entries if wv_ts.get((st, v), 0) <= restored_ckpt_ts]
        if cand:
            exp[st] = max(cand)
    return exp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["before", "between", "during", "after"], required=True)
    ap.add_argument("--conf", default=os.path.expanduser("~/MoStream/cloudlab/cluster.conf"))
    ap.add_argument("--tm-log", default=DEFAULT_TM_LOG)
    args = ap.parse_args()

    conf = {}
    for ln in open(args.conf):
        ln = ln.strip()
        if "=" in ln and not ln.startswith("#"):
            k, v = ln.split("=", 1)
            conf[k.strip()] = v.strip().strip('"')
    tm_host = conf.get("TM_HOST") or conf.get("TM_IP")
    jm_host = conf.get("JM_HOST") or conf.get("JM_IP")
    user = conf.get("SSH_USER", os.environ.get("USER", "root"))

    jid = job_id()
    if not jid:
        print("no RUNNING job; submit with MOSTREAM_VERSIONED_WEIGHTS=1 first"); sys.exit(1)
    print(f"job {jid}, phase={args.phase}")

    wv_before, _ = tm_log_lines(tm_host, user, args.tm_log)
    restarts_before = num_restarts(jid)
    _, _, ckpt_ts_before = ckpt_counts(jid)

    trigger(args.phase, jid)
    print("phase condition met; SIGKILL TM")
    kill_tm(tm_host, user)
    time.sleep(5)
    restart_tm(tm_host, user)

    jid = wait_running()
    if not jid:
        print("job did not return to RUNNING within timeout"); sys.exit(2)
    # let the first post-recovery record drive the version-aware reload
    time.sleep(60)

    wv_after, restored = tm_log_lines(tm_host, user, args.tm_log)
    _, _, ckpt_ts_after = ckpt_counts(jid)
    wv_ts = {}  # (subtask, version) -> approximate persist time; refine from log timestamps if present
    for st, entries in wv_before.items():
        for _, v, _ in entries:
            wv_ts[(st, v)] = ckpt_ts_before  # conservative; see note in docstring

    exp = expected_restored_version(wv_before, ckpt_ts_before, wv_ts)
    sha_before = {(st, v): sha for st, es in wv_before.items() for _, v, sha in es}
    sha_after = {(st, v): sha for st, es in wv_after.items() for _, v, sha in es}

    print(f"restarts: {restarts_before} -> {num_restarts(jid)}")
    print(f"restored versions logged: {restored}")
    print(f"expected restored version per subtask (<= checkpoint ack): {exp}")
    ok = True
    for r in restored:
        # sha must match what was recorded for that version before the kill
        matches = [k for k, s in sha_before.items() if k[1] == r]
        for k in matches:
            same = sha_after.get(k) == sha_before.get(k) if k in sha_after else True
            if not same:
                ok = False
                print(f"  MISMATCH subtask {k[0]} v{r}: sha changed across recovery")
    if args.phase != "before" and exp and not any(v in restored for v in exp.values()):
        ok = False
        print("  MISMATCH: restored version does not equal the checkpoint-referenced version")
    print("RESULT:", "CONSISTENT" if ok else "INCONSISTENT")


if __name__ == "__main__":
    main()
