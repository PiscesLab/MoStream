#!/usr/bin/env python3
"""E4 -- throughput-vs-time sampler for the fault-recovery figure (runs on the JobManager node).

A background thread feeds the Simulation topic steadily so the Source counter climbs; the main
loop samples the Source vertex's cumulative read-records every few seconds and turns it into a
records/s rate (counter delta over wall-clock, same measure as e4_probe.py). It also records the
job state, so the TaskManager SIGKILL (issued externally from the operator's laptop, because
JM->TM ssh is not keyed) shows up as state != RUNNING and rate -> 0, then recovery.
"""
import argparse, json, os, threading, time
from urllib.request import urlopen

JM = "http://localhost:8081"
DATA_CANDIDATES = [
    os.path.expanduser("~/MoStream/MoStream/WLGenerator-node1/dataset/training-data-simple.txt"),
    "/mnt/media/MDStream/WLGenerator/dataset/training-data-simple.txt",
]


def get(path, tries=3):
    for i in range(tries):
        try:
            with urlopen(JM + path, timeout=8) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if i == tries - 1:
                return {"_err": str(e)}
            time.sleep(0.4)


def running_jid():
    d = get("/jobs")
    for j in d.get("jobs", []):
        if j["status"] == "RUNNING":
            return j["id"]
    return None


def source_records(jid):
    d = get(f"/jobs/{jid}")
    if not d or "_err" in d:
        return None, "UNREACHABLE"
    state = d.get("state", "?")
    for v in d.get("vertices", []):
        if "Source" in v.get("name", ""):
            try:
                return int(v["metrics"]["read-records"]), state
            except (KeyError, ValueError, TypeError):
                return None, state
    return None, state


def load_seeds():
    path = next((p for p in DATA_CANDIDATES if os.path.exists(p)), None)
    smiles, ip = [], []
    with open(path) as f:
        for line in f:
            if "smiles" not in line:
                continue
            try:
                s = line.split(" ")[1].split(",")[0].split("'")[1]
                v = line.split(" ")[5].split("}")[0]; float(v)
            except Exception:
                continue
            smiles.append(s); ip.append(v)
    return smiles, ip


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--interval", type=float, default=1.0, help="producer seconds/record")
    ap.add_argument("--sample", type=float, default=4.0)
    ap.add_argument("--duration", type=float, default=300.0)
    ap.add_argument("--out", default="results/e4trace")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    from kafka import KafkaProducer, KafkaConsumer
    smiles, ip = load_seeds()
    n = len(smiles)
    prod = KafkaProducer(bootstrap_servers=args.bootstrap,
                         value_serializer=lambda v: json.dumps(v).encode())
    stop = {"v": False}

    def feed():
        i = 0
        while not stop["v"]:
            j = i % n
            try:
                prod.send("Simulation", {"timestamp": int(time.time()*1000),
                          "smiles": smiles[j], "inchi": "", "IP_simulate": ip[j], "model_id": 0})
            except Exception:
                pass
            i += 1; time.sleep(args.interval)
    threading.Thread(target=feed, daemon=True).start()

    # SINK-side throughput: count real Recommend output messages. Unlike the JM Source metric
    # (which serves a stale cached count while the TM is down), this drops to 0 during the outage
    # and resumes on recovery -- the true dip.
    sink = {"count": 0}
    def sink_counter():
        c = KafkaConsumer("Recommend", bootstrap_servers=args.bootstrap,
                          group_id=f"e4sink-{int(time.time())}", auto_offset_reset="latest",
                          enable_auto_commit=False, consumer_timeout_ms=800,
                          value_deserializer=lambda b: b.decode("utf-8", "ignore"))
        deadline = time.time() + 20
        while not c.assignment() and time.time() < deadline:
            c.poll(timeout_ms=500)
        try:
            c.seek_to_end()
        except Exception:
            pass
        while not stop["v"]:
            for _ in c:
                sink["count"] += 1
                if stop["v"]:
                    break
        c.close()
    threading.Thread(target=sink_counter, daemon=True).start()

    jid = running_jid()
    assert jid, "no RUNNING job"
    print(f"[e4] sampling jid={jid} for {args.duration:.0f}s (kill the TM externally mid-run)", flush=True)

    t0 = time.time(); prev_c = None; prev_t = None; rows = []
    prev_sink = 0; prev_sink_t = time.time()
    while time.time() - t0 < args.duration:
        c, state = source_records(jid)
        rate = None
        if c is not None and prev_c is not None and c >= prev_c and (time.time() - prev_t) > 0:
            rate = (c - prev_c) / (time.time() - prev_t)
        if c is not None:
            prev_c = c; prev_t = time.time()
        else:
            prev_c = None
        sc = sink["count"]; dt = time.time() - prev_sink_t
        out_rate = (sc - prev_sink) / dt if dt > 0 else 0.0
        prev_sink = sc; prev_sink_t = time.time()
        now = round(time.time() - t0, 1)
        rows.append({"t": now, "wall_ms": int(time.time()*1000), "state": state,
                     "cum": c, "rate": rate, "out_rate": round(out_rate, 2)})
        print(f"[e4] t={now:6.1f}s state={state:12s} cum={c} in_rate={None if rate is None else round(rate,1)} out_rate={round(out_rate,1)}", flush=True)
        time.sleep(args.sample)

    stop["v"] = True
    with open(os.path.join(args.out, "trace.json"), "w") as f:
        json.dump({"rows": rows}, f, indent=2)
    print(f"[e4] wrote {args.out}/trace.json ({len(rows)} samples)", flush=True)


if __name__ == "__main__":
    main()
