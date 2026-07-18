#!/usr/bin/env python3
"""E1-lat -- steering-latency vs load sweep (validates eq:lstream).

Drives the Simulation topic at several controlled arrival rates lambda and records the
steering latency the Flink job stamps on every recommendation. A background consumer
thread reads Recommend for the whole run; the main thread runs the rate phases and logs
their wall-clock boundaries, so latencies are bucketed per phase afterwards.

Unlike the closed-loop simulator (whose echo path is unpaced), this emits real (smiles, IP)
seed records at a precise interval, stamping timestamp=now(ms), so latency_ms measured at
Rank is the true end-to-end steering latency for arrival rate lambda = 1/interval.

Model under test:  L(rho) = rho/(1-rho) * Sbar + L0,  rho = lambda/mu,  Sbar = 1/mu.

Usage:
  python cloudlab/latency_sweep.py --bootstrap 10.10.1.3:9092 \
      --intervals 20,10,6.7,5 --warmup 90 --settle 120 --window 240 --out results/latsweep
"""
import argparse, json, os, random, re, threading, time
from collections import defaultdict
from kafka import KafkaProducer, KafkaConsumer

DATA_CANDIDATES = [
    "/mnt/media/MDStream/WLGenerator/dataset/training-data-simple.txt",
    os.path.expanduser("~/MoStream/MoStream/WLGenerator-node1/dataset/training-data-simple.txt"),
    os.path.expanduser("~/MoStream/MoStream/MDStream/WLGenerator/dataset/training-data-simple.txt"),
    "MoStream/WLGenerator-node1/dataset/training-data-simple.txt",
]
FIELD_RE = re.compile(r'(\w+):\s*(\S+)')
BAD = ('model_not_ready', 'search_space_empty', 'mol_dicts_empty', 'inference_error')


def load_seeds():
    path = next((p for p in DATA_CANDIDATES if os.path.exists(p)), None)
    if path is None:
        raise FileNotFoundError("no training-data-simple.txt in " + str(DATA_CANDIDATES))
    print(f"[sweep] seeds from {path}", flush=True)
    smiles, ip = [], []
    with open(path) as f:
        for line in f:
            if "smiles" not in line:
                continue
            try:
                s = line.split(" ")[1].split(",")[0].split("'")[1]
                v = line.split(" ")[5].split("}")[0]
                float(v)
            except Exception:
                continue
            smiles.append(s); ip.append(v)
    return smiles, ip


class Consumer(threading.Thread):
    """Reads Recommend from the live tail; appends (emit_ts_ms, src_ts_ms, latency_ms)."""
    def __init__(self, bootstrap):
        super().__init__(daemon=True)
        self.rows = []
        self.stop = False
        self.bootstrap = bootstrap

    def run(self):
        c = KafkaConsumer(
            "Recommend", bootstrap_servers=self.bootstrap,
            group_id=f"latsweep-{int(time.time())}",
            auto_offset_reset="latest", enable_auto_commit=False,
            consumer_timeout_ms=1000,
            value_deserializer=lambda b: b.decode("utf-8", "ignore"))
        # force partition assignment before seeking to the live tail (seek_to_end raises
        # if called with no partitions assigned yet)
        deadline = time.time() + 25
        while not c.assignment() and time.time() < deadline:
            c.poll(timeout_ms=500)
        try:
            c.seek_to_end()
        except Exception as e:
            print(f"[sweep] consumer seek_to_end skipped: {e}", flush=True)
        while not self.stop:
            for msg in c:
                if self.stop:
                    break
                text = msg.value
                if not text or any(p in text for p in BAD):
                    continue
                d = dict(FIELD_RE.findall(text))
                try:
                    emit = int(d["timestamp"]); src = int(d["src_ts"]); lat = int(d["latency_ms"])
                except (KeyError, ValueError):
                    continue
                if src > 0 and lat >= 0:
                    self.rows.append((emit, src, lat))
        c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--intervals", required=True, help="comma list of seconds/record, e.g. 20,10,6.7,5")
    ap.add_argument("--warmup", type=float, default=90.0, help="fast warm-up seconds (interval 2s)")
    ap.add_argument("--settle", type=float, default=120.0, help="skip this many s at the start of each phase")
    ap.add_argument("--window", type=float, default=240.0, help="measurement seconds per phase (after settle)")
    ap.add_argument("--out", default="results/latsweep")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    smiles, ip = load_seeds()
    n = len(smiles)
    print(f"[sweep] {n} seed molecules loaded", flush=True)

    cons = Consumer(args.bootstrap)
    cons.start()

    prod = KafkaProducer(bootstrap_servers=args.bootstrap,
                         value_serializer=lambda v: json.dumps(v).encode())

    def produce(interval, duration, tag, poisson=True):
        # Poisson arrivals (exponential inter-arrival gaps, mean=interval): a deterministic
        # fixed-interval source does not queue below saturation, so it cannot exercise the
        # rho/(1-rho) term. Exponential gaps give the M/M/1 arrival process the model assumes.
        t_end = time.time() + duration
        i = 0; mid = 0; sent = 0
        while time.time() < t_end:
            j = i % n
            prod.send("Simulation", {"timestamp": int(time.time()*1000),
                      "smiles": smiles[j], "inchi": "", "IP_simulate": ip[j], "model_id": mid})
            sent += 1; i += 1
            time.sleep(random.expovariate(1.0/interval) if poisson else interval)
        prod.flush()
        print(f"[sweep] {tag}: sent {sent} over {duration:.0f}s (mean interval {interval}s, "
              f"poisson={poisson})", flush=True)

    # 1) warm up the model fast (interval 2s) so Train passes batch_size before the sweep
    print(f"[sweep] warm-up {args.warmup}s @ interval 2s", flush=True)
    produce(2.0, args.warmup, "warmup", poisson=False)

    phases = []
    for s in args.intervals.split(","):
        interval = float(s)
        lam = 1.0 / interval
        dur = args.settle + args.window
        t0 = time.time()
        print(f"[sweep] phase interval={interval}s lambda={lam:.3f}/s for {dur:.0f}s", flush=True)
        produce(interval, dur, f"lam{lam:.3f}")
        t1 = time.time()
        # measurement region excludes the settle prefix
        phases.append({"interval": interval, "lambda": lam,
                       "t_start_ms": int((t0 + args.settle) * 1000),
                       "t_end_ms": int(t1 * 1000)})

    time.sleep(5)
    cons.stop = True
    time.sleep(2)

    rows = cons.rows
    # raw per-rec CSV
    with open(os.path.join(args.out, "recs.csv"), "w") as f:
        f.write("emit_ts_ms,src_ts_ms,latency_ms\n")
        for e, s, l in rows:
            f.write(f"{e},{s},{l}\n")

    # bucket by phase measurement window
    def pct(xs, q):
        xs = sorted(xs)
        if not xs:
            return None
        k = (len(xs) - 1) * q
        lo = int(k); hi = min(lo + 1, len(xs) - 1)
        return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)

    summary = []
    for p in phases:
        lat = [l for (e, s, l) in rows if p["t_start_ms"] <= e <= p["t_end_ms"]]
        rec = {"interval": p["interval"], "lambda": round(p["lambda"], 4), "n": len(lat)}
        if lat:
            rec.update({"median_ms": pct(lat, 0.5), "p95_ms": pct(lat, 0.95),
                        "mean_ms": sum(lat)/len(lat), "min_ms": min(lat), "max_ms": max(lat)})
        summary.append(rec)
        print(f"[sweep] lambda={p['lambda']:.3f}  n={len(lat)}  "
              f"median={rec.get('median_ms')}ms  p95={rec.get('p95_ms')}ms", flush=True)

    with open(os.path.join(args.out, "summary.json"), "w") as f:
        json.dump({"phases": summary, "total_recs": len(rows)}, f, indent=2)
    print(f"[sweep] wrote {args.out}/recs.csv and summary.json ({len(rows)} recs)", flush=True)


if __name__ == "__main__":
    main()
