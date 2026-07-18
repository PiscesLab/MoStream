#!/usr/bin/env python3
"""Steady fixed-rate feeder into the Simulation topic (keeps Train busy so the Python workers
accumulate). Used by the E7 worker-recycling demonstration. Runs on any node with Kafka access."""
import argparse, json, os, time
from kafka import KafkaProducer

DATA_CANDIDATES = [
    os.path.expanduser("~/MoStream/MoStream/WLGenerator-node1/dataset/training-data-simple.txt"),
    "/mnt/media/MDStream/WLGenerator/dataset/training-data-simple.txt",
]


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
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--duration", type=float, default=3000.0)
    args = ap.parse_args()
    smiles, ip = load_seeds()
    n = len(smiles)
    prod = KafkaProducer(bootstrap_servers=args.bootstrap,
                         value_serializer=lambda v: json.dumps(v).encode())
    print(f"[feed] {n} seeds, interval={args.interval}s, duration={args.duration}s", flush=True)
    t_end = time.time() + args.duration
    i = 0; sent = 0
    while time.time() < t_end:
        j = i % n
        prod.send("Simulation", {"timestamp": int(time.time()*1000),
                  "smiles": smiles[j], "inchi": "", "IP_simulate": ip[j], "model_id": 0})
        i += 1; sent += 1
        if sent % 200 == 0:
            print(f"[feed] sent {sent}", flush=True)
        time.sleep(args.interval)
    prod.flush()
    print(f"[feed] done, sent={sent}", flush=True)


if __name__ == "__main__":
    main()
