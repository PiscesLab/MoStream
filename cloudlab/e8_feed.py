#!/usr/bin/env python3
"""E8 controlled feed: publish valid Simulation records at a fixed sub-saturation rate.

The recovery-consistency test needs a steady, LOW feed, below the P=1 Infer-amplification
saturation point (each Train record fans out to ~500 Infer predictions, and P=1 Infer sustains
only ~60/s), so aligned checkpoint barriers complete and the single-key training window fills
predictably. At interval 15 s the rate is ~0.067 rec/s -> ~33 Infer/s, comfortably sub-saturation,
and the 16-record window fills in ~4 min.

Publishes {"smiles","IP_simulate","model_id","timestamp"} JSON to the Simulation topic. model_id is
ignored downstream in versioned/E8 mode (MDWorkflow routes the whole stream to one key). Run on a
node with kafka-python (a simulator node).
"""
import argparse, json, os, time
from kafka import KafkaProducer

# simple, definitely-valid organic SMILES with plausible IP labels (V)
MOLS = [
    ("CCO", 12.30), ("CCC", 11.10), ("CCCC", 10.90), ("CCCCC", 10.80),
    ("CCN", 11.50), ("CCCO", 11.90), ("CC(=O)O", 13.10), ("c1ccccc1", 12.60),
    ("CCOC", 11.20), ("CCCN", 11.40), ("CCCCO", 11.70), ("CC(C)O", 12.00),
    ("CCCCCC", 10.70), ("CCOCC", 11.00), ("CCC(=O)O", 12.90), ("CCCCN", 11.30),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "10.10.1.3:9092"))
    ap.add_argument("--interval", type=float, default=15.0)   # ~0.067 rec/s, sub-saturation at P=1
    ap.add_argument("--count", type=int, default=0)           # 0 = run forever
    args = ap.parse_args()
    prod = KafkaProducer(bootstrap_servers=args.bootstrap,
                         value_serializer=lambda v: json.dumps(v).encode())
    i = 0
    while args.count == 0 or i < args.count:
        smi, ip = MOLS[i % len(MOLS)]
        rec = {"smiles": smi, "IP_simulate": float(ip), "model_id": 0,
               "timestamp": int(time.time() * 1000)}
        prod.send("Simulation", rec)
        prod.flush()
        print(f"[e8_feed] {i} -> {smi} IP={ip}", flush=True)
        i += 1
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
