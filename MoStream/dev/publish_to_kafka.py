#!/usr/bin/env python3
"""Publish a few messages from repo dataset to Kafka topic 'Simulation'.
Usage: export KAFKA_BOOTSTRAP=localhost:9092; python3 dev/publish_to_kafka.py --count 5
"""
import os, json, time, argparse
from pathlib import Path

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--count", type=int, default=5)
args = parser.parse_args()

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "Simulation"

# try dataset paths
P = Path(__file__).resolve().parents[1]
CANDIDATES = [
    P / "WLGenerator-node1" / "dataset" / "training-data-simple.txt",
    P / "MDStream" / "WLGenerator" / "dataset" / "training-data-simple.txt",
    P / "WLGenerator" / "dataset" / "training-data-simple.txt",
]

def find_ds():
    for p in CANDIDATES:
        if p.exists():
            return p
    return None


def parse_simple(path):
    smiles, inchi, ip = [], [], []
    with open(path) as f:
        for line in f:
            if "smiles" in line:
                parts = line.split(" ")
                try:
                    s = parts[1].split(",")[0].split("'")[1]
                    i = parts[3].split("'")[1]
                    v = parts[5].split("}")[0]
                except Exception:
                    continue
                smiles.append(s)
                inchi.append(i)
                ip.append(v)
    return smiles, inchi, ip


def main():
    ds = find_ds()
    if not ds:
        print("Dataset not found. Run from repo root.")
        return
    smiles, inchis, ips = parse_simple(ds)
    if not smiles:
        print("Dataset parsed but empty")
        return

    producer = KafkaProducer(bootstrap_servers=[KAFKA], value_serializer=lambda v: json.dumps(v).encode())
    for i in range(min(args.count, len(smiles))):
        idx = i % len(smiles)
        msg = {"timestamp": int(time.time() * 1000), "smiles": smiles[idx], "inchi": inchis[idx], "IP_simulate": ips[idx], "model_id": 0}
        producer.send(TOPIC, msg)
        print("sent", msg)
        time.sleep(0.2)
    producer.flush()

if __name__ == '__main__':
    main()
