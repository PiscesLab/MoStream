#!/usr/bin/env python3
"""Dry-run producer: read local dataset file and print Simulation-style JSON messages.
Usage: python dev/dry_run_producer.py --count 5
"""
import json
import time
import argparse
from pathlib import Path

# Try dataset paths used in repo
POSSIBLE_PATHS = [
    Path("WLGenerator-node1/dataset/training-data-simple.txt"),
    Path("WLGenerator-node1/dataset/training-data.json"),
    Path("MDStream/WLGenerator/dataset/training-data-simple.txt"),
    Path("WLGenerator/dataset/training-data-simple.txt"),
]

parser = argparse.ArgumentParser()
parser.add_argument("--count", type=int, default=5, help="number of sample messages to print")
parser.add_argument("--out", type=str, default=None, help="optional output file to write JSON lines")
args = parser.parse_args()

def find_dataset():
    for p in POSSIBLE_PATHS:
        if p.exists():
            return p
    return None

def parse_simple_txt(path):
    # expected format lines that include keywords like "smiles"
    smiles = []
    inchi = []
    ip = []
    with open(path, "r") as f:
        for line in f:
            if "smiles" in line:
                parts = line.split(" ")
                try:
                    s = parts[1].split(",")[0].split("'")[1]
                    i = parts[3].split("'")[1]
                    val = parts[5].split("}")[0]
                except Exception:
                    continue
                smiles.append(s)
                inchi.append(i)
                ip.append(val)
    return smiles, inchi, ip


def make_message(smiles, inchi, ip_val, model_id=0):
    return {
        "timestamp": int(time.time() * 1000),
        "smiles": smiles,
        "inchi": inchi,
        "IP_simulate": ip_val,
        "model_id": model_id,
    }


def main():
    ds = find_dataset()
    if not ds:
        print("No dataset found in candidate paths. Please run from repo root or pass dataset into file.")
        return
    smiles_list, inchi_list, ip_list = parse_simple_txt(ds)
    if not smiles_list:
        print(f"Dataset {ds} parsed but no entries found.")
        return

    out_lines = []
    count = min(args.count, len(smiles_list))
    for i in range(count):
        idx = i % len(smiles_list)
        msg = make_message(smiles_list[idx], inchi_list[idx], ip_list[idx], model_id=0)
        line = json.dumps(msg)
        print(line)
        out_lines.append(line)

    if args.out:
        with open(args.out, "w") as fo:
            fo.write("\n".join(out_lines))

if __name__ == "__main__":
    main()
