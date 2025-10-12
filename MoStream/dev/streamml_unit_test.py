"""
Lightweight unit-test harness for StreamML (Train/Infer/Rank) without Flink/TensorFlow/Kafka.
Runs a simplified pipeline using synthetic data or the repository search_space file when available.


"""

import os
import json
import random
import time
import statistics

BATCH_SIZE = 16
FALLBACK_SMILES = [
    "CCO", "CCC", "CCN", "c1ccccc1", "O=C=O", "C#N", "CC(=O)O", "C1CC1", "CC(F)(F)F",
    "NCCO", "CNC", "C=O", "CCCl", "CBr", "COC", "CCS", "C=CC", "CN(C)C"
]


def load_search_space_chunk(chunk_id, chunk_size=500):
    path = os.path.join('MDStream', 'StreamML', 'search_space', 'MOS-search-simple.txt')
    if os.path.exists(path):
        smiles = []
        with open(path, 'r') as fh:
            for i, line in enumerate(fh):
                if 'smiles' in line:
                    s = line.split()[1].split(',')[0].strip("'")
                    smiles.append(s)
        # emulate chunking
        start = (chunk_id * chunk_size) % max(1, len(smiles))
        end = start + min(chunk_size, len(smiles) - start)
        return smiles[start:end] if smiles else FALLBACK_SMILES[:min(chunk_size, len(FALLBACK_SMILES))]
    else:
        # fallback small chunk
        return [random.choice(FALLBACK_SMILES) for _ in range(min(50, len(FALLBACK_SMILES)))]


class TrainMock:
    def __init__(self, batch_size=BATCH_SIZE):
        self.batch_size = batch_size
        self.state = {}  # model_id -> list of (smiles, ip)
        self.model_paras = {}  # model_id -> weights_json

    def process(self, smiles, ip_sim, model_id):
        lst = self.state.setdefault(model_id, [])
        lst.append((smiles, float(ip_sim)))
        # keep only last batch_size
        if len(lst) > self.batch_size:
            lst.pop(0)
        # if not enough examples yet, return placeholder
        if len(lst) < self.batch_size:
            print(f"TrainMock: accumulated {len(lst)}/{self.batch_size} for model {model_id}; returning placeholder")
            return f"{model_id}$haaah${model_id}$haaah"
        # else "train": create fake weights and infra json
        weights = [[random.random() for _ in range(5)] for _ in range(10)]  # tiny fake weight arrays
        weights_json = json.dumps(weights)
        infra_json = json.dumps({"model": "mock-mpnn", "layers": 10})
        # save in-memory
        self.model_paras[model_id] = weights_json
        chunk_id = random.randint(0, 10)
        out = f"{chunk_id}${weights_json}${model_id}${infra_json}"
        print(f"TrainMock: trained model {model_id} -> chunk {chunk_id}, weights(len)={len(weights_json)}")
        return out


class InferMock:
    def __init__(self):
        pass

    def process(self, chunk_id, weights_json_str, model_id, infra_json_str):
        # if placeholder
        if weights_json_str is None or 'haaah' in weights_json_str:
            print(f"InferMock: model not ready for chunk {chunk_id}")
            return [f"{chunk_id}$model_not_ready$0"]
        # load search space chunk
        smiles_list = load_search_space_chunk(int(chunk_id))
        # simple deterministic fake prediction: use hash of smiles to a float
        preds = []
        for s in smiles_list:
            # make deterministic
            val = (sum(ord(c) for c in s) % 100) / 5.0 + (len(s) % 3)
            preds.append((s, float(val)))
        results = [f"{chunk_id}${s}${p}" for s, p in preds]
        print(f"InferMock: predicted {len(results)} smiles for chunk {chunk_id}")
        return results


class RankMock:
    def __init__(self, top_k=10):
        self.searched = set()
        self.top_k = top_k

    def apply(self, predictions):
        # predictions: list of tuples (chunk_id, smiles, pred)
        if any('model_not_ready' in p[1] for p in predictions):
            ts = int(time.time() * 1000)
            return [f"smiles: model_not_ready ucb: 0 est_ip: 0 timestamp: {ts}$"]
        # aggregate per smiles
        store = {}
        for chunk_id, smiles, pred in predictions:
            store.setdefault(smiles, []).append(pred)
        # compute ucb = mean + std
        scored = {s: (statistics.mean(vals) + (statistics.pstdev(vals) if len(vals) > 1 else 0.0)) for s, vals in store.items()}
        sorted_smiles = sorted(scored.keys(), key=lambda s: scored[s], reverse=True)
        result = []
        ts = int(time.time() * 1000)
        count = 0
        for s in sorted_smiles:
            if s in self.searched:
                continue
            if count >= self.top_k:
                break
            self.searched.add(s)
            ucb = scored[s]
            line = f"smiles: {s} ucb: {ucb:.4f} est_ip: {ucb:.4f} timestamp: {ts}$"
            result.append(line)
            count += 1
        return result


def make_synthetic_simulations(n=40):
    sims = []
    for i in range(n):
        sims.append({
            'timestamp': int(time.time() * 1000),
            'smiles': random.choice(FALLBACK_SMILES),
            'inchi': 'InChI=1S/...',
            'IP_simulate': round(random.uniform(10, 25), 4),
            'model_id': random.choice([0, 1])
        })
    return sims


def main():
    print('\n=== StreamML unit-test harness (mock) ===\n')
    sims = make_synthetic_simulations(48)

    trainer = TrainMock()
    inferer = InferMock()
    ranker = RankMock(top_k=5)

    # Simulate the Flink pipeline steps
    infer_inputs = []  # collect infer outputs as tuples (chunk_id, smiles, pred)

    for m in sims:
        parse_smiles = m['smiles']
        ip_sim = m['IP_simulate']
        model_id = m['model_id']
        # Train step
        train_out = trainer.process(parse_smiles, ip_sim, model_id)
        # train_out is string chunk_id$weights_json$model_id$infra_json or placeholder
        parts = train_out.split('$')
        if len(parts) >= 4:
            chunk_id_s, weights_json, mid_s, infra_json = parts[0], parts[1], parts[2], parts[3]
            # Forward to infer (keyed by chunk_id in real job)
            infer_results = inferer.process(chunk_id_s, weights_json, int(mid_s), infra_json)
            for line in infer_results:
                pparts = line.split('$')
                if len(pparts) >= 3:
                    cid = int(pparts[0])
                    smiles = pparts[1]
                    pred = float(pparts[2])
                    infer_inputs.append((cid, smiles, pred))
        else:
            print(f"Unexpected train output: {train_out}")

    print('\nTotal inference predictions collected:', len(infer_inputs))
    # Simulate windowing: send all to ranker
    ranked = ranker.apply(infer_inputs)
    print('\n=== Ranked recommendations (sink -> Result) ===')
    for r in ranked:
        print(r)


if __name__ == '__main__':
    main()
