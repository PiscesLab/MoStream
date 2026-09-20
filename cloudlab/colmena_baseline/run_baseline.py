#!/usr/bin/env python3
"""Run the Colmena baseline arm.

The task-based comparison for the discovery experiment. Same oracle, same surrogate,
same candidate set and same worker budget as the streaming arm; the only difference is
that the ranking refreshes once per round instead of on every result.

Local smoke test, a handful of simulations on one machine:

    python cloudlab/colmena_baseline/run_baseline.py --budget 8 --round-size 4 --local

On the cluster, matching the three simulation nodes:

    python cloudlab/colmena_baseline/run_baseline.py \
        --budget 512 --round-size 16 --workers 3 --out results/colmena

Writes into --out:
    oracle_colmena.log    one line per verified molecule, same format as the
                          streaming arm's simulator logs, so plot_discovery.py reads it
    steering_latency.csv  per result, how long it waited before it could influence
                          a dispatch
    summary.json          run configuration and totals
"""
import argparse
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from colmena.queue import PipeQueues
from colmena.task_server import ParslTaskServer
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

from tasks import CHUNK_SIZE, HIT_THRESHOLD, score_chunk, simulate_molecule, train_surrogate
from thinker import RoundThinker


# --- dry-run stand-ins -------------------------------------------------------
# The oracle needs the xtb binary and the vendored moldesign package, which live on
# the simulation nodes only. These let the scheduling logic and the output format be
# exercised anywhere, which is what --dry-run is for. They never touch chemistry.
def fake_simulate(smiles):
    import random as _r, time as _t
    _t.sleep(0.05)
    return smiles, 13.0 + _r.Random(smiles).random() * 2.0, 0.05


def fake_train(records, weights=None):
    import json as _j, time as _t
    _t.sleep(0.02)
    return _j.dumps([len(records)])


def fake_score(chunk_id, weights):
    import random as _r
    rng = _r.Random(chunk_id)
    return [(f'DRY{chunk_id}_{i}', 12.0 + rng.random() * 3.0) for i in range(200)]


# Colmena dispatches by function name, so the stand-ins must answer to the real names.
fake_simulate.__name__ = 'simulate_molecule'
fake_train.__name__ = 'train_surrogate'
fake_score.__name__ = 'score_chunk'


def load_seed(path: str, n: int):
    """Seed labelled molecules, the same table the streaming arm warms up from."""
    records = []
    with open(path) as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) < 2:
                continue
            try:
                records.append((parts[0], float(parts[-1])))
            except ValueError:
                continue
            if len(records) >= n:
                break
    return records


def resolve_seed_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.abspath(os.path.join(here, '..', '..'))
    for cand in (os.environ.get('MOSTREAM_TRAINING_DATA', ''),
                 '/mnt/media/MDStream/WLGenerator/dataset/training-data-simple.txt',
                 os.path.join(root, 'MoStream', 'WLGenerator-node1', 'dataset',
                              'training-data-simple.txt')):
        if cand and os.path.exists(cand):
            return cand
    raise FileNotFoundError('seed table not found; set MOSTREAM_TRAINING_DATA')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--budget', type=int, default=512,
                    help='total oracle calls, the compute budget both arms share')
    ap.add_argument('--round-size', type=int, default=16,
                    help='simulations dispatched per round before the model is refreshed')
    ap.add_argument('--workers', type=int, default=3,
                    help='concurrent oracle workers, one per simulation node')
    ap.add_argument('--seed-size', type=int, default=16,
                    help='labelled molecules to warm start from')
    ap.add_argument('--n-chunks', type=int, default=2231,
                    help='candidate chunks in the search space')
    ap.add_argument('--out', default='results/colmena')
    ap.add_argument('--rng-seed', type=int, default=0)
    ap.add_argument('--dry-run', action='store_true',
                    help='exercise scheduling and output format with stand-in tasks; '
                         'no chemistry, so it runs off the simulation nodes')
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        handlers=[logging.StreamHandler(sys.stdout),
                                  logging.FileHandler(f'{args.out}/run.log')])

    if args.dry_run:
        methods = [fake_simulate, fake_train, fake_score]
        logging.warning('DRY RUN: stand-in tasks, the numbers mean nothing')
        seed = [(f'SEED{i}', 13.5) for i in range(args.seed_size)]
    else:
        methods = [simulate_molecule, train_surrogate, score_chunk]
        seed = load_seed(resolve_seed_path(), args.seed_size)
    logging.info('seed records: %d', len(seed))

    queues = PipeQueues(topics=['simulate', 'train', 'infer'])
    config = Config(executors=[HighThroughputExecutor(
        label='oracle', max_workers_per_node=args.workers,
        provider=LocalProvider(init_blocks=1, max_blocks=1),
    )], run_dir=f'{args.out}/parsl')

    server = ParslTaskServer(methods, queues, config)
    thinker = RoundThinker(queues, out_dir=args.out, n_chunks=args.n_chunks,
                           round_size=args.round_size, budget=args.budget,
                           seed_records=seed, rng_seed=args.rng_seed)

    started = time.time()
    server.start()
    try:
        thinker.run()
    finally:
        queues.send_kill_signal()
        server.join()

    hits = sum(1 for _, ip in thinker.records if ip > HIT_THRESHOLD)
    summary = {
        'arm': 'colmena-dry-run' if args.dry_run else 'colmena',
        'budget': args.budget,
        'round_size': args.round_size,
        'workers': args.workers,
        'chunk_size': CHUNK_SIZE,
        'hit_threshold_v': HIT_THRESHOLD,
        'simulations_completed': thinker.completed,
        'rounds': thinker.round_index,
        'hits': hits,
        'wall_clock_s': time.time() - started,
    }
    with open(f'{args.out}/summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    logging.info('done: %s', summary)


if __name__ == '__main__':
    main()
