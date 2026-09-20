"""The steering policy for the Colmena baseline.

This is the arm the paper compares against, so what matters is the one thing it does
differently from the streaming pipeline: the ranking refreshes at round boundaries
rather than on every result.

A round is:

  1. dispatch the top `round_size` candidates from the current ranked list
  2. wait for all of them to come back from the oracle
  3. retrain the surrogate on the widened window
  4. rescore one candidate chunk and re-rank

A result that lands early in step 2 cannot influence what gets simulated until step 4.
That waiting time is the quantity the comparison is about, and it is why the batch
design is slower to react even when the two arms share an oracle, a surrogate, a
candidate set and a machine.

Everything else is deliberately identical to the streaming arm. Constants live in
tasks.py so the two cannot drift apart.
"""
import json
import logging
import time
from typing import Dict, List, Optional, Tuple

from colmena.models import Result
from colmena.thinker import BaseThinker, agent, result_processor

from tasks import CHUNK_SIZE, HIT_THRESHOLD

logger = logging.getLogger(__name__)


class RoundThinker(BaseThinker):
    """Batch-synchronous active learning, the task-based baseline."""

    def __init__(self, queues, out_dir: str, n_chunks: int,
                 round_size: int = 16, budget: int = 512,
                 seed_records: Optional[List[Tuple[str, float]]] = None,
                 rng_seed: int = 0):
        super().__init__(queues)
        import random
        self.out_dir = out_dir
        self.n_chunks = n_chunks
        self.round_size = round_size
        self.budget = budget
        self.rng = random.Random(rng_seed)

        self.records: List[Tuple[str, float]] = list(seed_records or [])
        self.weights: Optional[str] = None
        self.ranked: List[str] = []       # current ranked candidates, frozen within a round
        self.searched = set(s for s, _ in self.records)

        self.dispatched = 0               # oracle calls started
        self.completed = 0                # oracle calls returned
        self.round_outstanding = 0
        self.round_index = 0
        self.round_started: Optional[float] = None
        # arrival time of each result in the current round, for steering latency
        self.round_arrivals: List[float] = []

        self.started = time.time()
        self.oracle_log = open(f'{out_dir}/oracle_colmena.log', 'a', buffering=1)
        self.latency_log = open(f'{out_dir}/steering_latency.csv', 'a', buffering=1)
        if self.latency_log.tell() == 0:
            self.latency_log.write('round,result_arrival_s,influenced_dispatch_s,steering_latency_s\n')

    # ------------------------------------------------------------------ helpers
    def _elapsed(self) -> float:
        return time.time() - self.started

    def _retrain_and_rank(self) -> None:
        """Step 3 and 4 of a round. Blocking, exactly as a Thinker round would be."""
        self.queues.send_inputs(self.records, self.weights,
                                method='train_surrogate', topic='train')
        res: Result = self.queues.get_result(topic='train')
        if not res.success:
            logger.error('train failed: %s', res.failure_info)
            return
        self.weights = res.value

        chunk = self.rng.randrange(self.n_chunks)
        self.queues.send_inputs(chunk, self.weights, method='score_chunk', topic='infer')
        res = self.queues.get_result(topic='infer')
        if not res.success:
            logger.error('score failed: %s', res.failure_info)
            return
        scored = [(s, p) for s, p in res.value if s not in self.searched]
        scored.sort(key=lambda sp: sp[1], reverse=True)
        self.ranked = [s for s, _ in scored]
        self.round_index += 1

    def _dispatch_round(self) -> None:
        """Step 1. Send the top of the frozen ranked list to the oracle."""
        batch = self.ranked[:self.round_size]
        self.ranked = self.ranked[self.round_size:]
        if not batch:
            return
        dispatch_at = self._elapsed()

        # every result that arrived in the previous round waited until now to matter
        for arrival in self.round_arrivals:
            self.latency_log.write(
                f'{self.round_index},{arrival:.3f},{dispatch_at:.3f},{dispatch_at - arrival:.3f}\n')
        self.round_arrivals = []

        for smiles in batch:
            self.searched.add(smiles)
            self.queues.send_inputs(smiles, method='simulate_molecule', topic='simulate')
        self.dispatched += len(batch)
        self.round_outstanding = len(batch)
        self.round_started = time.time()

    # ------------------------------------------------------------------- agents
    @agent(startup=True)
    def bootstrap(self):
        """Train on the seed set, rank, and start the first round."""
        logger.info('seeding from %d labelled molecules', len(self.records))
        self._retrain_and_rank()
        self._dispatch_round()

    @result_processor(topic='simulate')
    def collect(self, result: Result):
        """One oracle result. Record it, and close the round when the batch is done."""
        self.completed += 1
        self.round_outstanding -= 1
        arrival = self._elapsed()

        if result.success:
            smiles, ip, seconds = result.value
            if ip is not None:
                self.records.append((smiles, ip))
                self.round_arrivals.append(arrival)
                self.oracle_log.write(
                    f'{int(time.time())} [oracle] {smiles} est_ip=0 -> xtb_ip={ip:.4f} ({seconds:.1f}s)\n')
        else:
            logger.warning('oracle failed: %s', result.failure_info)

        if self.completed >= self.budget:
            logger.info('budget of %d simulations reached', self.budget)
            self.done.set()
            return

        # the round boundary: only now can new results change what gets simulated
        if self.round_outstanding <= 0:
            self._retrain_and_rank()
            self._dispatch_round()
