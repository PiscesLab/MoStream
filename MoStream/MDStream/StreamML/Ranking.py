import numpy as np
import pickle as pkl
import random, time
from collections import OrderedDict

from pyflink.datastream.functions import WindowFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import CountWindow
from pyflink.common.typeinfo import Types
from typing import List, Any, Optional, Tuple, Dict, Union, Iterable

class RankFunction(WindowFunction):
    """Rank a window of scored candidates and emit the unseen ones, best first.

    KEYED, not global. This was an AllWindowFunction fed by `.key_by(chunk_id).window_all(...)`
    -- and `window_all` DISCARDS the key_by and forces the operator to parallelism 1. The code
    read as though it were data-parallel; Flink issues no warning. The consequence was severe,
    because Rank sits downstream of Infer's 500x amplification: one Python worker absorbed ~500x
    the record rate of the entire rest of the pipeline while the other seven slots sat idle.
    Rank became the bottleneck (backpressure metrics: Rank 0% backpressured and the only busy
    operator, everything upstream stalled), the Infer->Rank queue grew without bound, and the
    resulting backpressure stopped checkpoint barriers from ever traversing the dataflow -- which
    is what put the job into a checkpoint-timeout restart loop.

    Keying by `chunk_id` is safe for de-duplication, which is the only cross-record state here:
    Infer derives a chunk's candidates from a fixed slice of the search-space file
    (lines [chunk_id*500, (chunk_id+1)*500)), so a given SMILES belongs to exactly ONE chunk and
    therefore always lands on the SAME sub-task. Each sub-task's `searched` set is disjoint from
    every other's, and the emitted set is identical to the parallelism-1 version.
    """

    # Bound on the de-duplication structure. Each sub-task only ever sees molecules from its own
    # chunks; the whole search space is ~1.1M molecules split across 8 sub-tasks, so ~140k per
    # sub-task. A 300k cap therefore covers a sub-task's entire reachable set with margin, so in
    # practice nothing is ever evicted before it would legitimately be re-seen -- but the memory
    # is bounded regardless of how long the campaign runs.
    SEARCHED_CAP = 300000

    def __init__(self):
        print("rank reach_init")
        # De-duplication of already-recommended molecules, as a BOUNDED LRU set.
        #
        # History: this was first a LIST (membership an O(n) linear scan, so the loop
        # decelerated as the campaign grew: 0.15 -> 0.023 rec/s once ~234k molecules had been
        # recommended), then an unbounded set() (membership O(1), but the set grew for the life
        # of the operator). Neither is safe in a long-running stream operator, whose state lives
        # as long as the job.
        #
        # An OrderedDict used as an LRU set keeps membership O(1) AND bounds the memory: on
        # insert past SEARCHED_CAP we evict the oldest key. This is the concrete form of the
        # "bounded structure" the design has always needed; a production system might instead use
        # a Bloom filter or engine-managed keyed state with a TTL. NOTE: this bounds the DEDUP
        # structure only. It is not the main driver of the slow (~0.8 GB/h) Python-worker growth
        # observed over long runs, which is TensorFlow/Beam accumulation in the worker process
        # and is why the workers must eventually be recycled (making weight persistence load-
        # bearing for multi-day operation).
        self.searched = OrderedDict()
        print("rank finished init")

    def _mark_recommended(self, smiles):
        """Record a just-recommended molecule; evict the oldest if over the cap. O(1)."""
        self.searched[smiles] = None
        if len(self.searched) > self.SEARCHED_CAP:
            self.searched.popitem(last=False)

    def open(self, runtime_context: RuntimeContext):
        print("rank reach open")
        self.state = runtime_context.get_map_state(MapStateDescriptor('mol_dicts', Types.STRING(), Types.LIST(Types.DOUBLE()))) 
        print("rank finished open")
  
    def apply(self, key, window: CountWindow, inputs: Iterable[tuple]) -> List:
        # `key` is the chunk_id this window belongs to (see the class docstring). The keyed
        # signature takes it as the first argument; the AllWindowFunction one did not.
        #print("rank inputs: ", inputs)
        # update state and inference
        smiles_list = []
        value_list = []
        # E0: newest simulation result whose training update is reflected in this window.
        # We take the max so the reported latency is the *conservative* one: the delay for the
        # freshest result to reach a recommendation, not for a stale one already in the pipe.
        src_ts = 0
        for est in inputs:
            chunk_id = int(est[0])
            est_smiles = est[1]
            est_ip = float(est[2])
            if len(est) > 3:
                src_ts = max(src_ts, int(est[3]))
            #print("chunk_id: ", chunk_id, " est_ip: ", est_ip)
            if "model_not_ready" in est_smiles:
               result = ["smiles: " + "model_not_ready" + " ucb: " + str(0) + " est_ip: " + str(0) + " timestamp: " + str(int(time.time() * 1000)) + "$"]
               return result
            #self.state.put(est_smiles, [est_ip])
            smiles_list.append(est_smiles)
            value_list.append(est_ip)
            #if self.state.contains(est_smiles):
            #   ip_list = self.state.get(est_smiles)
            #   ip_list.append(est_ip)
            #   self.state.put(est_smiles, ip_list)
            #else:
            #   self.state.put(est_smiles, [est_ip])
         
        # ranking
        molecules = {}
        #smiles_list = list(self.state.keys())
        #value_list = list(self.state.values())
        #print("smiles_list: ", smiles_list)
        #print("value_list: ", value_list)
        for smiles in smiles_list:
            #y_pred = self.state.get(smiles)
            y_pred = value_list[smiles_list.index(smiles)]
            #print("y_pred: ", y_pred)
            y_mean = np.mean(y_pred)
            y_std = np.std(y_pred)
            ucb = y_mean + 1 * y_std
            #ucb = abs(14-ucb)
            molecules[smiles] = ucb
        
        #sorted_smiles = sorted(molecules, key=molecules.get, reverse=False)
        sorted_smiles = sorted(molecules, key=molecules.get, reverse=True)
        
        # output recommend
        emit_ts = int(time.time() * 1000)
        # E0 steering latency: wall-clock delay from the simulation result entering the
        # workflow to this recommendation, which the model has now trained on. -1 means the
        # upstream producer did not stamp the record (no attribution possible).
        latency_ms = (emit_ts - src_ts) if src_ts > 0 else -1
        result = []
        count = 0
        for smiles in sorted_smiles:
            if count >= 10:
                break
            # `in` on an OrderedDict is O(1), same as a set. Only molecules we actually
            # recommend are recorded, so the ones we skip for lack of room stay eligible next
            # window -- identical behaviour to the previous unbounded set(), but bounded.
            if smiles in self.searched:
                continue
            self._mark_recommended(smiles)
            line = ("smiles: " + smiles
                    + " ucb: " + str(molecules[smiles])
                    + " est_ip: " + str(molecules[smiles])
                    + " timestamp: " + str(emit_ts)
                    + " src_ts: " + str(src_ts)
                    + " latency_ms: " + str(latency_ms) + "$")
            result.append(line)
            count = count + 1
        return result
