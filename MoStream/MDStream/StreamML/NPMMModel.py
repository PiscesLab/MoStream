import random, json, os, hashlib
import numpy as np
import pickle as pkl

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from typing import List, Any, Optional, Tuple, Dict, Union


# Molecule padding is quantized to this many atoms. Every distinct padded size is a distinct
# input SHAPE, and every distinct shape makes TensorFlow retrace its graph and retain a new
# ConcreteFunction indefinitely. Must match Inference._ATOM_BUCKET.
_ATOM_BUCKET = 16


def _default_model_path():
    """Resolve the pretrained MPNN architecture file.

    Order: MOSTREAM_MODEL_PATH, then the copy shipped beside this module, then a
    checkout in the home directory. The old build hardcoded a site-specific NFS
    mount, which made the pipeline unrunnable anywhere else.
    """
    import os
    env_path = os.environ.get('MOSTREAM_MODEL_PATH')
    if env_path and os.path.exists(env_path):
        return env_path
    base_dir = os.path.dirname(os.path.abspath(__file__))
    candidate = os.path.join(base_dir, 'networks', 'model.h5')
    if os.path.exists(candidate):
        return candidate
    return os.path.expanduser(
        '~/MoStream/MoStream/MDStream/StreamML/networks/model.h5')


def _bucket_size(n):
    """Round a molecule size up to the next multiple of _ATOM_BUCKET."""
    return int(((int(n) + _ATOM_BUCKET - 1) // _ATOM_BUCKET) * _ATOM_BUCKET)


class TrainFunction(KeyedProcessFunction):

    def __init__(self, persist_weights=True, train_once=False, window_size=None, versioned_weights=False, round_seconds=0, pretrained_path=None, freeze=False, replay=False, replay_anchor=64, replay_lr=1e-5, replay_data=None, replay_min_window=1):
        #print("reach_init")
        self.state = None
        self.num_epochs = 1
        self.batch_size = 16
        self.validation_split = 0.1
        self.learning_rate = 1e-3
        self.random_state = 1
        self.model_paras = None
        self._model = None          # built once in open()
        self._infra_json_str = None # cached architecture JSON (never changes)
        # Weight persistence on/off (E6). Passed IN rather than read from os.environ in open(),
        # because environment variables set at job submission do NOT propagate to the Beam Python
        # worker on the TaskManager (verified: the worker env has no MOSTREAM_* vars). A
        # constructor argument is serialized WITH the operator and therefore does travel. The
        # value is resolved client-side in MDWorkflow from MOSTREAM_PERSIST_WEIGHTS.
        self._persist = bool(persist_weights)
        # E5 train-once arm. When True the surrogate takes exactly ONE gradient step, on the
        # first full window it sees, and is frozen thereafter: every later record re-emits the
        # weights from that single fit. This is the baseline that separates "a surrogate helps"
        # from "updating the surrogate ONLINE helps", which is the claim the streaming loop
        # actually makes. Without it, MoStream vs Random cannot distinguish the two, because a
        # frozen model trained on 16 molecules already beats uniform sampling.
        #
        # Only `fit` is skipped. The window still slides, weights are still emitted downstream,
        # Infer still scores its chunk, and Rank still ranks, so the arms differ in exactly one
        # factor: whether the gradient step happens. Passed via the constructor rather than read
        # from os.environ, for the same reason as persist_weights -- env vars set at submission
        # do not reach the Beam worker.
        self._train_once = bool(train_once)
        self._fitted = False        # set once the single fit has happened (train-once arm only)

        # Round-based baseline arm. When round_seconds > 0 the surrogate is refreshed at most once
        # per round_seconds seconds: a record inside the current round skips the gradient step and
        # re-emits the current weights (the same freeze the train_once arm uses), so the model the
        # ranking sees is refreshed only at round boundaries. This isolates the execution model,
        # continuous per-record steering versus task-based per-round steering, as the single factor,
        # with the window, data, procedure, and downstream operators held identical. 0 keeps the
        # continuous per-record fit. Passed via the constructor, like train_once, because the
        # submission env var does not reach the Beam worker.
        self._round_seconds = int(round_seconds)
        self._last_fit_ts = None    # perf_counter of the last gradient step (round-based arm)

        # WARM-START (E5 discovery arms). Path to a JSON file of pretrained surrogate weights in
        # get_weights() order -- the SAME list-of-tensors the fit path emits. When set, open()
        # loads it into self.model_paras so the surrogate starts from a trained checkpoint instead
        # of random init, and process_element short-circuits the not-ready gate so Infer scores and
        # Rank emits from the FIRST record rather than after a full-window warm-up (which at the
        # oracle rate wastes ~1h+). Empty/None keeps the from-scratch behaviour every other
        # experiment relies on. Passed via the constructor, like the flags above, because a
        # submission env var does not reach the Beam worker.
        self._pretrained_path = (pretrained_path or "").strip()
        self._warm_started = False   # set True in open() once the pretrained JSON is loaded

        # STATIC (no-fit) discovery arm. When True the surrogate NEVER takes a gradient step: it
        # stays the warm-started pretrained model but still emits weights and drives Infer/Rank, so
        # it isolates "a good surrogate" from "an ONLINE surrogate". Distinct from train_once, which
        # fits exactly once; freeze fits zero times. Forces do_fit=False unconditionally. Passed in
        # for the same reason as the flags above.
        self._freeze = bool(freeze)

        # STABLE ONLINE FINE-TUNE (continuous-arm stability fix). Default OFF, so every other arm
        # is byte-for-byte unchanged. The plain warm-start continuous arm takes ONE gradient step
        # per record on the sliding window, which during warm-up holds only 1-2 molecules; a single
        # Adam step on a 1-2 molecule batch (Adam's first update moves EVERY weight by ~lr regardless
        # of gradient magnitude) walks the converged surrogate off its training manifold and its
        # search-space predictions explode (~262 / -49 V). Even once the window fills, fitting only on
        # the ~12 V feedback cluster + re-deriving the 'scale' layer from that cluster collapses the
        # model toward a poorly-ranked near-constant.
        #
        # REPLAY fixes both at once: open() loads a FIXED random sample of the seed training set (the
        # data the surrogate was pretrained on), and every online update fits ONE pass over
        # (anchor + current window) at a GENTLE lr, with NO tiny-window scale-override. The anchor
        # keeps each step anchored to the training distribution, so the update NUDGES the converged
        # model instead of corrupting it, while the fresh window still steers it. Intended to be
        # combined with warm-start (MOSTREAM_PRETRAINED_WEIGHTS); resolved client-side in MDWorkflow
        # from MOSTREAM_REPLAY* and passed in, like the flags above, because a submission env var does
        # not reach the Beam worker.
        #
        # Validated locally against the three arms on both an in-distribution (real-IP) stream and an
        # OOD (search-space feedback) stream: BROKEN blows record 1 to ~130-310 V (all non-physical,
        # Spearman -0.68); STATIC is the frozen upper bound (rho 0.88); this REPLAY mode stays
        # physical from record 1 and holds Spearman 0.81-0.86 vs static 0.88 on both streams. The lr
        # matters: 1e-4 still spikes on record 1 (Adam's first step ~= lr per weight); 1e-5 does not,
        # so 1e-5 is the default. The anchor matters too: gentle-lr WITHOUT it drifts to ~0.74 on the
        # in-distribution stream. anchor + 1e-5 is the combination that passes.
        self._replay = bool(replay)
        self._replay_anchor = int(replay_anchor)       # seed molecules held as the anchor
        self._replay_lr = float(replay_lr)             # gentle online lr (baked in at compile time)
        self._replay_data = (replay_data or "").strip()  # path to the labeled seed set (smiles+ip)
        self._replay_min_window = max(1, int(replay_min_window))  # optional gate before the first fit
        self._anchor_md = None       # np.array of anchor mol dicts (set in open())
        self._anchor_y = None        # np.array of anchor IP targets
        self._replay_ok = False      # True once the anchor is loaded; else fit is suppressed
        if self._replay:
            # Bake the gentle lr into the optimizer built in open(); every other arm keeps 1e-3.
            self.learning_rate = self._replay_lr

        # Recovery consistency (reviewer change 5). When on, weights are written to IMMUTABLE,
        # versioned files and the exact version is committed into Flink-checkpointed keyed state,
        # so a failover reloads the version that MATCHES the restored training window rather than
        # whatever file is newest on disk. Off by default, preserving the single-file behaviour
        # the running cluster build uses. Resolved client-side in MDWorkflow from
        # MOSTREAM_VERSIONED_WEIGHTS and passed in, like persist_weights, because env vars set at
        # submission do not reach the Beam worker.
        # STATUS: implemented and partially validated on the cluster -- versions are persisted and
        # committed to checkpointed state. NOT a full recovery-consistency proof yet: Train is keyed
        # by model_id, so the counter is per-key and the versioned filename below must also carry the
        # key to avoid collisions across keys, and the four-phase kill test in
        # cloudlab/e8_recovery_consistency.py has not been run to completion. The paper accordingly
        # still treats atomic recovery as future work.
        self._versioned = bool(versioned_weights)
        self._restored = False      # whether the one post-restart version-aware reload has run
        self._retain = 12           # versioned files kept on /tmp before the oldest is pruned
        self._wver = None           # ValueState handle for the weight version (set in open())

        # TRAINING WINDOW SIZE, separated from the SGD minibatch (E5 sweep).
        #
        # `batch_size` used to mean three things at once: the sliding window's cap, the
        # not-ready threshold, and the minibatch handed to fit(). Only the first two are about
        # HOW MUCH DATA the surrogate learns from; the third is an optimizer detail. Conflating
        # them meant the surrogate could only ever see 16 molecules, which is why it memorises
        # that window (train MAE 0.22 V) and emits a ~constant for the other 1.1M candidates
        # (est_ip spread 0.20 V over 1164 recommendations, 0 hits against a 14.1% base rate).
        #
        # Separating them lets us measure the real trade-off. The window is NOT bounded by state
        # (512 SMILES+IP strings is ~36 KB, nothing); it is bounded by TIME. steps_per_epoch is
        # len(train_X)//minibatch, so a window N times larger costs N times more gradient steps
        # per record, and the per-record cost is precisely the steering latency this paper sells.
        # Surrogate quality and steering latency therefore trade off directly, and that curve is
        # the experiment.
        #
        # Default None keeps the historical behaviour exactly (window == batch_size == 16).
        self.window_size = int(window_size) if window_size else self.batch_size
        print(f"finished init (persist_weights={self._persist}, train_once={self._train_once}, "
              f"round_seconds={self._round_seconds}, "
              f"window_size={self.window_size}, minibatch={self.batch_size}, "
              f"versioned_weights={self._versioned}, "
              f"pretrained={self._pretrained_path or 'OFF'}, freeze={self._freeze}, "
              f"replay={self._replay} (anchor={self._replay_anchor}, lr={self._replay_lr}))")

    def _versioned_path(self, v):
        return f"/tmp/mostream_weights_{self._subtask}_v{v}.json"

    def _prune_versions(self, cur):
        # Bound /tmp: drop the file that just fell outside the retention window.
        lo = cur - self._retain
        if lo > 0:
            try:
                old = self._versioned_path(lo)
                if os.path.exists(old):
                    os.remove(old)
            except OSError:
                pass

    def _resolve_replay_data(self):
        """Locate the labeled seed set (smiles+ip) for the REPLAY anchor. Prefer the explicit path
        (resolved client-side in MDWorkflow from MOSTREAM_REPLAY_DATA); else fall back to the
        repo-relative training data, then a home-dir clone. For cluster use the file must be present
        on the TaskManager (it is NOT distributed by add_python_file, which only ships StreamML)."""
        if self._replay_data and os.path.exists(self._replay_data):
            return self._replay_data
        cand = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..',
                                'WLGenerator-node1', 'dataset', 'training-data-simple.txt'))
        if os.path.exists(cand):
            return cand
        return os.path.expanduser(
            '~/MoStream/MoStream/WLGenerator-node1/dataset/training-data-simple.txt')

    def _load_replay_anchor(self):
        """Load a FIXED random sample of the seed training set into memory as the replay anchor.
        Fixed seed => the same anchor every open()/recycle, so an update is reproducible."""
        import ast
        from moldesign.utils.conversions import convert_string_to_dict
        path = self._resolve_replay_data()
        rows = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = ast.literal_eval(line)
                    rows.append((d["smiles"], float(d["ip"])))
                except Exception:
                    continue
        if not rows:
            raise RuntimeError(f"no (smiles,ip) rows parsed from {path}")
        rnd = random.Random(20260801)   # fixed anchor sample
        rnd.shuffle(rows)
        rows = rows[:self._replay_anchor]
        md, y = [], []
        for s, ip in rows:
            try:
                md.append(convert_string_to_dict(s)); y.append(ip)
            except Exception:
                continue
        if not md:
            raise RuntimeError(f"anchor featurization produced 0 molecules from {path}")
        self._anchor_md = np.array(md, dtype=object)
        self._anchor_y = np.array(y, dtype=float)
        self._replay_ok = True
        print(f"[TrainFunction] REPLAY anchor loaded: {len(md)} seed molecules from {path} "
              f"(IP mean={self._anchor_y.mean():.2f} std={self._anchor_y.std():.2f})")

    def open(self, runtime_context: RuntimeContext):
        import h5py as _h5py
        import tensorflow as tf
        import nfp
        from moldesign.score.nfp import ReduceAtoms
        print("train reach open")
        self.state = runtime_context.get_state(ValueStateDescriptor('training_dataset', Types.LIST(Types.STRING())))

        subtask_idx = runtime_context.get_index_of_this_subtask()
        self._subtask = subtask_idx
        self._model_ckpt = f"/tmp/mostream_weights_{subtask_idx}.json"
        # E6 ablation switch (self._persist), set in __init__ from the client-side flag. Weight
        # persistence is the whole of contribution 2: after a worker teardown, open() re-executes,
        # and the ONLY thing that lets training resume rather than restart from scratch is
        # reloading the persisted weights here. With it off, both the reload (below) and the write
        # (in the fit path) are skipped -- the no-persistence arm of E6 -- so every teardown resets
        # the model to its initial weights and the training MAE should saw-tooth.
        # Versioned mode keeps its keyed-state handle regardless of warm-start; the actual restore
        # runs in process_element once key context exists (open() has no keyed state).
        if self._versioned:
            self._wver = runtime_context.get_state(
                ValueStateDescriptor('weight_version', Types.LONG()))

        if self._pretrained_path:
            # WARM-START: seed self.model_paras with the pretrained weights so the EXISTING
            # set_weights path in process_element reinstates them on the FIRST record. This takes
            # priority over the /tmp resume-checkpoint, so a warm-start run always begins from
            # exactly the known pretrained weights and never from stale /tmp state left by an
            # earlier from-scratch run. (Consequence: a mid-run worker recycle re-warm-starts from
            # the pretrained file rather than resuming the latest online weights. The static arm is
            # unaffected -- the pretrained file IS its permanent model -- and continuous/periodic
            # re-converge from it.)
            try:
                with open(self._pretrained_path) as f:
                    self.model_paras = f.read()
                _ntensors = len(json.loads(self.model_paras))
                self._warm_started = True
                print(f"[TrainFunction] warm-started from {self._pretrained_path} ({_ntensors} tensors)")
            except Exception as e:
                print(f"[TrainFunction] warm-start load FAILED from {self._pretrained_path}: {e}")
                self.model_paras = None
                self._warm_started = False
        elif self._versioned:
            print("[TrainFunction] versioned weights ON; will restore the checkpoint-referenced version")
        elif self._persist and os.path.exists(self._model_ckpt):
            try:
                with open(self._model_ckpt) as f:
                    self.model_paras = f.read()
                print(f"[TrainFunction] Resumed weights from {self._model_ckpt}")
            except Exception as e:
                print(f"[TrainFunction] Checkpoint load failed, starting fresh: {e}")
                self.model_paras = None
        elif not self._persist:
            print("[TrainFunction] MOSTREAM_PERSIST_WEIGHTS=0: NOT resuming (E6 no-persist arm)")
        else:
            print(f"[TrainFunction] No checkpoint at {self._model_ckpt}, starting fresh")


        custom_objects = nfp.custom_objects.copy()
        custom_objects['ReduceAtoms'] = ReduceAtoms
        if hasattr(nfp, 'GlobalUpdate'):  custom_objects['GlobalUpdate']  = nfp.GlobalUpdate
        if hasattr(nfp, 'EdgeUpdate'):    custom_objects['EdgeUpdate']    = nfp.EdgeUpdate
        if hasattr(nfp, 'NodeUpdate'):    custom_objects['NodeUpdate']    = nfp.NodeUpdate
        if hasattr(nfp, 'ConcatDense'):   custom_objects['ConcatDense']   = nfp.ConcatDense

        # Build and compile model once — was ~60s bottleneck when done per-record
        skip_pretrained = os.environ.get("MOSTREAM_SKIP_PRETRAINED", "1") == "1"
        if skip_pretrained:
            with _h5py.File(_default_model_path(), "r") as f:
                model_config = f.attrs["model_config"]
            self._model = tf.keras.models.model_from_json(model_config, custom_objects=custom_objects)
        else:
            self._model = tf.keras.models.load_model(
                _default_model_path(),
                custom_objects=custom_objects, compile=False)
            config = self._model.get_config()
            self._model = tf.keras.Model.from_config(config, custom_objects=custom_objects)

        self._infra_json_str = self._model.to_json()
        self._model.compile(
            tf.optimizers.Adam(self.learning_rate),
            'mean_squared_error',
            metrics=['mean_absolute_error'],
            steps_per_execution=1
        )
        # REPLAY: load the fixed seed anchor. On failure, keep _replay_ok False so process_element
        # SUPPRESSES the online fit (serving the warm-started pretrained model, which stays physical)
        # rather than degrading to the unstable no-anchor tiny-window fit.
        if self._replay:
            try:
                self._load_replay_anchor()
            except Exception as e:
                self._replay_ok = False
                print(f"[TrainFunction] REPLAY anchor load FAILED ({e}); online fit DISABLED, "
                      f"serving warm-started pretrained (STATIC) to stay physical")
        print("train finished open")

    def process_element(self, new_tuple, ctx: 'KeyedProcessFunction.Context') -> List:
        import tensorflow as tf
        import numpy as np
        from moldesign.utils.conversions import convert_string_to_dict
        from moldesign.score.nfp import make_data_loader
        # retrieve the current dataset
        current_dataset = self.state.value()
        # Recovery consistency: on the first record after a (re)start in versioned mode, reload the
        # exact weight version the checkpoint recorded, so the model matches the restored window
        # rather than the newest file on disk.
        if self._versioned and not self._restored:
            self._restored = True
            _rv = self._wver.value()
            if _rv is not None:
                _rvpath = self._versioned_path(_rv)
                try:
                    with open(_rvpath) as f:
                        self.model_paras = f.read()
                    print(f"[TrainFunction] Restored weights v{_rv} from checkpoint reference ({_rvpath})")
                except Exception as e:
                    print(f"[TrainFunction] Versioned restore of v{_rv} failed: {e}")
        new_x = new_tuple[0]
        new_y = new_tuple[1]
        model_id = int(new_tuple[2])
        # E0: producer-side timestamp (ms) of the simulation result being trained on.
        # Carried through unchanged so Rank can attribute its output back to this record.
        src_ts = int(new_tuple[3]) if len(new_tuple) > 3 else 0
        #print("model_id", model_id)

        # Sliding window capped at window_size (NOT batch_size, which is now only the minibatch).
        if (current_dataset is None):
            current_dataset = [str(new_x) + "," + str(new_y) + "," + str(model_id)]
        elif (len(current_dataset) < self.window_size):
             current_dataset.append(str(new_x) + "," + str(new_y) + str(model_id))
        else:
             current_dataset.pop(0)
             current_dataset.append(str(new_x) + "," + str(new_y) + str(model_id))

        # Update NPMM Model State
        self.state.update(current_dataset)
        # Make the data loaders
        x_tmp = []
        y_tmp = []
        for item in current_dataset:
            x_tmp.append(convert_string_to_dict(item.split(",")[0]))
            #y_new = float(item.split(",")[1])
            #if (y_new > 14):
            #   y_new = float(1)
            #else:
            #   y_new = float(0)
            y_tmp.append(float(item.split(",")[1]))
            #y_tmp.append(y_new)
        mol_dicts = np.array(x_tmp)
        # Quantize the padded molecule size, for the same reason as InferFunction: an exact
        # per-window maximum changes the input SHAPE on nearly every record, TensorFlow
        # retraces its graph, and it retains every traced ConcreteFunction. Bucketing bounds
        # the number of distinct shapes, and hence the retained graphs, at a small constant.
        max_size = _bucket_size(max(len(x['atom']) for x in mol_dicts))
        y = np.array(y_tmp)

        # Make the training and validation splits
        rng = np.random.RandomState(self.random_state)
        train_split = rng.rand(len(x_tmp)) > self.validation_split
        train_X = mol_dicts[train_split]
        train_y = y[train_split]
        valid_X = mol_dicts[~train_split]
        valid_y = y[~train_split]
        print("train_y: ", train_y)

        # Not-ready gate: the window must be full before the surrogate is worth serving. Scales
        # with window_size, so a larger window also costs a longer warm-up (512 records at the
        # seed rate of ~0.12 rec/s is ~71 min before the first real recommendation).
        # Warm-start short-circuits this gate: a pretrained model is already worth serving, so
        # Infer scores and Rank emits from the FIRST record instead of idling ~1h+ through a
        # full-window warm-up at the oracle rate. The window still slides and grows for online
        # updates below; only EMISSION is un-gated. When NOT warm-started the gate is unchanged, so
        # the latency/memory/scaling experiments still see the from-scratch warm-up behaviour.
        _in_warmup = self._warm_started and (len(current_dataset) < self.window_size)
        if (not self._warm_started) and (len(current_dataset) < self.window_size):
           result = [str(model_id) + "$"+ "haaah" + "$" + str(model_id) + "$" + "haaah" + "$" + str(src_ts)]
           #print("result_list", result)
           return result

        # Make the loaders — use actual dataset size as batch_size if smaller than
        # self.batch_size to avoid steps_per_epoch=0 after train/valid split
        loader_batch = min(self.batch_size, len(train_X), max(1, len(valid_X)))
        steps_per_epoch = max(1, len(train_X) // loader_batch)
        train_loader = make_data_loader(train_X, train_y, repeat=True, batch_size=loader_batch, max_size=max_size, drop_last_batch=False, shuffle_buffer=32768)
        valid_steps = max(1, len(valid_X) // loader_batch)
        valid_loader = make_data_loader(valid_X, valid_y, batch_size=loader_batch, max_size=max_size, drop_last_batch=False)

        # Restore weights from previous training round
        if self.model_paras is not None:
            weights_list = json.loads(self.model_paras)
            weights = [np.array(arr) for arr in weights_list]
            self._model.set_weights(weights)

        # Re-derive the output normalization (the 'scale' layer) from the current window. Skipped
        # during the warm-start warm-up (window not yet full): a partial window -- a single sample
        # on the first record -- gives std 0 and would collapse the pretrained model to a constant
        # prediction, which Rank cannot rank. The pretrained weights already carry a properly fit
        # scale layer, so trust it until a full window of real targets is available, after which
        # behaviour matches the from-scratch path exactly (the gate guarantees a full window there,
        # so _in_warmup is always False and this override always runs, unchanged).
        # REPLAY additionally skips the override: its fit batch is (seed anchor + window), whose
        # targets already span the full training IP range, so the pretrained 'scale' is correct and
        # re-deriving it from the narrow ~12 V feedback cluster would collapse the dynamic range.
        if (not _in_warmup) and (not self._replay):
            try:
                scaler_layer = self._model.get_layer('scale')
                outputs = np.array(y_tmp)
                scaler_layer.set_weights([outputs.std()[None, None], outputs.mean()[None]])
            except ValueError:
                pass

        # --- PROFILING: attribute the per-record cost. The measured service rate of Train
        # --- is only ~0.15 rec/s (~53 s of work per record per sub-task), which is far more
        # --- than one gradient step on 16 molecules should cost. These timers say where it
        # --- actually goes. Emitted as a single parseable line per record; parse with
        # --- cloudlab/parse_train_profile.py.
        import time as _t
        _t0 = _t.perf_counter()
        # Decide whether this record triggers a gradient step. Two baseline arms suppress it, and
        # both suppress ONLY the gradient step: the window still slides, the current weights are
        # still emitted, and Infer and Rank still run, so an arm differs from continuous in exactly
        # one factor.
        #   train_once  : fit once on the first full window, then freeze forever.
        #   round-based : fit at most once per self._round_seconds, freezing between rounds, so a
        #                 completed result becomes visible to the ranking only at a round boundary.
        if self._freeze:
            # STATIC arm: never fit. The model stays the warm-started pretrained one and simply
            # re-emits its weights so Infer/Rank still run. Wins over every other arm.
            do_fit = False
        elif self._replay and not self._replay_ok:
            # REPLAY requested but the anchor failed to load: suppress the fit (behave STATIC) so a
            # missing seed file cannot corrupt the model.
            do_fit = False
        elif self._replay:
            # REPLAY: fit every record once past the (optional) minimum-window gate. Default gate is
            # 1, so it fits from the first record; the anchor keeps even a 1-molecule window stable.
            do_fit = len(current_dataset) >= self._replay_min_window
        elif self._train_once and self._fitted:
            do_fit = False
        elif self._round_seconds > 0 and self._last_fit_ts is not None \
                and (_t0 - self._last_fit_ts) < self._round_seconds:
            do_fit = False
        else:
            do_fit = True

        if not do_fit:
            history = None
        elif self._replay:
            # STABLE ONLINE FINE-TUNE: fit ONE pass over (fixed seed anchor + current window). The
            # anchor dominates the batch and keeps the gradient step on the training manifold; the
            # window supplies the fresh steering signal. Deliberately ONE pass rather than
            # steps_per_epoch=len(train_X) -- the latter is ~16 epochs over the tiny window, which is
            # what memorizes and corrupts it. lr is the gentle replay lr (baked in at compile time).
            # max_size must cover the anchor, whose molecules may be larger than the window's.
            rX = np.concatenate([self._anchor_md, mol_dicts])
            rY = np.concatenate([self._anchor_y, y])
            r_max = _bucket_size(max(len(x['atom']) for x in rX))
            r_loader = make_data_loader(list(rX), rY, repeat=False, batch_size=self.batch_size,
                                        max_size=r_max, drop_last_batch=False, shuffle_buffer=4096)
            history = self._model.fit(r_loader, epochs=self.num_epochs, shuffle=False, verbose=False)
            self._last_fit_ts = _t0
        elif len(valid_X) == 0:
            # A warm-started early record can produce a train/valid split with an EMPTY validation
            # set (a 1-2 record window). Passing an empty validation dataset with validation_steps>=1
            # makes Keras run out of data mid-validation, so fit WITHOUT validation here. In the
            # from-scratch path the gate guarantees a full window before any fit, so valid is
            # non-empty and this branch is never taken -- default behaviour is unchanged.
            history = self._model.fit(
                train_loader,
                epochs=self.num_epochs,
                shuffle=False,
                verbose=False,
                steps_per_epoch=len(train_X),
            )
        else:
            history = self._model.fit(
                train_loader,
                epochs=self.num_epochs,
                shuffle=False,
                verbose=False,
                steps_per_epoch=len(train_X),
                validation_data=valid_loader,
                validation_steps=valid_steps,
                validation_freq=1
            )
            self._last_fit_ts = _t0
            if self._train_once and not self._fitted:
                self._fitted = True
                print("[TrainFunction] MOSTREAM_TRAIN_ONCE=1: model FROZEN after this fit "
                      "(E5 train-once arm)")
            if self._round_seconds > 0:
                print(f"[TrainFunction] ROUND fit subtask={self._subtask} "
                      f"t={_t0:.1f} round_seconds={self._round_seconds}")
        _t_fit = _t.perf_counter() - _t0

        # A frozen arm has no fresh history; report the last fit's metrics so the profile line
        # stays parseable and the arms produce identically-shaped logs.
        if history is not None:
            train_loss = history.history['loss']
            train_mae = history.history['mean_absolute_error']
            self._last_loss, self._last_mae = train_loss, train_mae
        else:
            train_loss = getattr(self, '_last_loss', [float('nan')])
            train_mae = getattr(self, '_last_mae', [float('nan')])

        # get_weights() -> nested Python lists. This is where the model becomes data.
        _t0 = _t.perf_counter()
        weights = []
        for v in self._model.get_weights():
            v = np.array(v)
            if np.isnan(v).any():
                raise ValueError('Found some NaN weights.')
            weights.append(v.tolist())
        _t_tolist = _t.perf_counter() - _t0

        # Serialize the ENTIRE model to a JSON string -- ~13.6 MB in our configuration --
        # once per record. This string is then (a) written to disk and (b) emitted into the
        # dataflow, where Infer must parse it back and reinstate it.
        _t0 = _t.perf_counter()
        weights_json_str = json.dumps(weights)
        _t_dumps = _t.perf_counter() - _t0
        _payload_mb = len(weights_json_str) / 1048576.0

        self.model_paras = weights_json_str

        # Persist to disk so weights survive a worker restart (see E6). Gated by the E6 switch:
        # the no-persistence arm writes nothing, so a re-executed open() finds no checkpoint.
        _t0 = _t.perf_counter()
        _wsha = hashlib.sha256(weights_json_str.encode()).hexdigest()[:16]
        if self._persist and self._versioned:
            # Immutable, versioned write, then commit the version into checkpointed state. Ordering
            # matters: the file exists before the state references it, so any checkpointed version
            # is always present on disk for a later restore. The sha lets the recovery-consistency
            # harness match the recovered model against a fault-free run at the same version.
            _v = (self._wver.value() or 0) + 1
            _vpath = self._versioned_path(_v)
            try:
                tmp = _vpath + ".tmp"
                with open(tmp, 'w') as f:
                    f.write(weights_json_str)
                os.replace(tmp, _vpath)
                self._wver.update(_v)
                self._prune_versions(_v)
                print(f"WEIGHTVER subtask={self._subtask} version={_v} sha={_wsha}")
            except Exception as e:
                print(f"[TrainFunction] Versioned save failed: {e}")
        elif self._persist:
            try:
                tmp = self._model_ckpt + ".tmp"
                with open(tmp, 'w') as f:
                    f.write(weights_json_str)
                os.replace(tmp, self._model_ckpt)
                print(f"WEIGHTVER subtask={self._subtask} version=NA sha={_wsha}")
            except Exception as e:
                print(f"[TrainFunction] Checkpoint save failed: {e}")
        _t_persist = _t.perf_counter() - _t0

        _t_total = _t_fit + _t_tolist + _t_dumps + _t_persist
        print(f"TRAINPROF subtask={self._subtask} model_id={model_id} "
              f"fit={_t_fit:.3f} tolist={_t_tolist:.3f} dumps={_t_dumps:.3f} "
              f"persist={_t_persist:.3f} total={_t_total:.3f} payload_mb={_payload_mb:.2f} "
              f"loss={train_loss[-1]:.4f} mae={train_mae[-1]:.4f}")

        chunk_id = random.choice(range(2231))
        result = [str(chunk_id) + "$"+ weights_json_str + "$" + str(model_id) + "$" + self._infra_json_str + "$" + str(src_ts)]
        return result
