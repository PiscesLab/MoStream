import random, json, os
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


def _bucket_size(n):
    """Round a molecule size up to the next multiple of _ATOM_BUCKET."""
    return int(((int(n) + _ATOM_BUCKET - 1) // _ATOM_BUCKET) * _ATOM_BUCKET)


class TrainFunction(KeyedProcessFunction):

    def __init__(self, persist_weights=True, train_once=False, window_size=None):
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
              f"window_size={self.window_size}, minibatch={self.batch_size})")

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
        if self._persist and os.path.exists(self._model_ckpt):
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
            with _h5py.File("/mnt/media/MDStream/StreamML/networks/model.h5", "r") as f:
                model_config = f.attrs["model_config"]
            self._model = tf.keras.models.model_from_json(model_config, custom_objects=custom_objects)
        else:
            self._model = tf.keras.models.load_model(
                "/mnt/media/MDStream/StreamML/networks/model.h5",
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
        print("train finished open")

    def process_element(self, new_tuple, ctx: 'KeyedProcessFunction.Context') -> List:
        import tensorflow as tf
        import numpy as np
        from moldesign.utils.conversions import convert_string_to_dict
        from moldesign.score.nfp import make_data_loader
        # retrieve the current dataset
        current_dataset = self.state.value()
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
        if (len(current_dataset) < self.window_size):
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
        if self._train_once and self._fitted:
            # E5 train-once arm, after the single fit: skip the gradient step and re-emit the
            # frozen weights. Everything downstream is unchanged, so the arms differ only in
            # whether the model keeps learning from the stream.
            history = None
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
            if self._train_once and not self._fitted:
                self._fitted = True
                print("[TrainFunction] MOSTREAM_TRAIN_ONCE=1: model FROZEN after this fit "
                      "(E5 train-once arm)")
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
        if self._persist:
            try:
                tmp = self._model_ckpt + ".tmp"
                with open(tmp, 'w') as f:
                    f.write(weights_json_str)
                os.replace(tmp, self._model_ckpt)
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
