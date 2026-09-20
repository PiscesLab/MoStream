"""Task functions for the Colmena baseline.

These are deliberately thin wrappers. Every one of them calls the SAME code the
streaming arm calls, so the two systems differ in how work is scheduled and not in
what the work is:

  simulate_molecule  -> SimulateFromSmiles, the xTB oracle from the simulator
  train_surrogate    -> the MPNN architecture in model.h5, window and optimizer
                        settings copied from NPMMModel.TrainFunction
  score_chunk        -> the chunk reader and predict path from Inference.py

Anything that differs between the arms is a confound. If you change a constant here,
change it in the streaming operator too, or the comparison stops meaning anything.
"""
import json
import os
import time
from typing import List, Optional, Tuple

# Policy constants. These mirror the streaming operators exactly.
WINDOW_SIZE = 16        # NPMMModel.window_size, the sliding training window
MINIBATCH = 16          # NPMMModel.batch_size, the SGD minibatch
LEARNING_RATE = 1e-3    # NPMMModel.learning_rate
CHUNK_SIZE = 500        # MDWorkflow chunk, how many candidates one update scores
HIT_THRESHOLD = 14.0    # a discovery, in volts, same as plot_discovery.HIT


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def _streamml_dir() -> str:
    return os.path.join(_repo_root(), 'MoStream', 'MDStream', 'StreamML')


def simulate_molecule(smiles: str) -> Tuple[str, Optional[float], float]:
    """Run the xTB oracle on one molecule.

    Returns (smiles, ionization potential in volts or None if it failed, seconds).
    Imports the streaming arm's own oracle so both arms run identical chemistry.
    """
    import sys
    sim_dir = os.path.join(_repo_root(), 'MoStream', 'WLGenerator-node1')
    if sim_dir not in sys.path:
        sys.path.insert(0, sim_dir)
    from simulator import SimulateFromSmiles

    started = time.time()
    try:
        ip = SimulateFromSmiles(smiles)
    except Exception:
        ip = None
    return smiles, (float(ip) if ip is not None else None), time.time() - started


def _build_model():
    """Load the MPNN architecture, the same file and the same custom objects."""
    import sys
    sml = _streamml_dir()
    if sml not in sys.path:
        sys.path.insert(0, sml)
    import h5py
    import nfp
    import tensorflow as tf
    from NPMMModel import _default_model_path
    from moldesign.score.nfp import ReduceAtoms

    custom = nfp.custom_objects.copy()
    custom['ReduceAtoms'] = ReduceAtoms
    for name in ('GlobalUpdate', 'EdgeUpdate', 'NodeUpdate', 'ConcatDense'):
        if hasattr(nfp, name):
            custom[name] = getattr(nfp, name)
    with h5py.File(_default_model_path(), 'r') as f:
        cfg = f.attrs['model_config']
    model = tf.keras.models.model_from_json(cfg, custom_objects=custom)
    model.compile(tf.optimizers.Adam(LEARNING_RATE), 'mean_squared_error',
                  metrics=['mean_absolute_error'], steps_per_execution=1)
    return model


def train_surrogate(records: List[Tuple[str, float]],
                    weights: Optional[str] = None) -> str:
    """Fit the surrogate on the most recent WINDOW_SIZE labelled molecules.

    `records` is [(smiles, ip), ...] in arrival order. `weights` is the JSON blob
    returned by a previous call, or None to start from the architecture alone.
    Returns the updated weights as JSON, which is how the streaming arm also moves
    them between operators.
    """
    import sys
    sml = _streamml_dir()
    if sml not in sys.path:
        sys.path.insert(0, sml)
    import numpy as np
    from moldesign.score.nfp import make_data_loader
    from moldesign.utils.conversions import convert_string_to_dict

    model = _build_model()
    if weights:
        model.set_weights([np.array(w) for w in json.loads(weights)])

    window = records[-WINDOW_SIZE:]
    if not window:
        return json.dumps([w.tolist() for w in model.get_weights()])

    x = [convert_string_to_dict(s) for s, _ in window]
    y = [float(ip) for _, ip in window]
    max_size = int(np.ceil(max(len(m['atom']) for m in x) / 16.0) * 16)
    loader = make_data_loader(x, values=y, repeat=True, batch_size=min(MINIBATCH, len(window)),
                              max_size=max_size, drop_last_batch=False, shuffle_buffer=32768)
    model.fit(loader, steps_per_epoch=max(1, len(window) // MINIBATCH), epochs=1, verbose=0)
    return json.dumps([w.tolist() for w in model.get_weights()])


def score_chunk(chunk_id: int, weights: str) -> List[Tuple[str, float]]:
    """Predict the ionization potential for one CHUNK_SIZE block of candidates.

    Reads the same search-space file, by the same fixed non-overlapping offsets, as
    the streaming arm's Infer operator.
    """
    import sys
    sml = _streamml_dir()
    if sml not in sys.path:
        sys.path.insert(0, sml)
    import numpy as np
    from Inference import load_search_space
    from moldesign.score.nfp import make_data_loader
    from moldesign.utils.conversions import convert_string_to_dict

    smiles_list = load_search_space(chunk_id)
    if not smiles_list:
        return []
    model = _build_model()
    model.set_weights([np.array(w) for w in json.loads(weights)])

    mols, kept = [], []
    for s in smiles_list:
        try:
            mols.append(convert_string_to_dict(s))
            kept.append(s)
        except Exception:
            continue
    if not mols:
        return []
    max_size = int(np.ceil(max(len(m['atom']) for m in mols) / 16.0) * 16)
    loader = make_data_loader(mols, values=None, repeat=False, batch_size=64,
                              max_size=max_size, drop_last_batch=False)
    preds = model.predict(loader, verbose=0).reshape(-1)
    return [(s, float(p)) for s, p in zip(kept, preds)]
