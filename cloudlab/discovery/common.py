#!/usr/bin/env python3.9
"""Shared protocol machinery for the Colmena arm of the discovery figure.

Everything here is copied verbatim from insearch_discovery.py so that the pool, the split,
the prior and the ranking are identical. Nothing in insearch_discovery.py is modified.
"""
import os, sys, json, time, random, csv, h5py
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
os.environ.setdefault("KMP_WARNINGS", "0")

STREAMML = os.path.join(REPO, "MoStream", "MDStream", "StreamML")
sys.path.insert(0, STREAMML)
MODEL_H5 = os.path.join(STREAMML, "networks", "model.h5")
import os as _os
REPO = _os.environ.get("MOSTREAM_REPO") or _os.path.abspath(
    _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", ".."))
SCR = _os.environ.get("MOSTREAM_DISCOVERY_DIR") or _os.path.join(REPO, "results", "discovery")
CSV = os.path.join(SCR, "insearch_labels.csv")

import numpy as np
import nfp
import tensorflow as tf
from tensorflow.python.keras import callbacks as cb
from moldesign.score.nfp import ReduceAtoms, make_data_loader
from moldesign.utils.conversions import convert_string_to_dict

# --------------------------------------------------------------------- config (identical)
def _ei(n, d): return int(os.environ.get(n, d))
def _ef(n, d): return float(os.environ.get(n, d))
SEEDS        = [int(x) for x in os.environ.get("DISC_SEEDS", "0,1,2").split(",")]
PRIOR_K      = _ei("DISC_PRIOR", 100)
N_PVAL       = _ei("DISC_NPVAL", 150)
BATCH        = _ei("DISC_BATCH", 16)
ROUNDS       = _ei("DISC_ROUNDS", 64)
LR_ONLINE    = _ef("DISC_LR", 1e-4)
EPOCHS_UPD   = _ei("DISC_EPUPD", 8)
ANCHOR_MAX   = 100
RESERVOIR_MAX= 64
PRETRAIN_LR  = 1e-3
PRETRAIN_MAXEP = _ei("DISC_MAXEP", 200)
PRETRAIN_PAT = _ei("DISC_PAT", 25)
ATOM_CAP     = _ei("DISC_ATOMCAP", 48)
USE_STATIC_ROWS = _ei("DISC_USE_STATIC", 0)
RATE_PER_S   = _ef("DISC_RATE", 0.024)
HRS_PER_EVAL = 1.0 / (RATE_PER_S * 3600.0)
_AB = 16
def bucket(n): return int(((int(n) + _AB - 1) // _AB) * _AB)
def log(*a): print(*a, flush=True)

# ------------------------------------------------------------- custom objects (identical)
CO = nfp.custom_objects.copy(); CO['ReduceAtoms'] = ReduceAtoms
for n in ('GlobalUpdate', 'EdgeUpdate', 'NodeUpdate', 'ConcatDense'):
    if hasattr(nfp, n): CO[n] = getattr(nfp, n)
with h5py.File(MODEL_H5, "r") as f:
    _MC = f.attrs["model_config"]
if isinstance(_MC, bytes): _MC = _MC.decode()
def build():
    return tf.keras.models.model_from_json(_MC, custom_objects=CO)

# --------------------------------------------------------------------- data (identical)
def load_csv():
    rows = []
    with open(CSV) as f:
        for d in csv.DictReader(f):
            if not USE_STATIC_ROWS and int(d["unbiased"]) == 0:
                continue
            rows.append((d["smiles"], float(d["xtb_ip"])))
    return rows

def load_pool():
    md_all, y_all, sm_all = [], [], []
    for s, ip in load_csv():
        try:
            md_all.append(convert_string_to_dict(s)); y_all.append(ip); sm_all.append(s)
        except Exception:
            pass
    md_all = np.array(md_all, dtype=object)
    y_all = np.array(y_all, np.float64)
    sm_all = np.array(sm_all, dtype=object)
    _na = np.array([len(x['atom']) for x in md_all]); _keep = _na <= ATOM_CAP
    n_drop = int((~_keep).sum())
    md_all, y_all, sm_all = md_all[_keep], y_all[_keep], sm_all[_keep]
    return md_all, y_all, sm_all, n_drop

# --------------------------------------------------------------------- helpers (identical)
def predict(m, md, GLOBAL, batch=512):
    l = make_data_loader(list(md), batch_size=min(batch, len(md)), repeat=False, max_size=GLOBAL)
    return np.asarray(m.predict(l, verbose=0)).reshape(-1)

def pretrain(prior_md, prior_y, pval_md, pval_y, seed, GLOBAL):
    tf.random.set_seed(seed); np.random.seed(seed)
    m = build()
    try:
        m.get_layer('scale').set_weights([prior_y.std()[None, None], prior_y.mean()[None]])
    except ValueError:
        pass
    m.compile(tf.optimizers.Adam(PRETRAIN_LR), 'mean_squared_error',
              metrics=['mean_absolute_error'], steps_per_execution=1)
    pb = min(128, max(16, len(prior_md) // 4)); steps = max(1, int(np.ceil(len(prior_md)/pb)))
    tl = make_data_loader(list(prior_md), prior_y, repeat=True, batch_size=pb, max_size=GLOBAL,
                          drop_last_batch=False, shuffle_buffer=32768)
    vb = min(128, len(pval_md)); vsteps = max(1, len(pval_md)//vb)
    vl = make_data_loader(list(pval_md), pval_y, repeat=True, batch_size=vb, max_size=GLOBAL, drop_last_batch=False)
    early = cb.EarlyStopping(monitor="val_mean_absolute_error", mode="min",
                             patience=PRETRAIN_PAT, restore_best_weights=True, verbose=0)
    rlr = cb.ReduceLROnPlateau(monitor="val_mean_absolute_error", mode="min",
                               factor=0.5, patience=8, min_lr=1e-6, verbose=0)
    h = m.fit(tl, epochs=PRETRAIN_MAXEP, steps_per_epoch=steps, verbose=0, shuffle=False,
              validation_data=vl, validation_steps=vsteps, validation_freq=1,
              callbacks=[early, rlr, cb.TerminateOnNaN()])
    return m, len(h.history['loss']), min(h.history['val_mean_absolute_error'])

def split(md_all, y_all, SEED):
    idx = np.arange(len(md_all)); np.random.RandomState(SEED).shuffle(idx)
    prior_sel = idx[:PRIOR_K]
    pval_sel  = idx[PRIOR_K:PRIOR_K + N_PVAL]
    pool_sel  = idx[PRIOR_K + N_PVAL:]
    return prior_sel, pval_sel, pool_sel
