#!/usr/bin/env python3.9
"""
Cumulative high-IP DISCOVERY over wall-clock time: online (MoStream AL) vs static vs random.

Same in-distribution search-space pool + real xTB labels + model-build/pretrain/stable-online
machinery as insearch_al.py. The campaign runs as a batched active-learning loop over a candidate
POOL of real search-space molecules (true xtb_ip known, so no live oracle needed):

  each round r:                                     (batch B = 16)
    STATIC  rank the remaining pool by the FROZEN prior; take top-B
    ONLINE  rank by the CURRENT surrogate; take top-B; then retrain on the acquired batch
            with the stable update (anchor replay from the prior seed + reservoir + gentle lr)
    RANDOM  take B uniformly at random
    a molecule is a "discovery" iff its true xtb_ip >= HIGH_THR (top decile of this dataset)

x-axis is wall-clock hours at the MEASURED campaign throughput (0.024 mol/s aggregate across the
3 simulators, i.e. Colmena's rate) so the curve is directly comparable to the E5 figure.

Shared WEAKEST prior k=100 (trained on 100 labels) for static+online -- the regime where online
has the most room. Multiple seeds -> mean + spread band in the plot.
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

# --------------------------------------------------------------------- config
def _ei(n, d): return int(os.environ.get(n, d))
def _ef(n, d): return float(os.environ.get(n, d))
SEEDS        = [int(x) for x in os.environ.get("DISC_SEEDS", "0,1,2").split(",")]
PRIOR_K      = _ei("DISC_PRIOR", 100)        # weakest prior = trained on 100 labels
N_PVAL       = _ei("DISC_NPVAL", 150)        # disjoint early-stopping set (NOT in the pool)
BATCH        = _ei("DISC_BATCH", 16)
ROUNDS       = _ei("DISC_ROUNDS", 64)        # 64*16 = 1024 evaluations
LR_ONLINE    = _ef("DISC_LR", 1e-4)
EPOCHS_UPD   = _ei("DISC_EPUPD", 8)
ANCHOR_MAX   = 100
RESERVOIR_MAX= 64
PRETRAIN_LR  = 1e-3
PRETRAIN_MAXEP = _ei("DISC_MAXEP", 200)
PRETRAIN_PAT = _ei("DISC_PAT", 25)
ATOM_CAP     = _ei("DISC_ATOMCAP", 48)
USE_STATIC_ROWS = _ei("DISC_USE_STATIC", 0)  # 0 = unbiased rows only
RATE_PER_S   = _ef("DISC_RATE", 0.024)       # measured campaign throughput (mol/s), Colmena-matched
HRS_PER_EVAL = 1.0 / (RATE_PER_S * 3600.0)   # hours per single oracle evaluation
_AB = 16
def bucket(n): return int(((int(n) + _AB - 1) // _AB) * _AB)
def log(*a): print(*a, flush=True)

# ------------------------------------------------------------- custom objects
CO = nfp.custom_objects.copy(); CO['ReduceAtoms'] = ReduceAtoms
for n in ('GlobalUpdate', 'EdgeUpdate', 'NodeUpdate', 'ConcatDense'):
    if hasattr(nfp, n): CO[n] = getattr(nfp, n)
with h5py.File(MODEL_H5, "r") as f:
    _MC = f.attrs["model_config"]
if isinstance(_MC, bytes): _MC = _MC.decode()
def build():
    return tf.keras.models.model_from_json(_MC, custom_objects=CO)

# --------------------------------------------------------------------- data
def load_csv():
    rows = []
    with open(CSV) as f:
        for d in csv.DictReader(f):
            if not USE_STATIC_ROWS and int(d["unbiased"]) == 0:
                continue
            rows.append((d["smiles"], float(d["xtb_ip"])))
    return rows

log("[data] featurizing in-distribution labeled molecules ...")
t0 = time.time()
md_all, y_all, sm_all = [], [], []
for s, ip in load_csv():
    try:
        md_all.append(convert_string_to_dict(s)); y_all.append(ip); sm_all.append(s)
    except Exception:
        pass
md_all = np.array(md_all, dtype=object); y_all = np.array(y_all, np.float64); sm_all = np.array(sm_all, dtype=object)
_na = np.array([len(x['atom']) for x in md_all]); _keep = _na <= ATOM_CAP
n_drop = int((~_keep).sum())
md_all, y_all, sm_all = md_all[_keep], y_all[_keep], sm_all[_keep]
GLOBAL = bucket(ATOM_CAP)
HIGH_THR = float(np.quantile(y_all, 0.90))
n_high_all = int((y_all >= HIGH_THR).sum())
log(f"[data] N={len(md_all)} kept (dropped {n_drop} >{ATOM_CAP} atoms) in {time.time()-t0:.1f}s "
    f"| GLOBAL={GLOBAL} | IP mean={y_all.mean():.3f} min={y_all.min():.3f} max={y_all.max():.3f} "
    f"| high-IP thr(90pct)={HIGH_THR:.4f} | n_high={n_high_all} | rate={RATE_PER_S} mol/s "
    f"({1/HRS_PER_EVAL:.1f} mol/h)")

# --------------------------------------------------------------------- helpers
def predict(m, md, batch=512):
    l = make_data_loader(list(md), batch_size=min(batch, len(md)), repeat=False, max_size=GLOBAL)
    return np.asarray(m.predict(l, verbose=0)).reshape(-1)

def pretrain(prior_md, prior_y, pval_md, pval_y, seed):
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

def online_update(m, anchor_md, anchor_y, older_md, older_y, recent_md, recent_y, rng):
    parts_md = [anchor_md]; parts_y = [anchor_y]
    if len(older_md) > 0:
        if len(older_md) > RESERVOIR_MAX:
            sel = rng.choice(len(older_md), RESERVOIR_MAX, replace=False)
            parts_md.append(older_md[sel]); parts_y.append(older_y[sel])
        else:
            parts_md.append(older_md); parts_y.append(older_y)
    parts_md.append(recent_md); parts_y.append(recent_y)
    X = np.concatenate(parts_md); Y = np.concatenate(parts_y)
    l = make_data_loader(list(X), Y, repeat=False, batch_size=16, max_size=GLOBAL,
                         drop_last_batch=False, shuffle_buffer=4096)
    m.fit(l, epochs=EPOCHS_UPD, verbose=False, shuffle=False)

# --------------------------------------------------------------------- run
all_curves = {}   # seed -> {static:[...], online:[...], random:[...]}
meta = {}
for SEED in SEEDS:
    idx = np.arange(len(md_all)); np.random.RandomState(SEED).shuffle(idx)
    prior_sel = idx[:PRIOR_K]
    pval_sel  = idx[PRIOR_K:PRIOR_K + N_PVAL]
    pool_sel  = idx[PRIOR_K + N_PVAL:]
    prior_md, prior_y = md_all[prior_sel], y_all[prior_sel]
    pval_md,  pval_y  = md_all[pval_sel],  y_all[pval_sel]
    pool_md,  pool_y  = md_all[pool_sel],  y_all[pool_sel]
    ishigh = pool_y >= HIGH_THR
    n_high_pool = int(ishigh.sum())
    R = min(ROUNDS, len(pool_md) // BATCH)
    log("\n" + "#" * 78)
    log(f"SEED {SEED} | prior(k)={len(prior_sel)} pval={len(pval_sel)} POOL={len(pool_sel)} "
        f"| pool high-IP={n_high_pool} | rounds={R} batch={BATCH}")

    m, ep, best_val = pretrain(prior_md, prior_y, pval_md, pval_y, SEED)
    W0 = m.get_weights()
    pri_pred = predict(m, pool_md)
    log(f"[prior seed={SEED} k={PRIOR_K}] {ep} ep PVAL-MAE={best_val:.4f} "
        f"pred[pool] min/max={pri_pred.min():.2f}/{pri_pred.max():.2f}")

    # ---- STATIC: frozen prior ranks the pool once; consume top-B each round
    order = np.argsort(-pri_pred)
    static_cum = [0]
    hs = 0
    for r in range(R):
        picked = order[r*BATCH:(r+1)*BATCH]
        hs += int(ishigh[picked].sum()); static_cum.append(hs)

    # ---- RANDOM: uniform picks (no model)
    rr = np.random.RandomState(1000 + SEED)
    perm = rr.permutation(len(pool_md))
    random_cum = [0]; hr = 0
    for r in range(R):
        picked = perm[r*BATCH:(r+1)*BATCH]
        hr += int(ishigh[picked].sum()); random_cum.append(hr)

    # ---- ONLINE: rerank each round with the updated surrogate, retrain on acquired batch
    tf.random.set_seed(SEED); np.random.seed(SEED)
    mo = build(); mo.set_weights(W0)
    mo.compile(tf.optimizers.Adam(LR_ONLINE), 'mean_squared_error', metrics=['mean_absolute_error'])
    arng = random.Random(SEED)
    a_n = min(ANCHOR_MAX, len(prior_md)); a_sel = arng.sample(range(len(prior_md)), a_n)
    anchor_md, anchor_y = prior_md[a_sel], prior_y[a_sel]
    rng = np.random.RandomState(SEED)
    remaining = np.ones(len(pool_md), dtype=bool)
    res_md, res_y = [], []
    online_cum = [0]; ho = 0
    tk = time.time()
    for r in range(R):
        rem_ix = np.where(remaining)[0]
        preds = predict(mo, pool_md[rem_ix])
        top = rem_ix[np.argsort(-preds)[:BATCH]]
        ho += int(ishigh[top].sum()); online_cum.append(ho)
        remaining[top] = False
        rec_md = pool_md[top]; rec_y = pool_y[top]
        older_md = np.array(res_md, dtype=object) if res_md else np.array([], dtype=object)
        older_y  = np.array(res_y) if res_y else np.array([])
        online_update(mo, anchor_md, anchor_y, older_md, older_y, rec_md, rec_y, rng)
        res_md.extend(list(rec_md)); res_y.extend(list(rec_y))
    log(f"[online seed={SEED}] {R} rounds in {time.time()-tk:.0f}s | "
        f"FINAL discoveries  online={online_cum[-1]}  static={static_cum[-1]}  random={random_cum[-1]} "
        f"(of {n_high_pool} in pool)")

    all_curves[str(SEED)] = dict(static=static_cum, online=online_cum, random=random_cum)
    meta[str(SEED)] = dict(n_high_pool=n_high_pool, pool=len(pool_sel), rounds=R,
                           prior_ep=ep, prior_val=best_val)

# --------------------------------------------------------------------- save
out = dict(
    config=dict(SEEDS=SEEDS, PRIOR_K=PRIOR_K, N_PVAL=N_PVAL, BATCH=BATCH, ROUNDS=ROUNDS,
                LR_ONLINE=LR_ONLINE, EPOCHS_UPD=EPOCHS_UPD, HIGH_THR=HIGH_THR,
                RATE_PER_S=RATE_PER_S, HRS_PER_EVAL=HRS_PER_EVAL, GLOBAL=GLOBAL,
                N_data=len(md_all), n_high_all=n_high_all, IP_max=float(y_all.max())),
    curves=all_curves, meta=meta)
_rj = os.path.join(SCR, "insearch_discovery_results.json")
with open(_rj, "w") as f:
    json.dump(out, f, indent=1)
log(f"\n[saved] {_rj}")
log("DONE-DISCOVERY")
