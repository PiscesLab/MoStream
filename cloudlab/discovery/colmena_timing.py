#!/usr/bin/env python3.9
"""STEP 2: measure Colmena's real ML overhead AT THIS POOL SIZE.

Runs upstream Colmena's own functions, in the env the live campaign used
(colmena-upstream: tf 2.8.4, nfp 0.3.9), on the same model.h5 architecture:

  (a) retrain_mpnn(num_epochs=128, learning_rate=1e-3, timeout=2700, batch_size=32,
                   validation_split=0.1, random_state=0)   <- exactly the live run's call
      over the accumulated labelled sizes this protocol spans (100 ... 1124 molecules)
  (b) evaluate_mpnn(..., batch_size=128) over the full 1384-molecule candidate pool

Resources match the live run's ML worker: COLMENA_ML_WORKERS=1, one process.
"""
import os, sys, json, time
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
import numpy as np
import h5py

UP = _os.environ.get("UPSTREAM_DIR",
                     _os.path.expanduser("~/colmena-upstream")) + "/molecular-design"
sys.path.insert(0, UP)
import os as _os
REPO = _os.environ.get("MOSTREAM_REPO") or _os.path.abspath(
    _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", ".."))
SCR = _os.environ.get("MOSTREAM_DISCOVERY_DIR") or _os.path.join(REPO, "results", "discovery")
MODEL_H5 = _os.path.join(REPO, "MoStream", "MDStream", "StreamML", "networks", "model.h5")

import tensorflow as tf
from moldesign.score.nfp import retrain_mpnn, evaluate_mpnn, NFPMessage, custom_objects
from moldesign.utils.conversions import convert_string_to_dict

RETRAIN_REPS = int(os.environ.get("RETRAIN_REPS", 2))
SCORE_REPS = int(os.environ.get("SCORE_REPS", 5))
RATE_PER_S = 0.024

def log(*a): print(*a, flush=True)

log(f"[env] tf {tf.__version__}  python {sys.version.split()[0]}")
model = tf.keras.models.load_model(MODEL_H5, custom_objects=custom_objects)
model_config = model.get_config()          # run.py sends get_config(); retrain builds from scratch
log(f"[model] loaded {MODEL_H5}  params={model.count_params()}")

data = json.load(open(os.path.join(SCR, "step2_sets.json")))
pool_smiles = data['pool_smiles']
log(f"[data] pool to score = {len(pool_smiles)} molecules; "
    f"train sizes = {sorted(int(k) for k in data['sets'])}")

# ----------------------------------------------------------------- (a) retrain
retrain = {}
for key in sorted(data['sets'], key=int):
    pairs = data['sets'][key]
    database = {s: v for s, v in pairs}
    ts, eps = [], []
    for rep in range(RETRAIN_REPS):
        # timeout=None, not the live run's 2700. At this pool size a retrain takes seconds to
        # minutes, so the 45-minute limit is never reached and training is unaffected. It is
        # passed as None only because upstream's `if timeout is not None` branch then calls
        # model.set_weights(early_stopping.best_weights) unguarded, and at N<~320 the training
        # set is small enough that valid_steps = len(valid_X)//32 == 0, so no validation runs,
        # EarlyStopping(monitor='val_loss') never fires and best_weights stays None -> crash.
        # Everything else is the live run's call verbatim.
        t0 = time.time()
        weights, hist = retrain_mpnn(model_config, database, num_epochs=128,
                                     learning_rate=1e-3, timeout=None,
                                     batch_size=32, validation_split=0.1, random_state=0)
        dt = time.time() - t0
        ts.append(dt); eps.append(len(hist['loss']))
        # At the smallest sizes upstream's valid_steps = len(valid_X)//32 is 0, so no validation
        # runs and its default EarlyStopping(monitor='val_loss') cannot fire. That is upstream's
        # own behaviour at this scale; record whether it happened rather than assume it away.
        vm = hist.get('val_mean_absolute_error')
        log(f"[retrain] N={key:>5}  rep{rep}  {dt:7.1f}s  epochs={len(hist['loss'])}  "
            f"val_mae={min(vm):.4f}" if vm else
            f"[retrain] N={key:>5}  rep{rep}  {dt:7.1f}s  epochs={len(hist['loss'])}  "
            f"(no validation split at this size -> early stopping inactive)")
    retrain[key] = dict(times=ts, epochs=eps, mean=float(np.mean(ts)),
                        n_train=len(database), validated=bool(vm))
    log(f"[retrain] N={key:>5}  mean={np.mean(ts):.1f}s over {RETRAIN_REPS} reps  "
        f"epochs={eps}")

# ----------------------------------------------------------------- (b) score
mol_dicts = [convert_string_to_dict(s) for s in pool_smiles]
msg = NFPMessage(model)
score_ts = []
for rep in range(SCORE_REPS):
    t0 = time.time()
    y = evaluate_mpnn(msg, mol_dicts, batch_size=128)
    dt = time.time() - t0
    score_ts.append(dt)
    log(f"[score]  pool={len(mol_dicts)}  rep{rep}  {dt:7.2f}s  (n_pred={np.size(y)})")
log(f"[score]  mean={np.mean(score_ts):.2f}s  sd={np.std(score_ts):.2f}s over {SCORE_REPS} reps")

out = dict(retrain=retrain, score=dict(times=score_ts, mean=float(np.mean(score_ts)),
                                       sd=float(np.std(score_ts)), n=len(mol_dicts)),
           rate_per_s=RATE_PER_S, tf=tf.__version__,
           retrain_reps=RETRAIN_REPS, score_reps=SCORE_REPS)
json.dump(out, open(os.path.join(SCR, "step2_timing.json"), "w"), indent=1)

# ----------------------------------------------------------------- derived quantities
t_first = retrain['100']['mean'] + out['score']['mean']
log(f"\n[derived] startup (retrain on the 100 initial labels + one full pool score) "
    f"= {t_first:.1f}s = {t_first*RATE_PER_S:.2f} evaluations at {RATE_PER_S} mol/s")
for key in sorted(retrain, key=int):
    t = retrain[key]['mean'] + out['score']['mean']
    log(f"[derived] refresh at N={key:>5}: {t:7.1f}s = {t*RATE_PER_S:5.2f} evaluations")
log("DONE-STEP2")
