#!/usr/bin/env python3.9
"""
Pretrain the MoStream MPNN surrogate (nfp 0.1.3) on the seed IP dataset and
export its weights as the exact JSON the streaming pipeline consumes.

WHY THIS EXISTS
---------------
The trained .h5 files on disk (networks/model.h5, model-local.h5,
model-1687718027256.h5) carry ORPHANED weights: they were saved by an nfp
whose EdgeUpdate layer had 10 weight tensors, while every nfp the project can
run (0.1.3 / 0.3.12) builds EdgeUpdate with 4. So tf.keras.models.load_model
fails with a weight-count mismatch and the pipeline always trains from random
init. This script produces a usable, drop-in warm start.

The architecture, custom_objects, featurization, scale-layer logic, and weight
serialization here are copied EXACTLY from the pipeline so the JSON loads
straight into TrainFunction / InferFunction:
  - build:      tf.keras.models.model_from_json(model.h5['model_config'], custom_objects)
                (NPMMModel.TrainFunction.open)
  - custom:     nfp.custom_objects + ReduceAtoms + GlobalUpdate/EdgeUpdate/NodeUpdate/ConcatDense
  - featurize:  convert_string_to_dict(smiles); make_data_loader(..., max_size=_bucket_size(max_atoms))
  - scale:      model.get_layer('scale').set_weights([y.std()[None,None], y.mean()[None]])
  - serialize:  json.dumps([np.array(v).tolist() for v in model.get_weights()])

Run in the pinned env ONLY:
  source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream_pin
  python cloudlab/pretrain_surrogate.py
"""
import os, sys, json, ast, time, argparse

# ---------------------------------------------------------------------------
# Paths. The vendored moldesign lives in the StreamML dir; put it on sys.path
# so `from moldesign...` resolves exactly like it does inside the pipeline.
# ---------------------------------------------------------------------------
REPO = "/home/namdo/applications/MoStream"
STREAMML = os.path.join(REPO, "MoStream", "MDStream", "StreamML")
MODEL_H5 = os.path.join(STREAMML, "networks", "model.h5")
OUT_JSON = os.path.join(STREAMML, "networks", "pretrained_weights.json")
DATA = os.path.join(REPO, "MoStream", "WLGenerator-node1", "dataset", "training-data-simple.txt")
sys.path.insert(0, STREAMML)

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")  # hush TF C++ chatter

import numpy as np

_ATOM_BUCKET = 16  # must match NPMMModel._ATOM_BUCKET / Inference._ATOM_BUCKET


def _bucket_size(n):
    """Round a molecule size up to the next multiple of _ATOM_BUCKET (pipeline padding)."""
    return int(((int(n) + _ATOM_BUCKET - 1) // _ATOM_BUCKET) * _ATOM_BUCKET)


def parse_seed_data(path):
    """The seed file is one Python-dict repr per line (single quotes -> NOT json)."""
    smiles, ips = [], []
    bad = 0
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = ast.literal_eval(line)
                smiles.append(d["smiles"])
                ips.append(float(d["ip"]))
            except Exception:
                bad += 1
    return smiles, np.array(ips, dtype=np.float64), bad


def build_model(custom_objects):
    """Build EXACTLY as NPMMModel.TrainFunction.open(): model_from_json(model_config)."""
    import h5py
    import tensorflow as tf
    with h5py.File(MODEL_H5, "r") as f:
        model_config = f.attrs["model_config"]
    if isinstance(model_config, bytes):
        model_config = model_config.decode()
    return tf.keras.models.model_from_json(model_config, custom_objects=custom_objects)


def make_custom_objects():
    import nfp
    from moldesign.score.nfp import ReduceAtoms
    custom_objects = nfp.custom_objects.copy()
    custom_objects["ReduceAtoms"] = ReduceAtoms
    if hasattr(nfp, "GlobalUpdate"):
        custom_objects["GlobalUpdate"] = nfp.GlobalUpdate
    if hasattr(nfp, "EdgeUpdate"):
        custom_objects["EdgeUpdate"] = nfp.EdgeUpdate
    if hasattr(nfp, "NodeUpdate"):
        custom_objects["NodeUpdate"] = nfp.NodeUpdate
    if hasattr(nfp, "ConcatDense"):
        custom_objects["ConcatDense"] = nfp.ConcatDense
    return custom_objects


def predict(model, mol_dicts, max_size, batch_size=128):
    """Inference exactly like evaluate_mpnn: loader with values=None, predict, squeeze."""
    from moldesign.score.nfp import make_data_loader
    loader = make_data_loader(list(mol_dicts), batch_size=batch_size, repeat=False, max_size=max_size)
    return np.asarray(model.predict(loader, verbose=0)).reshape(-1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--epochs", type=int, default=200)
    ap.add_argument("--batch-size", type=int, default=128)
    ap.add_argument("--patience", type=int, default=25, help="early-stopping patience (val MAE)")
    ap.add_argument("--val-frac", type=float, default=0.10)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--lr", type=float, default=1e-3)
    ap.add_argument("--out", default=OUT_JSON)
    args = ap.parse_args()

    import tensorflow as tf
    from tensorflow.python.keras import callbacks as cb
    from moldesign.utils.conversions import convert_string_to_dict
    from moldesign.score.nfp import make_data_loader

    np.random.seed(args.seed)
    tf.random.set_seed(args.seed)

    # --- 1. Load + featurize -------------------------------------------------
    print(f"[data] reading {DATA}")
    smiles, y, bad = parse_seed_data(DATA)
    print(f"[data] parsed {len(smiles)} molecules ({bad} unparseable); "
          f"IP mean={y.mean():.3f} std={y.std():.3f} min={y.min():.3f} max={y.max():.3f}")

    t0 = time.time()
    mol_dicts, keep_y, kept_smiles = [], [], []
    n_fail = 0
    for s, ip in zip(smiles, y):
        try:
            mol_dicts.append(convert_string_to_dict(s))
            keep_y.append(ip)
            kept_smiles.append(s)
        except Exception:
            n_fail += 1
    mol_dicts = np.array(mol_dicts, dtype=object)
    y = np.array(keep_y, dtype=np.float64)
    kept_smiles = np.array(kept_smiles, dtype=object)
    print(f"[data] featurized {len(mol_dicts)} molecules in {time.time()-t0:.1f}s "
          f"({n_fail} featurization failures)")

    # global padding size, bucketed like the pipeline
    raw_max = max(len(x["atom"]) for x in mol_dicts)
    max_size = _bucket_size(raw_max)
    print(f"[data] max atoms (with H) = {raw_max}  ->  padded/bucketed max_size = {max_size}")

    # --- 2. 90/10 split (fixed seed) ----------------------------------------
    rng = np.random.RandomState(args.seed)
    is_train = rng.rand(len(mol_dicts)) > args.val_frac
    train_X, train_y = mol_dicts[is_train], y[is_train]
    val_X, val_y = mol_dicts[~is_train], y[~is_train]
    val_smiles = kept_smiles[~is_train]
    print(f"[split] train={len(train_X)}  val={len(val_X)}  (seed={args.seed})")

    # --- 3. Build + scale-init + compile ------------------------------------
    custom_objects = make_custom_objects()
    model = build_model(custom_objects)
    n_params = model.count_params()
    print(f"[model] built from model.h5 model_config: {len(model.get_weights())} weight tensors, "
          f"{n_params:,} params")

    # scale layer initialised from TRAINING IPs (mean/std), exactly like the pipeline
    try:
        scale_layer = model.get_layer("scale")
        scale_layer.set_weights([train_y.std()[None, None], train_y.mean()[None]])
        print(f"[model] 'scale' layer set from train IPs: std={train_y.std():.4f} mean={train_y.mean():.4f}")
    except ValueError:
        print("[model] no 'scale' layer found (unexpected)")

    model.compile(tf.optimizers.Adam(args.lr), "mean_squared_error",
                  metrics=["mean_absolute_error"], steps_per_execution=1)

    # --- 4. Train to convergence w/ early stopping on val MAE ----------------
    batch = args.batch_size
    steps_per_epoch = max(1, len(train_X) // batch)
    valid_steps = max(1, len(val_X) // batch)
    train_loader = make_data_loader(list(train_X), train_y, repeat=True, batch_size=batch,
                                    max_size=max_size, drop_last_batch=True, shuffle_buffer=32768)
    valid_loader = make_data_loader(list(val_X), val_y, batch_size=batch,
                                    max_size=max_size, drop_last_batch=True)

    early = cb.EarlyStopping(monitor="val_mean_absolute_error", mode="min",
                             patience=args.patience, restore_best_weights=True, verbose=1)
    reduce_lr = cb.ReduceLROnPlateau(monitor="val_mean_absolute_error", mode="min",
                                     factor=0.5, patience=8, min_lr=1e-6, verbose=1)
    callbacks = [early, reduce_lr, cb.TerminateOnNaN()]

    print(f"[train] up to {args.epochs} epochs, batch={batch}, steps/epoch={steps_per_epoch}, "
          f"patience={args.patience}")
    t0 = time.time()
    history = model.fit(train_loader, epochs=args.epochs, shuffle=False, verbose=2,
                        steps_per_epoch=steps_per_epoch, validation_data=valid_loader,
                        validation_steps=valid_steps, validation_freq=1, callbacks=callbacks)
    print(f"[train] done in {time.time()-t0:.1f}s; ran {len(history.history['loss'])} epochs")
    best_val_mae = min(history.history["val_mean_absolute_error"])
    print(f"[train] best val MAE during fit = {best_val_mae:.4f} V")

    # --- 5. Serialize weights EXACTLY like TrainFunction ---------------------
    weights_json = json.dumps([np.array(v).tolist() for v in model.get_weights()])
    with open(args.out, "w") as f:
        f.write(weights_json)
    n_tensors = len(model.get_weights())
    nbytes = os.path.getsize(args.out)
    print(f"[save] wrote {args.out}  ({nbytes:,} bytes, {nbytes/1048576.0:.2f} MB, {n_tensors} tensors)")

    # --- 6. ROUND-TRIP: fresh model <- JSON, predictions must match ----------
    fresh = build_model(custom_objects)
    loaded = [np.array(a) for a in json.loads(open(args.out).read())]
    fresh.set_weights(loaded)
    # NOTE: not compiled -> predict-only, which is all Inference needs
    trained_pred = predict(model, val_X, max_size)
    fresh_pred = predict(fresh, val_X, max_size)
    max_abs_diff = float(np.max(np.abs(trained_pred - fresh_pred)))
    print(f"[roundtrip] max |trained - fresh| over {len(val_X)} val preds = {max_abs_diff:.3e}  "
          f"({'IDENTICAL' if max_abs_diff < 1e-4 else 'MISMATCH'})")

    # --- 7. VALIDATE quality on held-out val --------------------------------
    p = fresh_pred  # use the round-tripped model (proves the pipeline artifact works)
    resid = p - val_y
    mae = float(np.mean(np.abs(resid)))
    rmse = float(np.sqrt(np.mean(resid ** 2)))
    ss_res = float(np.sum(resid ** 2))
    ss_tot = float(np.sum((val_y - val_y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot
    pear = float(np.corrcoef(p, val_y)[0, 1])
    # Spearman rank corr (ranking ability) without scipy
    def spearman(a, b):
        ra = np.argsort(np.argsort(a)); rb = np.argsort(np.argsort(b))
        return float(np.corrcoef(ra, rb)[0, 1])
    spear = spearman(p, val_y)

    # baseline: predict the constant training mean
    const_mae = float(np.mean(np.abs(val_y - train_y.mean())))

    print("\n================= VALIDATION (held-out, round-tripped model) =================")
    print(f"val N={len(val_y)}   MAE={mae:.4f} V   RMSE={rmse:.4f} V   R2={r2:.4f}   "
          f"Pearson r={pear:.4f}   Spearman={spear:.4f}")
    print(f"baseline (predict train-mean {train_y.mean():.3f} V): MAE={const_mae:.4f} V  "
          f"-> model {'BEATS' if mae < const_mae else 'DOES NOT BEAT'} constant by "
          f"{const_mae - mae:+.4f} V")

    # 14 V hit-threshold behaviour
    thr = 14.0
    true_hit = val_y > thr
    pred_hit = p > thr
    correct_side = float(np.mean(true_hit == pred_hit))
    tp = int(np.sum(true_hit & pred_hit)); fp = int(np.sum(~true_hit & pred_hit))
    fn = int(np.sum(true_hit & ~pred_hit)); tn = int(np.sum(~true_hit & ~pred_hit))
    prec = tp / (tp + fp) if (tp + fp) else float("nan")
    rec = tp / (tp + fn) if (tp + fn) else float("nan")
    print(f"\n14V hit threshold: base rate={true_hit.mean():.3f}  correct-side acc={correct_side:.3f}")
    print(f"  confusion  TP={tp} FP={fp} FN={fn} TN={tn}  precision={prec:.3f} recall={rec:.3f}")

    # ranking utility: does top-k by predicted IP concentrate real hits?
    order = np.argsort(-p)  # descending predicted IP
    for k in (10, 25, 50, 100):
        k = min(k, len(p))
        topk_hit_rate = float(np.mean(true_hit[order[:k]]))
        print(f"  top-{k:>3} by predicted IP: real-hit rate={topk_hit_rate:.3f} "
              f"(vs base {true_hit.mean():.3f}, lift {topk_hit_rate/max(true_hit.mean(),1e-9):.2f}x)")

    # 5 example predictions
    print("\n5 example (SMILES, true IP, pred IP):")
    for i in range(min(5, len(val_y))):
        print(f"  {str(val_smiles[i]):<28} true={val_y[i]:7.3f}  pred={p[i]:7.3f}  err={p[i]-val_y[i]:+.3f}")

    # anchor molecule
    anchor = "OCC(F)(F)F"
    a_dict = convert_string_to_dict(anchor)
    a_pred = float(predict(fresh, np.array([a_dict], dtype=object), max_size)[0])
    print(f"\nanchor {anchor}: true=15.9334  pred={a_pred:.4f}  err={a_pred-15.9334397319864:+.4f}")

    print("\n[done]")


if __name__ == "__main__":
    main()
