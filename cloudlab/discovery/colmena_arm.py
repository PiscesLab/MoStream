#!/usr/bin/env python3.9
"""STEP 3: the Colmena arm under the discovery figure's offline protocol.

Runs inside the colmena-upstream env so that every model refresh is upstream Colmena's OWN
retrain_mpnn / evaluate_mpnn, with the live campaign's settings.

Policy modelled (the two things that separate Colmena from the streaming `online` arm):

  STARTUP   run.py's __init__ ends with `self.start_training.set()` ("Start with training"),
            so nothing can be dispatched until the first retrain AND the first full scoring
            pass have finished. That measured time, converted to evaluations at 0.024 mol/s,
            is dead time at the head of the curve: the curve stays at 0.

  STALENESS Between refreshes the dispatcher pops from a frozen ranked list (run.py's
            self.task_queue, rebuilt only by _select_molecules). A refresh costs one
            retrain + one full rescore; converted to evaluations at the same rate, that is
            how often the ranking may change.

  RETRAIN   Colmena retrains FROM SCRATCH: run.py sends model.get_config() (no weights) and
            retrain_mpnn rebuilds with Model.from_config, so every refresh is a fresh
            128-epoch fit on the whole accumulated labelled set. This is a genuine design
            difference from the streaming arm's 8-epoch gentle update, and it is kept.

  ACQUISITION  _select_molecules ranks by ucb = y_mean + beta*y_std. The live run used
            --model-count 1, so y_std is identically 0 and UCB is exactly the highest
            prediction: the same acquisition rule as `online`.

Everything else matches the protocol: same pool, same seeds, same prior, BATCH=16,
1024 total evaluations, hit iff xtb_ip >= HIGH_THR.
"""
import os, sys, json, time, math
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
import numpy as np

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

RATE_PER_S = 0.024
SEC_PER_EVAL = 1.0 / RATE_PER_S


def log(*a): print(*a, flush=True)


def main():
    data = json.load(open(os.path.join(SCR, "step3_seed_data.json")))
    tim = json.load(open(os.path.join(SCR, "step2_timing.json")))
    BATCH, ROUNDS, PRIOR_K = data['BATCH'], data['ROUNDS'], data['PRIOR_K']
    HIGH_THR = data['HIGH_THR']
    TOTAL_EVALS = BATCH * ROUNDS
    T_END = TOTAL_EVALS * SEC_PER_EVAL

    # ---- timing model from the Step 2 measurements
    sizes = np.array(sorted(int(k) for k in tim['retrain']), float)
    means = np.array([tim['retrain'][str(int(s))]['mean'] for s in sizes], float)
    T_SCORE = tim['score']['mean']          # upstream rescores the WHOLE search space each refresh

    def t_retrain(n):
        return float(np.interp(n, sizes, means))

    # ---- cost model ---------------------------------------------------------------
    # Both systems really scan the 1,115,320-molecule search space; the 1384-molecule pool is
    # only the evaluation stand-in (those are the molecules with stored xTB labels). So the
    # default prices Colmena's overhead from the live full-scale campaign
    # (runs/xtb-N3-n1-c0b172-21Sep26-134031/runtime.log), not from the pool.
    MODEL = os.environ.get("COST_MODEL", "fullscale")
    if MODEL == "fullscale":
        # launch 06:40:31.237 -> first simulation submitted 07:38:45.349
        T_STARTUP = 3494.1
        # round-2 rescore of the 1.1M space, measured with xTB running:
        # inference submitted 08:26:56.468 -> task list updated 10:15:53.196
        T_REFRESH_CONST = 6536.7
    elif MODEL == "fullscale_full_refresh":
        # as above but charging the WHOLE refresh, retrain + rescore:
        # retraining started 07:41:09.334 -> task list updated 10:15:53.196
        T_STARTUP = 3494.1
        T_REFRESH_CONST = 9283.9
    elif MODEL == "poolscale":
        T_STARTUP = t_retrain(PRIOR_K) + T_SCORE
        T_REFRESH_CONST = None            # size-dependent, measured on the 1384-molecule pool
    else:
        raise SystemExit(f"unknown COST_MODEL {MODEL}")

    def refresh_s(n):
        return T_REFRESH_CONST if T_REFRESH_CONST is not None else t_retrain(n) + T_SCORE

    n_startup = T_STARTUP * RATE_PER_S
    log(f"[cost] model = {MODEL}")
    log(f"[timing] pool-scale retrain sizes {sizes.tolist()} -> means {np.round(means,1).tolist()} s")
    log(f"[timing] pool-scale full-pool rescore = {T_SCORE:.2f} s")
    log(f"[timing] STARTUP = {T_STARTUP:.1f} s x {RATE_PER_S} mol/s "
        f"= {n_startup:.2f} evaluations -> first pick at evaluation {math.ceil(n_startup)}")
    for n in (100, 356, 612, 868, 1124):
        tr = refresh_s(n)
        log(f"[timing] refresh at N={n:>5}: {tr:8.1f} s = {tr*RATE_PER_S:6.2f} evaluations")

    model = tf.keras.models.load_model(MODEL_H5, custom_objects=custom_objects)
    model_config = model.get_config()

    only = os.environ.get("ONLY_SEED")
    seed_list = [only] if only else sorted(data['seeds'], key=int)

    out_curves, out_meta = {}, {}
    for SEED in seed_list:
        sd = data['seeds'][SEED]
        pool_sm = sd['pool_smiles']
        pool_ip = np.array(sd['pool_ip'], float)
        ishigh = pool_ip >= HIGH_THR
        prior_pairs = list(zip(sd['prior_smiles'], sd['prior_ip']))
        pri_pred = np.array(sd['prior_pred'], float)
        npool = len(pool_sm)
        log(f"\n{'#'*78}\nSEED {SEED} | pool={npool} pool_high={int(ishigh.sum())} "
            f"| rounds={ROUNDS} batch={BATCH}")

        pool_dicts = [convert_string_to_dict(s) for s in pool_sm]

        def rank_from_weights(w):
            m = tf.keras.models.Model.from_config(model_config, custom_objects=custom_objects)
            m.set_weights(w)
            y = np.asarray(evaluate_mpnn(NFPMessage(m), pool_dicts, batch_size=128)).reshape(-1)
            return np.argsort(-y)

        searched = np.zeros(npool, dtype=bool)
        labelled = list(prior_pairs)          # (smiles, ip) known to Colmena
        cur_rank = None
        next_rank = np.argsort(-pri_pred)     # the prior's ranking; lands at T_STARTUP
        avail_t = T_STARTUP
        cursor = 0                            # position in cur_rank
        picks = []
        n_trains, n_refresh_events, refresh_log = 0, 0, []
        t_wall = time.time()

        for e in range(TOTAL_EVALS):
            t = e * SEC_PER_EVAL
            if next_rank is not None and t >= avail_t:
                cur_rank, cursor = next_rank, 0
                n_refresh_events += 1
                launch_t = avail_t
                N_at_launch = len(labelled)     # prior + molecules actually acquired so far
                dt = refresh_s(N_at_launch)
                refresh_evals = dt * RATE_PER_S
                # The protocol's finest resolution is one acquisition batch. If Colmena's true
                # refresh is faster than that, the ranking still cannot change mid-batch here,
                # so the cadence floors at BATCH. That overstates Colmena's staleness slightly.
                eff_evals = max(float(BATCH), refresh_evals)
                next_avail = launch_t + eff_evals * SEC_PER_EVAL
                refresh_log.append(dict(event=n_refresh_events, at_eval=e,
                                        N_train=N_at_launch, refresh_s=dt,
                                        refresh_evals=refresh_evals, eff_evals=eff_evals))
                if next_avail < T_END:
                    # Launched now, on everything labelled now; lands next_avail.
                    tf.random.set_seed(int(SEED) * 1000 + n_trains)
                    db = {s: v for s, v in labelled}
                    w, hist = retrain_mpnn(model_config, db, num_epochs=128,
                                           learning_rate=1e-3, timeout=None,
                                           batch_size=32, validation_split=0.1,
                                           random_state=0)
                    next_rank = rank_from_weights(w)
                    n_trains += 1
                    log(f"  [refresh {n_refresh_events:>3}] eval={e:>4} N_train={N_at_launch:>5} "
                        f"epochs={len(hist['loss']):>3} cost={dt:6.1f}s={refresh_evals:5.2f} evals "
                        f"({time.time()-t_wall:.0f}s wall)")
                    # checkpoint: a full seed is hours of compute, do not lose it to a crash
                    json.dump(dict(seed=SEED, picks=picks, refresh_log=refresh_log),
                              open(os.path.join(SCR, f"step3_ckpt_{MODEL}_seed{SEED}.json"), "w"))
                else:
                    next_rank = None
                avail_t = next_avail

            if cur_rank is None:
                picks.append(-1)              # startup dead time: nothing dispatched
                continue
            while cursor < npool and searched[cur_rank[cursor]]:
                cursor += 1
            if cursor >= npool:
                picks.append(-1); continue
            j = int(cur_rank[cursor]); cursor += 1
            searched[j] = True
            picks.append(j)
            labelled.append((pool_sm[j], float(pool_ip[j])))

        cum = [0]; h = 0
        for r in range(ROUNDS):
            for j in picks[r*BATCH:(r+1)*BATCH]:
                if j >= 0 and ishigh[j]:
                    h += 1
            cum.append(h)
        n_idle = sum(1 for j in picks if j < 0)
        log(f"[colmena seed={SEED}] FINAL={cum[-1]} | idle startup slots={n_idle} "
            f"| refresh events={n_refresh_events} | retrains run={n_trains} "
            f"| {time.time()-t_wall:.0f}s wall")
        out_curves[SEED] = cum
        out_meta[SEED] = dict(final=cum[-1], idle_evals=n_idle,
                              refresh_events=n_refresh_events, retrains=n_trains,
                              refresh_log=refresh_log)

    res = dict(curves=out_curves, meta=out_meta, cost_model=MODEL,
               timing=dict(T_STARTUP=T_STARTUP, n_startup_evals=n_startup,
                           T_REFRESH_CONST=T_REFRESH_CONST,
                           T_SCORE=T_SCORE, retrain_sizes=sizes.tolist(),
                           retrain_means=means.tolist(), RATE_PER_S=RATE_PER_S,
                           BATCH=BATCH, ROUNDS=ROUNDS))
    tag = f"_seed{only}" if only else ""
    fn = os.path.join(SCR, f"step3_colmena_arm_{MODEL}{tag}.json")
    json.dump(res, open(fn, "w"), indent=1)
    log(f"\n[saved] {fn}")
    log("DONE-STEP3")


if __name__ == "__main__":
    main()
