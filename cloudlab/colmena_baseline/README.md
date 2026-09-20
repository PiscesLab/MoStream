# Colmena baseline

The task-based arm the streaming pipeline is compared against. Colmena runs the same
molecular discovery loop as discrete tasks and refreshes the ranking between scheduling
rounds, which is the design the paper argues against.

## What is held fixed

A comparison is only worth reporting if one thing differs. These are shared, and the
task functions import the streaming arm's own code rather than reimplementing it:

| Held fixed | Where it comes from |
| --- | --- |
| Oracle | `SimulateFromSmiles`, imported from `WLGenerator-node1/simulator.py` |
| Surrogate architecture | `model.h5`, loaded through `NPMMModel._default_model_path` |
| Candidate set | the same search space file, same 500-molecule chunks |
| Training window, minibatch, learning rate | constants at the top of `tasks.py`, copied from `NPMMModel` |
| Hit threshold | 14 V, the same rule `plot_discovery.py` applies |
| Worker budget | `--workers`, set to the number of simulation nodes |
| Total simulations | `--budget`, the shared compute budget |

## What differs, which is the point

The streaming arm updates the surrogate on every completed result and can act on it
immediately. This arm runs rounds:

1. dispatch the top `--round-size` candidates from the current ranked list
2. wait for the whole batch to return
3. retrain on the widened window
4. rescore a chunk and re-rank

A result that arrives early in step 2 cannot change what is simulated until step 4.
`steering_latency.csv` records that wait for every result, which is the same quantity
the streaming arm reports as steering latency.

## Environment

Colmena pulls `cloudpickle>=3`, and the PyFlink arm needs `cloudpickle~=2.2.1` through
apache-beam. Installing both in one environment breaks PyFlink, so keep them apart:

```bash
conda create -n colmena-base python=3.9
conda activate colmena-base
pip install colmena
pip install -r requirements.txt     # the chemistry and model stack
```

## Running

Smoke test, a few simulations locally:

```bash
python cloudlab/colmena_baseline/run_baseline.py --budget 8 --round-size 4 --workers 2
```

The measured run, matching three simulation nodes:

```bash
python cloudlab/colmena_baseline/run_baseline.py \
    --budget 512 --round-size 16 --workers 3 --out results/colmena
```

`--budget` must match the number of oracle calls the streaming arm was given, or the
compute budgets are not equal and the curves cannot be compared.

## Outputs

| File | Contents |
| --- | --- |
| `oracle_colmena.log` | one line per verified molecule, the same format the simulators write, so the existing parser reads it |
| `steering_latency.csv` | per result, how long it waited before it could influence a dispatch |
| `summary.json` | configuration, simulations completed, rounds, hits, wall clock |

Then build the discovery curve:

```bash
python cloudlab/colmena_baseline/make_curve.py \
    --log results/colmena/oracle_colmena.log \
    --out results/campaign/colmena.csv
python cloudlab/plot_discovery.py
```

`plot_discovery.py` overlays `colmena.csv` when it exists, so the figure gains the arm
without any other change.

## Random arm

The no-model floor uses the same budget and oracle, drawing uniformly from the search
space. `cloudlab/random_baseline.py` produces it, and `make_curve.py` converts its log
the same way into `results/campaign/random.csv`.
