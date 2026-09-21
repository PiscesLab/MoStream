# Colmena baseline

The task-based arm the streaming pipeline is compared against.

There are two ways to run it. The first runs Colmena's own application unmodified and
is the one worth reporting. The second is a small harness in this directory, kept as a
fallback because the upstream environment is from 2023 and may not build everywhere.

## 1. Upstream Colmena, the arm to report

Colmena's molecular-design application is the artifact behind Ward et al., and it
targets this exact task: the same search space, the same MPNN surrogate, the same xTB
oracle. Running it directly removes any argument about whether our version of their
design is faithful.

```bash
./cloudlab/colmena_baseline/setup_upstream.sh    # clone at a pinned commit, build its env
./cloudlab/colmena_baseline/run_upstream.sh      # run the campaign against our data
```

`setup_upstream.sh` clones `exalearn/multi-site-campaigns` at commit `59b1456` into
`~/colmena-upstream` and builds a conda environment from their `environment.yml`,
reduced to the molecular-design application. Their full file also pulls psi4 and torch
for a different app in that repo, and funcx, which `--use-parsl` makes unnecessary.

Nothing is vendored. The upstream repository publishes no license, so it is fetched at
run time rather than copied into this one.

### Inputs

`run_upstream.sh` points their `run.py` at three files, all shared with the streaming
arm. Override any of them with the matching environment variable.

| Input | Default |
| --- | --- |
| `SEARCH_SPACE` | `MoStream/MDStream/WLGenerator/search_space/MOS-search.csv` |
| `TRAINING_SET` | `MoStream/MDStream/WLGenerator/dataset/training-data.json` |
| `MPNN_MODEL` | `MoStream/MDStream/StreamML/networks/model.h5` |

The first two are the gitignored data files from the download linked in the repository
README. They are the same files upstream's own launch script uses.

### One decision you have to make

Upstream defaults to `--model-count 8`, an eight-model ensemble, and ranks by an upper
confidence bound over its spread. The streaming arm runs a single model, so its
standard deviation is zero and the same formula reduces to picking the highest
prediction.

That is a real difference in the acquisition policy, not just in scheduling, and it
cuts both ways:

- `MODEL_COUNT=8` is Colmena as published. The comparison then covers the whole design,
  and the baseline gets a better acquisition function than ours.
- `MODEL_COUNT=1` isolates the scheduling difference alone, which is the paper's actual
  claim, at the cost of not being the configuration their paper reports.

Running both is the strongest answer, and it is cheap relative to the oracle time.
Whichever you report, say which one it was.

## 2. Local harness, the fallback

`tasks.py`, `thinker.py` and `run_baseline.py` implement the same round-based policy
against the current Colmena release. The task functions import the streaming arm's own
code, so the oracle, the surrogate and the chunk reader are literally shared:

| Held fixed | Where it comes from |
| --- | --- |
| Oracle | `SimulateFromSmiles`, from `WLGenerator-node1/simulator.py` |
| Surrogate architecture | `model.h5`, via `NPMMModel._default_model_path` |
| Candidate set | the same search space file, same 500-molecule chunks |
| Window, minibatch, learning rate | constants in `tasks.py`, copied from `NPMMModel` |
| Hit threshold | 14 V, the rule `plot_discovery.py` applies |

A round dispatches the top of a frozen ranked list, waits for the batch, retrains, then
re-ranks. A result arriving early in a round cannot change what is simulated until the
round closes, and `steering_latency.csv` records that wait per result.

```bash
# scheduling and output format only, no chemistry, runs anywhere
python cloudlab/colmena_baseline/run_baseline.py --dry-run --budget 24 --round-size 6

# the real thing, on a simulation node
python cloudlab/colmena_baseline/run_baseline.py \
    --budget 512 --round-size 16 --workers 3 --out results/colmena
```

Report this arm as our implementation of a batch policy, not as Colmena.

## Building the figure

Either path converts to the curve `plot_discovery.py` overlays:

```bash
# upstream
python cloudlab/colmena_baseline/make_curve.py \
    --upstream ~/colmena-upstream/molecular-design/runs/<timestamp> \
    --out results/campaign/colmena.csv

# local harness
python cloudlab/colmena_baseline/make_curve.py \
    --log results/colmena/oracle_colmena.log \
    --out results/campaign/colmena.csv

python cloudlab/plot_discovery.py
```

## Environments

Three environments, kept apart on purpose:

| Env | For | Why separate |
| --- | --- | --- |
| `mostream` | the streaming arm | apache-beam inside PyFlink pins `cloudpickle~=2.2.1` |
| `colmena-base` | the local harness | current colmena pulls `cloudpickle>=3`, which breaks PyFlink |
| `colmena-upstream` | upstream Colmena | 2023 pins: `colmena==0.4.*`, `tensorflow-cpu==2.8`, `qcengine==0.23.0` |

Installing colmena into `mostream` will break PyFlink. It has happened once already.

## Budget

Whichever arm you run, the number of oracle calls must match what the streaming arm was
given. Different budgets make the curves incomparable, and the budget is the control
the review specifically asked about.
