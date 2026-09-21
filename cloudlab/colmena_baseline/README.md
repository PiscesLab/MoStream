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
./cloudlab/colmena_baseline/setup_upstream.sh    # clone at a pinned commit, build and patch
./cloudlab/colmena_baseline/run_upstream.sh      # 6 h campaign, then writes the curve
```

`run_upstream.sh` stops the campaign at the end of the window and writes
`results/campaign/colmena.csv` itself, measured from launch.

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

### Ensemble size

Upstream defaults to `--model-count 8`, an eight-model ensemble ranked by an upper
confidence bound over its spread. The measured run uses one model, for a reason of
feasibility rather than preference. On a 16-core host each model costs 23 to 42 minutes
to train and about 35 minutes to score the search space, and with one ML worker they
run in sequence, so eight models would not submit a first simulation for 7.6 to 10.3
hours, past a 6-hour window. One model also matches the streaming arm, whose single
model makes the same formula reduce to taking the highest prediction.

### Memory

A full-scale run does not fit on a 15 GB host as upstream wrote it. `patch_upstream.py`
applies three memory-only fixes, each found by a run that had to be stopped before the
kernel killed it, and each verified to leave predictions bit-identical:

| Problem | Fix |
| --- | --- |
| Parsing 1.1M molecule graphs into Python lists needs about 12.7 GB | store them as small integer arrays, about 3.5 GB |
| The ML worker keeps about 190 MB per scoring call and never frees it | clear Keras state at the start of each call |
| TensorFlow spreads allocations over dozens of malloc arenas never trimmed | cap the ML worker at 2 arenas |

`run_upstream.sh` also runs with ProxyStore on, using the backends from upstream's own
`run-xtb-lambda-parsl.sh`. `--no-proxystore` is their ablation, and at this scale it
ships each 50,000-molecule chunk as a message of about 58 MB with twenty in flight.

### Measured result

Six hours on a 16-core host, 3 xTB workers at 4 threads each, one model:

| Hours from launch | Simulated | Above 14 V |
| --- | --- | --- |
| 1 | 0 | 0 |
| 2 | 23 | 19 |
| 3 | 43 | 34 |
| 4 | 62 | 48 |
| 5 | 77 | 57 |
| 6 | 94 | 63 |

The first simulation was submitted 58.2 minutes after launch, spent training and then
scoring all 1.1 million candidates, and the first result landed at 1.01 hours. Only two
ranking refreshes fit in the window, because each one retrains and rescores the whole
space while sharing the CPU with xTB.

Two things any comparison must reproduce exactly. TensorFlow's thread pool was not
capped, so during training and scoring the ML worker used about 9 cores alongside the
12 xTB threads, and xTB averaged 9.45 minutes per molecule under that contention. The
streaming arm has to run on the same host under the same caps, one after the other and
never concurrently, or the curves measure the machine rather than the two designs.

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
