# MoStream

MoStream is a streaming active-learning pipeline for molecular discovery. A simulator emits
molecules (SMILES plus a simulated ionization potential) to Kafka; a PyFlink job trains a
message-passing neural network on the stream, scores a candidate search space, ranks candidates by
an upper-confidence-bound score, and feeds the top recommendations back to the simulator, closing
the active-learning loop. It runs locally for testing and distributed on a standalone Flink 2.0
cluster with native Kafka.

## Repository layout

```
MoStream/MDStream/StreamML/     the Flink pipeline: MDWorkflow.py + operator UDFs
                                (NPMMModel.py = Train, Inference.py = Infer, Ranking.py = Rank)
MoStream/WLGenerator-node1/     the closed-loop simulator (polls Recommend, runs the oracle)
cloudlab/                       cluster deploy (submit_job.sh, setup + restart scripts) and
                                figure-plotting scripts (plot_*.py, make_figures.py)
scripts/                        node setup scripts (Kafka, JobManager)
requirements.txt                hard-pinned runtime dependencies (see note below)
requirements-dev.txt            looser local dev set
```

## Environment

Use a dedicated Python 3.9 environment. Runtime dependencies are **hard-pinned** in
`requirements.txt` (`nfp==0.1.3`, `h5py==3.1.0`, `pandas<2`, `numpy<2`, `tensorflow==2.14.0`,
`apache-flink==2.0.0`): the saved model was written with `nfp` 0.1.x layers and a non-standard HDF5
float type, and `pandas>=2` breaks the apache-beam used internally by PyFlink on Python 3.9. Do not
bump these without regenerating the model.

```bash
conda create -n mostream python=3.9
conda activate mostream
pip install -r requirements.txt
```

## Data and model files

The trained model and the seed training table ship with the repository:

| File | Size | In repo |
| --- | --- | --- |
| `MDStream/StreamML/networks/model.h5` | 2.8 MB | yes |
| `*/dataset/training-data-simple.txt` | 600 KB | yes |

Three larger inputs are distributed separately, because the candidate search space alone
holds about 1.1 million molecules and exceeds the GitHub per-file size limit:

- `MDStream/StreamML/search_space/MOS-search-simple.txt` (193 MB)
- `MDStream/WLGenerator/search_space/MOS-search.csv`
- `MDStream/WLGenerator/dataset/training-data.json`

**Download:** [Google Drive](https://drive.google.com/drive/folders/1HLcg6sIDlEDwt4GKN6UcShYovxWzcGyq?usp=sharing)

The folder is access controlled. Use the *Request access* button on the Drive page and the
maintainers will approve it.

The Drive layout mirrors this repository, so copy the contents of `StreamData/MDStream/` into
`MoStream/MDStream/` and the files land where the operators expect them. To keep the search
space elsewhere, point `KAFKA_SEARCH_SPACE_PATH` at it instead. Every TaskManager needs its own
local copy, because `Infer` reads a chunk directly from disk rather than over the network.

## Quick local test (no cluster)

Run the pipeline logic with an in-memory source, no Kafka required:

```bash
python MoStream/MDStream/StreamML/MDWorkflow.py --local
```

To exercise the full path locally, start a single-node Kafka (a `docker-compose.yml` is provided)
and create the two topics:

```bash
docker compose up -d
docker compose exec kafka kafka-topics --create --topic Simulation --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
docker compose exec kafka kafka-topics --create --topic Recommend   --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

Run the job against Kafka (use `earliest` and a fresh group id to replay existing messages):

```bash
python MoStream/MDStream/StreamML/MDWorkflow.py --kafka-bootstrap localhost:9092 --starting-offset earliest --group-id debug-1
```

Run the closed-loop simulator (this is the one that closes the loop by polling `Recommend`):

```bash
python MoStream/WLGenerator-node1/simulator.py --topic Simulation --interval 1.0
# on a cluster the bootstrap comes from the KAFKA_BOOTSTRAP environment variable
```

Two topics are used: `Simulation` (simulator to Flink) and `Recommend` (Flink to simulator).

## Cluster deployment

1. Copy `cloudlab/cluster.conf.template` to `cloudlab/cluster.conf` and fill in the node hosts
   (`KAFKA_HOST`, `JM_HOST`, `TM_HOST`, `SIMULATOR_HOST`).
2. Bring up Kafka, the Flink JobManager, and a TaskManager using the setup scripts in `scripts/`
   and `cloudlab/`.
3. Submit the job from the JobManager:

```bash
bash cloudlab/submit_job.sh        # PARALLELISM, STARTING_OFFSET, FLINK_HOME are env-overridable
```

`submit_job.sh` sources `cluster.conf` and runs `flink run -py MDWorkflow.py`.

## Reproducing the figures

The plotting scripts in `cloudlab/` regenerate each figure from measured data under `results/`
(produced by the experiments; not included in the repository). Run each from the repository root in
the `mostream` environment:

| Figure | Script | Input data |
|--------|--------|-----------|
| Throughput vs parallelism | `plot_scaling_tf.py` | per-rep values embedded in the script |
| Operator utilization | `make_figures.py` (`fig_utilisation`) | `results/e3/metrics_p*.csv` |
| Steering-latency CDF | `make_figures.py` (`fig_latency`) | `results/e0/e0_tuned.csv` |
| Latency vs load | `plot_latency_model.py` | `results/latsweep*/summary.json` |
| Worker memory footprint | `plot_footprint_combined.py` | `results/e1/tm_memory.log`, `results/e7_long.csv` |
| Bounded operator state | `plot_systems_panels.py` (`checkpoint`) | measured rate constants in the script |
| Fault recovery | `plot_systems_panels.py` (`recovery`) | `results/e4trace/trace.json` |
| End-to-end discovery | `plot_discovery.py` | `results/campaign/oracle_*.log` |

## Notes

- Kafka mode showing no output is almost always a consumer offset/group issue: use
  `--starting-offset earliest` and a fresh `--group-id`.
- All pipeline randomness is seeded (`random_state = 1`) for repeatable train/valid splits.
