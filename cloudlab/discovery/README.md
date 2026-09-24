# Discovery experiment

Produces the paper's discovery figure: cumulative high-IP molecules found against
elapsed time, for four selection strategies.

This is a replay, not a live campaign. Every molecule in the pool already has a real
xTB value measured on the cluster, so the experiment looks answers up instead of
running chemistry. That is what lets 11.85 simulated hours finish in minutes rather
than the roughly 54 hours a live run of the same budget would take.

## The pool

`results/discovery/insearch_labels.csv` holds 2,046 molecules parsed from cluster
oracle logs, each with a measured xTB ionization potential. They reduce to the search
space the experiment uses:

| | count |
| --- | --- |
| molecules with a measured value | 2,046 |
| minus surrogate-selected, which would pre-enrich the pool | −232 |
| minus molecules above the featurizer's atom cap | −180 |
| minus the set that trains the starting model | −100 |
| minus the validation set for early stopping | −150 |
| **candidates all arms search** | **1,384** |

Dropping the surrogate-selected molecules matters: they were chosen by an earlier
model, so leaving them in would measure that model rather than the ones under test.
The remaining pool is about 10 percent high-IP, which is the natural rate.

The model never sees an answer it has not earned. Each round it ranks by prediction
from structures alone, and only the molecules it picks have their value revealed, to
score them and to train on.

## The arms

| arm | policy |
| --- | --- |
| `online` | re-rank and retrain every round of 16 |
| `colmena` | Colmena's policy: act only after training and scoring, then re-rank on its measured cadence |
| `static` | train once, then rank with a frozen model |
| `random` | uniform selection, no model |

## Running

```bash
conda activate mostream
python cloudlab/discovery/insearch_discovery.py     # online, static, random
python cloudlab/discovery/colmena_timing.py         # measure Colmena's retrain and rescore cost
python cloudlab/discovery/colmena_arm.py            # the Colmena arm
python cloudlab/plot_discovery_online.py            # the figure
```

Paths resolve relative to the repository. Override with `MOSTREAM_REPO`,
`MOSTREAM_DISCOVERY_DIR`, or `UPSTREAM_DIR` for the Colmena checkout.

## The Colmena arm's cost model

Colmena is charged what scanning the real search space costs, not what scanning the
pool costs, because 1.1 million candidates is what it really scans. Both figures come
from the live campaign in `cloudlab/colmena_baseline/`:

- startup 3,494 s, so its first pick lands at evaluation 84 of 1,024
- a refresh costs 9,284 s of retraining and rescoring, about 223 evaluations

Pricing it on the 1,384-molecule pool instead would cost 3 seconds, which would let it
re-rank more often than the streaming arm and invert the comparison. That pool-scale
variant is available through `COST_MODEL=poolscale` and is reported for transparency.

## What this figure can and cannot show

It isolates one thing: how often a strategy can act on what it has learned, under an
equal evaluation budget. It is not a systems measurement. No Flink operators, no
Colmena task server, no Kafka, no chemistry.

Two consequences of the pool being 800 times smaller than the real search space. All
arms converge near the end, because 1,024 evaluations consume 74 percent of it, so
that convergence is exhaustion rather than a result. And `random` reaches 73 percent
for the same reason; at real scale it finds almost nothing. Enrichment, hits per
evaluation over the base rate, is the artifact-free way to read the figure: early on
the streaming arm reaches 5.3 against Colmena's 1.9 and random's 0.8.

The live comparison, where the real software runs against all 1,115,320 candidates
with real xTB, lives in `cloudlab/colmena_baseline/`.
