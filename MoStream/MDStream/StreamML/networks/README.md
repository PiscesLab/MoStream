# Surrogate model files

The pipeline needs one file here: `model.h5`. `NPMMModel.py` reads only the
architecture from it, through `model_config`, and then trains the weights online from
the simulation stream. Point `MOSTREAM_MODEL_PATH` elsewhere to override the location.

| File | Size | Role |
| --- | --- | --- |
| `model.h5` | 2.8 MB | Architecture the pipeline loads at operator startup. This is the only one the running job needs. |
| `model-1687718027256.h5` | 8.2 MB | Earlier trained checkpoint, kept for reference. Timestamp is the training run's epoch in milliseconds. |
| `model-local.h5` | 8.2 MB | Variant used by the local, no cluster path in `NPMMModel_local.py`. |

## How these were produced

The architecture is a message-passing neural network built with
[nfp](https://github.com/NREL/nfp), following the hyperparameters of the Colmena
electrolyte design application. It predicts ionization potential from a SMILES string.
The reference labels come from xTB through QCEngine, the same pipeline that produced
the seed table in `WLGenerator/dataset/`.

Weights in the timestamped files are from offline pretraining on that seed table. The
experiments in the paper do not depend on them: the runs reported there start from the
architecture alone and learn online, which is what `MOSTREAM_SKIP_PRETRAINED=1`, the
default, selects.

## Reproducing

`cloudlab/pretrain_surrogate.py` rebuilds a pretrained checkpoint from the seed table.
The architecture itself comes from the upstream application described in the
Attribution section of the repository README.
