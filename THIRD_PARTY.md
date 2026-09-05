# Third-party code

This repository is released under the MIT License (see `LICENSE`), with one
exception described below.

## Vendored: ExaLearn multi-site-campaigns

The `moldesign` package vendored under `MoStream/*/moldesign/` was copied from
[exalearn/multi-site-campaigns](https://github.com/exalearn/multi-site-campaigns).

That upstream repository publishes no license. Code without a license carries no
grant of rights, so these files are **not** covered by the MIT License above and
may not be redistributable. Anyone intending to reuse or redistribute this
repository should contact the upstream authors first.

Modification made here: the Parsl configuration module (`moldesign/config.py`) was
removed from each copy. It targeted the ALCF Theta scheduler, was never imported by
this project, and carried a site-specific conda environment path.

## Application origin

The active-learning search for organic electrolytes with high ionization potential
comes from Colmena, which is Apache 2.0 licensed:

- L. Ward et al., "Colmena: Scalable machine-learning-based steering of ensemble
  simulations for high performance computing", MLHPC 2021.
- L. Ward et al., "Employing Artificial Intelligence to Steer Exascale Workflows with
  Colmena", IJHPCA 39(1), 2025.

## Runtime dependencies

Apache 2.0: Apache Flink, TensorFlow, Kafka, Parsl.
BSD 3-Clause: RDKit, QCEngine, geomeTRIC.
