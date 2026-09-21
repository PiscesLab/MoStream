#!/usr/bin/env python3
"""Make upstream Colmena runnable off ALCF, without touching its steering logic.

Upstream's --use-parsl path builds `theta_debug_and_venti`: simulations on ALCF's
Theta through the Cobalt scheduler, ML tasks on an Argonne GPU host over SSH. Theta
was decommissioned in 2023, so that configuration cannot run anywhere. This applies
the smallest change that fixes it, and nothing else:

  1. config.py gains `local_config`, two HighThroughputExecutors on this machine with
     the same labels upstream uses, 'cpu' for simulations and 'gpu' for ML. run.py
     routes tasks by those labels, so routing is unchanged.
  2. run.py imports `local_config` instead of `theta_debug_and_venti`.
  3. run_simulation's thread count reads COLMENA_XTB_CORES, defaulting to upstream's
     own 64, so the value is unchanged unless you set it.

The Thinker, its agents, the acquisition function, the retraining schedule and the
chemistry are all untouched. Idempotent: running it twice changes nothing.

    python patch_upstream.py ~/colmena-upstream/molecular-design
"""
import re
import sys
from pathlib import Path

LOCAL_CONFIG = '''

def local_config(log_dir: str) -> Config:
    """Run everything on this machine. Added so the campaign runs off ALCF.

    Keeps upstream's executor labels, 'cpu' for simulations and 'gpu' for the ML
    tasks, which is how run.py routes work. Worker counts come from the environment:
      COLMENA_QC_WORKERS  concurrent simulations, match the streaming arm's node count
      COLMENA_ML_WORKERS  concurrent ML tasks
    """
    import os
    from parsl.providers import LocalProvider
    qc_workers = int(os.environ.get('COLMENA_QC_WORKERS', '3'))
    ml_workers = int(os.environ.get('COLMENA_ML_WORKERS', '1'))
    return Config(
        run_dir=log_dir,
        retries=1,
        executors=[
            HighThroughputExecutor(label='cpu', max_workers=qc_workers,
                                   address='127.0.0.1',
                                   provider=LocalProvider(init_blocks=1, max_blocks=1)),
            HighThroughputExecutor(label='gpu', max_workers=ml_workers,
                                   address='127.0.0.1',
                                   provider=LocalProvider(init_blocks=1, max_blocks=1)),
        ],
    )
'''


def main(app_dir: str) -> None:
    app = Path(app_dir)
    cfg, run = app / 'config.py', app / 'run.py'

    c = cfg.read_text()
    if 'def local_config' not in c:
        cfg.write_text(c.rstrip() + '\n' + LOCAL_CONFIG)
        print('  config.py: added local_config')
    else:
        print('  config.py: local_config already present')

    r = run.read_text()
    old_import = 'from config import theta_debug_and_venti as make_config'
    if old_import in r:
        r = r.replace(old_import, 'from config import local_config as make_config')
        print('  run.py: now imports local_config')

    old_cfg = "compute_config = {'nnodes': n_nodes, 'cores_per_rank': 2, 'ncores': 64}"
    new_cfg = ("compute_config = {'nnodes': n_nodes, 'cores_per_rank': 2, "
               "'ncores': int(__import__('os').environ.get('COLMENA_XTB_CORES', '64'))}")
    if old_cfg in r:
        r = r.replace(old_cfg, new_cfg)
        print('  run.py: xTB thread count now reads COLMENA_XTB_CORES, default 64')
    run.write_text(r)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    main(sys.argv[1])
