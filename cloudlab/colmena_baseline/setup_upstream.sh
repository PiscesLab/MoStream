#!/usr/bin/env bash
# Fetch and set up the real Colmena molecular-design application.
#
# The baseline is Colmena's own code, not a reimplementation, because the point of
# the comparison is what their system does rather than what we think it does. This
# clones the artifact the paper cites at a pinned commit and builds the environment
# it was written against.
#
#   ./cloudlab/colmena_baseline/setup_upstream.sh
#
# Nothing is vendored into this repository. The upstream repo publishes no license,
# so it is fetched at run time and left where it lands.
set -euo pipefail

UPSTREAM_DIR="${UPSTREAM_DIR:-$HOME/colmena-upstream}"
# exalearn/multi-site-campaigns, the artifact behind Ward et al.
PINNED_COMMIT="${PINNED_COMMIT:-59b145636767cc6c7c6c7ff8715d2a427142dad8}"
ENV_NAME="${ENV_NAME:-colmena-upstream}"

echo "==> cloning multi-site-campaigns into $UPSTREAM_DIR"
if [[ ! -d "$UPSTREAM_DIR/.git" ]]; then
  git clone https://github.com/exalearn/multi-site-campaigns.git "$UPSTREAM_DIR"
fi
git -C "$UPSTREAM_DIR" fetch --depth 50 origin
git -C "$UPSTREAM_DIR" checkout -q "$PINNED_COMMIT"
echo "    at $(git -C "$UPSTREAM_DIR" log -1 --format='%h %ad %s' --date=short)"

echo "==> creating conda env '$ENV_NAME'"
# A reduced version of their environment.yml: the molecular-design app only. Their
# full file also pulls psi4 and torch for the surrogate-finetuning app, and
# funcx_endpoint, which --use-parsl makes unnecessary.
conda create -y -n "$ENV_NAME" -c conda-forge python=3.9 \
  redis-server rdkit=2022.09.4 qcengine=0.23.0 geometric=0.9 xtb-python \
  'pyyaml<6' 'py-cpuinfo<6' 'msgpack-python=1' psutil tqdm 'pandas==1.*'

# shellcheck disable=SC1091
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$ENV_NAME"

# Pins found by building this environment, none of which their environment.yml
# states. qcengine 0.23 and the vendored moldesign models are written for pydantic
# v1, and conda-forge now resolves pydantic v2, which fails at import with
# "AttributeError: __pydantic_private__". qcelemental has to move back with it.
conda install -y -c conda-forge 'pydantic<2' 'qcelemental<0.26'

# colmena 0.4 brings proxystore 0.4, the last release with the store module layout
# run.py imports at module level (proxystore.store.redis and friends).
pip install 'colmena==0.4.*' 'parsl==2023.2.27' 'tensorflow-cpu==2.8.*' \
            'ase==3.22.1' python-git-info flatten-dict 'redis<5' 'protobuf<3.20'
pip install 'git+https://github.com/exalearn/nfp.git@gc_updates'
pip install -e "$UPSTREAM_DIR/molecular-design"

echo "==> patching upstream to run off ALCF"
# Their --use-parsl path targets the decommissioned Theta machine. The patch adds a
# local executor config with the same labels and nothing else; see patch_upstream.py.
python "$(dirname "${BASH_SOURCE[0]}")/patch_upstream.py" "$UPSTREAM_DIR/molecular-design"

echo "==> sanity check"
python -c "import qcengine; assert 'xtb' in qcengine.list_available_programs(), 'xtb not visible to qcengine'; print('    xtb visible to qcengine')"
( cd "$UPSTREAM_DIR/molecular-design" && python run.py --help >/dev/null ) && echo "    run.py loads"

echo
echo "==> done"
echo "    upstream:  $UPSTREAM_DIR"
echo "    env:       $ENV_NAME"
echo "    next:      ./cloudlab/colmena_baseline/run_upstream.sh"
