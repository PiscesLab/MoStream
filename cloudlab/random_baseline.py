#!/usr/bin/env python3
"""Random-selection baseline: the base-rate arm for the discovery/enrichment figure.

This is the control MoStream is measured against. It draws molecules UNIFORMLY AT RANDOM from
the same 1.1M search space the closed loop recommends from, scores each with the SAME xTB oracle
(`SimulateFromSmiles`), and logs one `[oracle]` line per molecule in the EXACT format the
simulator uses -- so `plot_discovery.py` / `plot_enrichment.py` parse both arms with one regex.
The fraction of these random molecules whose true IP exceeds 14 V is the base rate; MoStream's
hit rate divided by it is the enrichment factor.

WHERE THIS RUNS. On a simulator node ONLY (apt013/015/001): it imports `SimulateFromSmiles` from
`MoStream/WLGenerator-node1/simulator.py`, which needs the `xtb` binary, `xtb-python`, and the
vendored `moldesign` package that lives next to that simulator. None of that is on a dev laptop,
so the oracle path CANNOT run locally -- use `--dry-run` there to exercise sampling/IO/format.

The oracle is the pacer: an xTB call is ~14 s (6 atoms) to ~126 s (18 atoms). There is no
artificial sleep. A few thousand molecules is a multi-hour campaign, matching the MoStream run
it is compared against.

RUN ON A SIM NODE (inside the sim-node conda env that has xtb + the oracle stack):
    source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream
    cd ~/MoStream/MoStream/WLGenerator-node1        # so vendored moldesign imports
    python ~/MoStream/cloudlab/random_baseline.py --n 600 \
        --search-space /mnt/media/MDStream/StreamML/search_space/MOS-search-simple.txt \
        --out ~/MoStream/results/campaign/random/oracle_random.log --seed 0
    # (the script also chdir's into the simulator dir itself, so an explicit cd is belt-and-braces)

LOGIC-CHECK ANYWHERE (no xtb needed) -- confirms sampling + parsing + the emitted line format:
    python cloudlab/random_baseline.py --n 20 --dry-run \
        --search-space MoStream/MDStream/StreamML/search_space/MOS-search-simple.txt \
        --out /tmp/oracle_random_dryrun.log
"""
import argparse
import ast
import os
import random
import sys
import time

# Molecules whose true (oracle) ionization potential exceeds this are "hits". Same threshold the
# discovery/enrichment plotters use; kept here only for the dry-run's synthetic IP spread.
HIT = 14.0


def reservoir_sample(path, n, seed):
    """Return `n` SMILES drawn uniformly at random from the search space (Algorithm R).

    The search space is one Python-dict repr per line -- `{'smiles': '...', 'inchi': '...'}` --
    with NO ip field (verified: 0/1,115,320 lines carry one), which is exactly why every scored
    molecule needs a real oracle call. We stream the file once (it is ~200 MB) and keep a uniform
    reservoir of raw lines, then parse only those `n`. Seeded, so a given seed reproduces the run.
    """
    rng = random.Random(seed)
    reservoir = []
    with open(path) as f:
        for i, line in enumerate(f):
            if i < n:
                reservoir.append(line)
            else:
                # Algorithm R: replace a random slot with prob n/(i+1) -> uniform over all lines.
                j = rng.randint(0, i)
                if j < n:
                    reservoir[j] = line

    smiles = []
    for line in reservoir:
        try:
            rec = ast.literal_eval(line.strip())
            s = rec.get('smiles') if isinstance(rec, dict) else None
        except (ValueError, SyntaxError):
            s = None
        if s:
            smiles.append(s)
    if len(smiles) < len(reservoir):
        print(f"[random] warning: {len(reservoir) - len(smiles)} sampled line(s) did not parse "
              f"as a smiles dict; scoring {len(smiles)}", file=sys.stderr, flush=True)
    return smiles


def load_oracle(sim_dir):
    """chdir into the simulator dir (for vendored moldesign) and import the real xTB oracle."""
    sim_dir = os.path.abspath(sim_dir)
    if not os.path.isfile(os.path.join(sim_dir, 'simulator.py')):
        sys.exit(f"[random] no simulator.py under {sim_dir} (need --sim-dir <WLGenerator-node1>)")
    os.chdir(sim_dir)               # vendored moldesign resolves against cwd/sys.path[0]
    sys.path.insert(0, sim_dir)
    import simulator                 # runs its top-level imports (moldesign, rdkit, kafka)
    return simulator.SimulateFromSmiles


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--n', type=int, default=600, help='molecules to draw and score')
    ap.add_argument('--search-space',
                    default='/mnt/media/MDStream/StreamML/search_space/MOS-search-simple.txt',
                    help='one dict-per-line {smiles,inchi} search space')
    ap.add_argument('--out', default='results/campaign/random/oracle_random.log',
                    help='oracle log to append to (created if absent)')
    ap.add_argument('--seed', type=int, default=0, help='RNG seed for reproducible sampling')
    ap.add_argument('--sim-dir', default=None,
                    help='WLGenerator-node1 dir holding simulator.py + vendored moldesign '
                         '(default: inferred from this script location)')
    ap.add_argument('--dry-run', action='store_true',
                    help='skip xtb; emit placeholder lines to check sampling/IO/format locally')
    args = ap.parse_args()

    # Resolve output to an absolute path BEFORE any chdir into the simulator directory.
    out_path = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if args.sim_dir is None:
        here = os.path.dirname(os.path.abspath(__file__))
        args.sim_dir = os.path.join(here, '..', 'MoStream', 'WLGenerator-node1')

    print(f"[random] sampling {args.n} molecules (seed={args.seed}) from {args.search_space}",
          flush=True)
    smiles = reservoir_sample(args.search_space, args.n, args.seed)
    print(f"[random] got {len(smiles)} molecules; writing to {out_path} "
          f"(dry_run={args.dry_run})", flush=True)

    oracle = None
    if not args.dry_run:
        oracle = load_oracle(args.sim_dir)

    # A separate RNG stream for the dry-run's synthetic IPs, so placeholder logs look plausible
    # (and are themselves parseable by the plotters) without touching the sampling RNG.
    synth = random.Random(args.seed + 1)

    n_ok = n_fail = n_hit = 0
    with open(out_path, 'a', buffering=1) as log:   # line-buffered: one flushed line per molecule
        for smi in smiles:
            t0 = time.time()
            if args.dry_run:
                # No oracle available locally: fabricate a plausible IP purely so the line is
                # well-formed and regex-matchable. NOT a measurement.
                ip = synth.gauss(12.5, 1.4)
                line = (f"{int(time.time())} [oracle] {smi} est_ip=NA "
                        f"-> xtb_ip={ip:.4f} ({time.time() - t0:.1f}s)")
                log.write(line + "\n")
                n_ok += 1
                n_hit += (ip > HIT)
                continue
            try:
                ip = oracle(smi)
                # est_ip=NA: there is no model in the random arm, so no predicted IP exists.
                # `\S+` in the plotter regex matches NA, so this parses as a normal oracle line.
                line = (f"{int(time.time())} [oracle] {smi} est_ip=NA "
                        f"-> xtb_ip={ip:.4f} ({time.time() - t0:.1f}s)")
                log.write(line + "\n")
                n_ok += 1
                n_hit += (ip > HIT)
            except Exception as e:                  # xTB does not converge on every structure
                # FAILED line mirrors the simulator's; the plotter regex ignores it (no xtb_ip),
                # so a failed molecule is dropped rather than counted -- never fabricate a label.
                log.write(f"{int(time.time())} [oracle] FAILED {smi} after "
                          f"{time.time() - t0:.1f}s -> {type(e).__name__}: {str(e)[:80]}\n")
                n_fail += 1

    print(f"[random] done: {n_ok} scored, {n_fail} failed, {n_hit} hits (IP>{HIT:.0f} V) "
          f"-> {out_path}", flush=True)


if __name__ == '__main__':
    main()
