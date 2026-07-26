import json, csv, sys, random, subprocess, requests, time, os

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer
from threading import Timer
from subprocess import call, check_output

from moldesign.simulate.functions import generate_inchi_and_xyz, relax_structure
from moldesign.simulate.specs import get_qcinput_specification
from moldesign.store.models import MoleculeData
from moldesign.store.recipes import apply_recipes
from rdkit import Chem
import logging
logger = logging.getLogger("qcengine")
logger.setLevel(logging.CRITICAL)

def load_dataset():
        file_path = "/mnt/media/MDStream/WLGenerator/dataset/training-data-simple.txt"
        smiles_list = []
        inchi_list = []
        ip_list = []
        valid_inchi = []
        invalid_inchi = []
        with open(file_path) as file:
                for line in file:
                    if ("smiles" in line):
                       smiles_list.append(line.split(" ")[1].split(",")[0].split("'")[1])
                       inchi_list.append(line.split(" ")[3].split("'")[1])
                       ip_list.append(line.split(" ")[5].split("}")[0])
                       if (float(line.split(" ")[5].split("}")[0]) > 14):
                          valid_inchi.append(line.split(" ")[3].split("'")[1])
                       else:
                          invalid_inchi.append(line.split(" ")[3].split("'")[1])
        return smiles_list, inchi_list, ip_list, valid_inchi, invalid_inchi

def SendData():
        future = producer.send('Simulation', data)
        #future = producer.send('Result', data)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            print(e)

def parse_recommend_msg(msg_value):
        # Flink writes: "smiles: CCO ucb: 0.42 est_ip: 9.1 timestamp: 1234"
        try:
                text = msg_value.decode('utf-8') if isinstance(msg_value, bytes) else msg_value
                tokens = text.split()
                parts = {tokens[i].rstrip(':'): tokens[i+1] for i in range(0, len(tokens)-1, 2)}
                smiles = parts.get('smiles')
                est_ip = float(parts.get('est_ip', 0.0))
                return smiles, est_ip
        except Exception:
                return None, None

def SimulateFromSmiles(smiles):
        """THE ORACLE. Run the real xTB calculation for `smiles` and return its ionization
        potential, or None if the calculation fails.

        This exists because a recommendation carries only a SMILES (Rank emits
        "smiles: X ucb: ... est_ip: ..."), while SimulationTask below takes an InChI. It is the
        same computation: relax the neutral geometry, relax the oxidized geometry, apply the
        recipes, read off the vacuum IP.

        WHY THIS MATTERS. Before this, the feedback path took `est_ip` -- the MODEL'S OWN
        PREDICTION -- straight off the Recommend topic and published it back as `IP_simulate`.
        The surrogate then trained on its own output, and a "hit" (IP > 14 V) counted a molecule
        the model merely BELIEVED was good. There is no oracle in that loop, so it can
        manufacture hits by predicting high, and any comparison between two arms graded that way
        is meaningless.

        The search space (MOS-search-simple.txt, 1.1M molecules) carries smiles and inchi but NO
        ip field, so a recommended molecule has no ground truth to look up: the only way to score
        it is to actually run the chemistry. That is what this does.

        Verified against the stored training data: this returns 15.9334 V for OCC(F)(F)F, where
        training-data-simple.txt records 15.9334397319864 -- the same pipeline that generated the
        dataset, agreeing to four decimals. Cost scales steeply with size: ~14 s for 6 atoms,
        ~126 s for 18. At ~126 s x 3 simulators the arrival rate is ~0.024 rec/s, which is
        Colmena's published 0.025 rec/s arrived at independently, because it is the same oracle.
        """
        inchi, xyz = generate_inchi_and_xyz(smiles)
        data = MoleculeData.from_identifier(smiles=smiles)
        compute_config = {'nnodes': 1, 'cores_per_rank': 1, 'ncores': 16}
        spec, code = get_qcinput_specification('xtb')
        neutral_relax = relax_structure(xyz, spec, compute_config=compute_config, charge=0, code=code)
        oxidized_relax = relax_structure(neutral_relax.final_molecule.to_string('xyz'), spec,
                                         compute_config=compute_config, charge=1, code=code)
        for r in (neutral_relax, oxidized_relax):
                data.add_geometry(r)
        data.update_thermochem()
        apply_recipes(data)
        return data.oxidation_potential['xtb-vacuum']

def SimulationTask(inchi):
        mol = Chem.MolFromInchi(inchi)
        smiles = Chem.MolToSmiles(mol)
        inchi, xyz = generate_inchi_and_xyz(smiles)
        data = MoleculeData.from_identifier(smiles=smiles)
        compute_config = {'nnodes': 1, 'cores_per_rank': 1, 'ncores': 16}
        spec, code = get_qcinput_specification('xtb')
        neutral_relax = relax_structure(xyz, spec, compute_config=compute_config, charge=0, code=code)
        oxidized_relax = relax_structure(neutral_relax.final_molecule.to_string('xyz'), spec, compute_config=compute_config, charge=1, code=code)
        opt_records = [neutral_relax, oxidized_relax]
        hess_records = []
        for r in opt_records:
                data.add_geometry(r)
        for r in hess_records:
                data.add_single_point(r)
        data.update_thermochem()
        apply_recipes(data)
        return smiles, data.oxidation_potential['xtb-vacuum']

if __name__ == "__main__":
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--interval', type=float, default=1.0, help='Seconds between messages')
        args = parser.parse_args()

        logger = logging.getLogger("__main__")
        logger.setLevel(logging.CRITICAL)
        smiles_list, inchi_list, ip_list, valid_list, invalid_list = load_dataset()
        unsearched_mol = inchi_list
        model_id = 0
        flag = 0
        # Number of distinct model_id keys to cycle. Default 16 (one per ensemble slot). E6 sets
        # MOSTREAM_NKEYS=1 so every record trains ONE model: warm-up (batch_size records for a
        # key) then takes seconds instead of ~35 min, and the training-MAE trace is a single
        # clean line rather than 16 interleaved ones -- which is exactly what the persistence
        # ablation wants to plot.
        n_keys = max(1, int(os.environ.get('MOSTREAM_NKEYS', '16')))
        kafka_bootstrap = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')
        producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap], acks=1, retries=10, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        # Non-blocking consumer: poll Recommend topic, fall back to random if empty
        recommend_consumer = KafkaConsumer(
                'Recommend',
                bootstrap_servers=[kafka_bootstrap],
                auto_offset_reset='latest',
                consumer_timeout_ms=500)

        # SKIP THE BACKLOG, EXPLICITLY. `Recommend` is never truncated, so it accumulates every
        # recommendation this cluster has ever emitted (measured: 10.2M messages). Without this,
        # a fresh simulator replays other runs' recommendations and, now that a recommendation
        # costs a ~100 s xTB call, it burns the whole run scoring molecules some earlier job
        # chose. That also DEADLOCKS the warm-up: the oracle blocks the seed path, so the new
        # model never reaches its 16-record threshold, never emits a real recommendation, and the
        # simulator keeps chewing stale ones forever. (Observed: 11 records ingested in 6.6 min,
        # Infer emitting only `model_not_ready`, Rank emitting nothing, while the simulator ran
        # the oracle on a recommendation the running job never made.)
        #
        # auto_offset_reset='latest' is NOT sufficient: it only applies when the group has no
        # committed offset, and assignment happens lazily, so the first poll can still deliver
        # buffered history. poll() forces assignment, then seek_to_end() pins us to the live tail.
        recommend_consumer.poll(timeout_ms=2000)
        recommend_consumer.seek_to_end()
        print(f"[simulator] skipped Recommend backlog; consuming from the live tail", flush=True)

        # THE ORACLE SWITCH. Default ON: a recommended molecule is scored by actually running
        # xTB (SimulateFromSmiles). Set MOSTREAM_REAL_ORACLE=0 only to reproduce the old
        # est_ip-echo behaviour, which is NOT a valid experiment -- see SimulateFromSmiles.
        real_oracle = os.environ.get('MOSTREAM_REAL_ORACLE', '1') == '1'
        print(f"[simulator] real_oracle={real_oracle}  n_keys={n_keys}")

        _bad = ('search_space_empty', 'model_not_ready', 'mol_dicts_empty', 'inference_error')
        while len(unsearched_mol) > 0:
              # ALWAYS ACT ON THE FRESHEST RECOMMENDATION. A recommendation now costs a ~100 s
              # oracle call, and Rank emits ~80 recommendations/s, so ~8000 pile up in `Recommend`
              # during one call. If we consumed that buffer FIFO -- taking the first message and
              # breaking -- every pick would be one full oracle-call staler than the last, and the
              # simulator would fall progressively behind, acting on minutes-old rankings while the
              # live model has moved on. (Observed: sims scoring est_ip=13.55 molecules while the
              # running job was already emitting est_ip=12.8-13.0.) So we jump to the live tail and
              # take the LAST valid message, discarding the backlog the oracle's own latency built
              # up. This is the simulator-side analogue of the steering-latency argument itself:
              # under a slow consumer, only the newest recommendation is worth acting on.
              recommend_consumer.seek_to_end()
              smiles_train, ip_train, est_ip = None, None, None
              for msg in recommend_consumer:          # messages arriving in the next 500 ms
                    s, e = parse_recommend_msg(msg.value)
                    if s and not any(p in s for p in _bad):
                          smiles_train, est_ip = s, e   # keep overwriting -> last valid == freshest

              if smiles_train and real_oracle:
                    # A recommendation is a molecule from the 1.1M search space, which carries no
                    # ground-truth ip. Score it for real. The oracle is the pacing element here,
                    # not args.interval: it costs ~14-126 s depending on molecule size, which is
                    # exactly why the loop's arrival rate lands at Colmena's.
                    _t0 = time.time()
                    try:
                          ip_train = SimulateFromSmiles(smiles_train)
                          print(f"[oracle] {smiles_train} est_ip={est_ip} -> xtb_ip={ip_train:.4f} "
                                f"({time.time()-_t0:.1f}s)", flush=True)
                    except Exception as e:
                          # xTB does not converge on every structure. Drop the molecule rather
                          # than fabricate a label for it; the next loop takes a fresh one.
                          print(f"[oracle] FAILED {smiles_train} after {time.time()-_t0:.1f}s "
                                f"-> {type(e).__name__}: {str(e)[:80]}", flush=True)
                          smiles_train, ip_train = None, None
              elif smiles_train:
                    ip_train = est_ip   # MOSTREAM_REAL_ORACLE=0: the old, invalid echo path

              # Fall back to a seed molecule when no recommendation is usable. These come from
              # the 4511-molecule training table, which DOES carry a measured ip, so they need no
              # oracle call: they are the already-known data that warms the surrogate up.
              if not smiles_train:
                    if (flag == 0):
                          inchi = random.choice(valid_list)
                          flag = 1
                    else:
                          inchi = random.choice(invalid_list)
                          flag = 0
                    smiles_train = smiles_list[inchi_list.index(inchi)]
                    ip_train = ip_list[inchi_list.index(inchi)]
                    time.sleep(args.interval)   # only the seed path needs artificial pacing

              timestamp = int(time.time() * 1000)
              #model_id = random.choice(range(1))
              model_id = (model_id + 1) % n_keys
              data = {"timestamp": timestamp, "smiles": smiles_train, "inchi": "", "IP_simulate": ip_train, "model_id": model_id}
              SendData()

        
