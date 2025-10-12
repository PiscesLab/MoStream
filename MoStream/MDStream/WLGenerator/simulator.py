import json, csv, sys, random, subprocess, requests, time, os, argparse, logging

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
logger = logging.getLogger("qcengine")
logger.setLevel(logging.CRITICAL)

def load_dataset():
        # Use a relative dataset path when possible; allow override with env or CLI
        file_path = os.environ.get('SIM_DATASET', os.path.join(os.path.dirname(__file__), 'dataset', 'training-data-simple.txt'))
        smiles_list = []
        inchi_list = []
        ip_list = []
        if not os.path.exists(file_path):
                raise FileNotFoundError(f"Dataset not found: {file_path}")
        with open(file_path) as file:
                for line in file:
                        # expect lines that contain 'smiles' key like: "smiles: 'CCO', inchi: 'InChI=1S/...', ip: 5.6"
                        if ("smiles" in line):
                                try:
                                        # crude but resilient parsing
                                        parts = line.replace('\n','').split(',')
                                        s = parts[0].split("'")[1]
                                        i = parts[1].split("'")[1]
                                        ip_val = float(parts[2].split(':')[-1].strip())
                                except Exception:
                                        # fallback: try previous parsing style
                                        try:
                                                s = line.split(" ")[1].split(",")[0].split("'")[1]
                                                i = line.split(" ")[3].split("'")[1]
                                                ip_val = float(line.split(" ")[5].split("}")[0])
                                        except Exception:
                                                continue
                                smiles_list.append(s)
                                inchi_list.append(i)
                                ip_list.append(ip_val)
        return smiles_list, inchi_list, ip_list

def SendData(producer, data, topic='Simulation', dry_run=False):
        """Send a dict `data` to Kafka using `producer` or print when dry_run=True."""
        payload = json.dumps(data).encode('utf-8')
        if dry_run:
                print(f"[DRY-RUN] Topic={topic} Payload={payload.decode()}")
                return True
        try:
                future = producer.send(topic, value=payload)
                # optionally block briefly for send
                record_metadata = future.get(timeout=10)
                return True
        except KafkaError as e:
                print("KafkaError while sending:", e)
                return False

def get_new_smile(kafka_bootstrap='localhost:9092'):
        consumer = KafkaConsumer('Recommend', bootstrap_servers=[kafka_bootstrap], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', api_version=(0,10,0), value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        for msg in consumer:
                return msg.value

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
        logger = logging.getLogger("__main__")
        logger.setLevel(logging.INFO)
        parser = argparse.ArgumentParser(description='Simulator to send molecule data to Kafka or run locally')
        parser.add_argument('--kafka-bootstrap', default=os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092'))
        parser.add_argument('--topic', default=os.environ.get('SIM_TOPIC', 'Simulation'))
        parser.add_argument('--interval', type=float, default=1.0, help='seconds between messages')
        parser.add_argument('--model-id', type=int, default=1)
        parser.add_argument('--local', action='store_true', help='dry-run local mode (print JSON)')
        args = parser.parse_args()

        smiles_list, inchi_list, ip_list = load_dataset()
        if len(inchi_list) == 0:
                print('No molecules found in dataset; exiting')
                sys.exit(1)

        searched_mol = []
        unsearched_mol = inchi_list.copy()

        producer = None
        if not args.local:
                try:
                        print(f"[simulator] Creating KafkaProducer connecting to {args.kafka_bootstrap}")
                        producer = KafkaProducer(bootstrap_servers=[args.kafka_bootstrap], acks=0, retries=5, api_version=(0,10,0))
                        print("[simulator] KafkaProducer created")
                except Exception as e:
                        print("[simulator] Failed to create KafkaProducer:", e)
                        producer = None

        try:
                while len(unsearched_mol) > 0:
                        inchi = random.choice(unsearched_mol)
                        idx = inchi_list.index(inchi)
                        smiles_train = smiles_list[idx]
                        ip_train = ip_list[idx]
                        time.sleep(args.interval)

                        timestamp = int(time.time() * 1000)
                        data = {
                                "timestamp": timestamp,
                                "smiles": smiles_train,
                                "inchi": inchi,
                                "IP_simulate": float(ip_train),
                                "model_id": int(args.model_id)
                        }

                        try:
                                success = SendData(producer, data, topic=args.topic, dry_run=args.local)
                                if not success:
                                        print('[simulator] SendData reported failure for', smiles_train)
                                else:
                                        print('[simulator] Message sent for', smiles_train)
                        except Exception as e:
                                print('[simulator] Exception during SendData:', e)
                                success = False
                        if not success:
                                print('Failed to send data for', smiles_train)

                        searched_mol.append(inchi)
                        unsearched_mol.remove(inchi)
        except KeyboardInterrupt:
                print('Interrupted by user')
        finally:
                if producer is not None:
                        try:
                                producer.flush()
                                producer.close()
                        except Exception:
                                pass

        
