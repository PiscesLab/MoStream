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
        logger.setLevel(logging.CRITICAL)
        smiles_list, inchi_list, ip_list, valid_list, invalid_list = load_dataset()
        unsearched_mol = inchi_list
        model_id = 0        
        flag = 0
        kafka_bootstrap = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')
        producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap], acks=1, retries=10, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        # Non-blocking consumer: poll Recommend topic, fall back to random if empty
        recommend_consumer = KafkaConsumer(
                'Recommend',
                bootstrap_servers=[kafka_bootstrap],
                auto_offset_reset='latest',
                consumer_timeout_ms=500)

        while len(unsearched_mol) > 0:
              # Try to get a recommended smiles from Flink
              smiles_train, ip_train = None, None
              for msg in recommend_consumer:
                    smiles_train, ip_train = parse_recommend_msg(msg.value)
                    if smiles_train:
                          print(f"[feedback] using recommended smiles: {smiles_train} est_ip: {ip_train}")
                          break

              # Fall back to random if no recommendation available
              if not smiles_train:
                    if (flag == 0):
                          inchi = random.choice(valid_list)
                          flag = 1
                    else:
                          inchi = random.choice(invalid_list)
                          flag = 0
                    smiles_train = smiles_list[inchi_list.index(inchi)]
                    ip_train = ip_list[inchi_list.index(inchi)]

              time.sleep(1)
              timestamp = int(time.time() * 1000)
              model_id = random.choice(range(1))
              data = {"timestamp": timestamp, "smiles": smiles_train, "inchi": "", "IP_simulate": ip_train, "model_id": model_id}
              SendData()

        
