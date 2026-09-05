import json, csv, sys, random, subprocess, requests, time

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
        with open(file_path) as file:
                for line in file:
                    if ("smiles" in line):
                       smiles_list.append(line.split(" ")[1].split(",")[0].split("'")[1])
                       inchi_list.append(line.split(" ")[3].split("'")[1])
                       ip_list.append(line.split(" ")[5].split("}")[0])
        return smiles_list, inchi_list, ip_list

def SendData():
        future = producer.send('Simulation', data)
        #future = producer.send('Result', data)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            print(e)

def get_new_smile():
        consumer = KafkaConsumer('Recommend', bootstrap_servers=['KAFKA_HOST:9092'])
        for msg in consumer:
                return eval(str(msg.value))

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
        smiles_list, inchi_list, ip_list = load_dataset()
        searched_mol = []
        unsearched_mol = inchi_list
        model_id = 0        

        while len(unsearched_mol) > 0:
        #for i in range(5):
              inchi = random.choice(unsearched_mol)
              smiles_train = smiles_list[inchi_list.index(inchi)]
              ip_train = ip_list[inchi_list.index(inchi)]  
              time.sleep(1)
              #smiles, ip_simulate = SimulationTask(inchi)

              producer = KafkaProducer(bootstrap_servers=["KAFKA_HOST:9092"], acks=0, retries=10, api_version=(0,10,0), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
              timestamp = int(time.time() * 1000)
              #model_id = random.choice(range(16))
              #model_id = (model_id + 1) % 16
              model_id = 0
              data = {"timestamp": timestamp, "smiles": smiles_train, "inchi": inchi, "IP_simulate": ip_train, "model_id": model_id}
              #print("smiles: ", smiles, " smiles_tmp: ", smiles_tmp, " IP_simulate: " , data.oxidation_potential['xtb-vacuum'], " IP_tmp: ", ip_tmp, inchi_list.index(inchi))
              SendData()
 
              searched_mol.append(inchi)
              #unsearched_mol.remove(inchi)

        
