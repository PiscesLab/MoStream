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

def SendData():
        future = producer.send('Simulation', data)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            print(e)

def get_new_smile():
        consumer = KafkaConsumer('Result', bootstrap_servers=['128.110.96.15:9092'])
        result = []
        for msg in consumer:
              print(str(msg.value))
              result.append(str(msg.value.decode()))
        return result

def SimulationTask(smiles):
        #mol = Chem.MolFromInchi(inchi)
        #smiles = Chem.MolToSmiles(mol)
        inchi, xyz = generate_inchi_and_xyz(smiles)
        data = MoleculeData.from_identifier(smiles=smiles)
        compute_config = {'nnodes': 1, 'cores_per_rank': 1, 'ncores': 1}
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
        count = 0
        while True:
              smiles_input = input("Enter recommend smiles: ")
              est_ip = input("Enter est_ip: ")
              smiles, ip_simulate = SimulationTask(smiles_input)
              if (float(ip_simulate) > 14):
                 count = count + 1
              timestamp = int(time.time()*1000) 
              print("timestamp: ", timestamp, " smiles: ", smiles, " IP_simulate: ", ip_simulate, " est_ip: ", est_ip, " Found molecule: ", count)
