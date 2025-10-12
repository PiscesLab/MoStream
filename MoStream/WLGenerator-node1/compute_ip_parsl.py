import json, csv, sys, random, subprocess, requests, time
import parsl
from parsl import python_app

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

def SendData(data):
        producer = KafkaProducer(bootstrap_servers=["128.110.96.15:9092"], acks=0, retries=10, api_version=(0,10,0), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        future = producer.send('Simulation', data)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            print(e)
@python_app
def SimulationTask(smiles, count, est_ip):
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
        ip_simulate = data.oxidation_potential['xtb-vacuum']
        if (float(ip_simulate) > 14):
                 count = count + 1
        timestamp = int(time.time()*1000)
        error = abs(float(est_ip) - float(ip_simulate))
        result_str = "timestmap: " + str(timestamp) + " smiles: " + smiles + " IP_simulate: " + str(ip_simulate) + " IP_est: " + str(est_ip) + " molecules Found: " + str(count) + " inference error: " + str(error) + "\n"
        with open("./streaming-results.log", 'a') as log_file:
             log_file.write(result_str)
        return smiles, ip_simulate, inchi, count

if __name__ == "__main__":
        parsl.load()
        logger = logging.getLogger("parsl")
        logger.setLevel(logging.CRITICAL)
        count = 0
        consumer = KafkaConsumer('Result', bootstrap_servers=['128.110.96.15:9092'])
        for msg in consumer:
              value = str(msg.value)
              smiles = value.split(" ")[1]
              est_ip = value.split(" ")[5].strip("'")
              #print(smiles, est_ip)
              if "model_not_ready" in value:
                 continue
              future = SimulationTask(smiles, count, est_ip)
              smiles, ip_simulate, inchi, count = future.result()
              timestamp = int(time.time()*1000)
              #model_id = random.choice(range(16))
              model_id = 0
              data = {"timestamp": timestamp, "smiles": smiles, "inchi": inchi, "IP_simulate": ip_simulate, "model_id": model_id}
              
