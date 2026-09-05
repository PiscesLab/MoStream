import json, csv, sys, random, subprocess, requests, time
import parsl
from parsl import python_app
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl.config import Config

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
        producer = KafkaProducer(bootstrap_servers=["KAFKA_HOST:9092"], acks=0, retries=10, api_version=(0,10,0), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        future = producer.send('Simulation', data)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            print(e)
@python_app
def SimulationTask(smiles, est_ip, recommend_time):
        from moldesign.simulate.functions import generate_inchi_and_xyz, relax_structure
        from moldesign.simulate.specs import get_qcinput_specification
        from moldesign.store.models import MoleculeData
        from moldesign.store.recipes import apply_recipes
        from rdkit import Chem

        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        from kafka import KafkaConsumer

        def SendData(data):
            producer = KafkaProducer(bootstrap_servers=["KAFKA_HOST:9092"], acks=0, retries=10, api_version=(0,10,0), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            future = producer.send('Simulation', data)
            try:
               record_metadata = future.get(timeout=10)
            except KafkaError as e:
               print(e)

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
        timestamp = int(time.time()*1000)
        error = abs(float(est_ip) - float(ip_simulate))
        result_str = "timestmap: " + str(timestamp) + " smiles: " + smiles + " IP_simulate: " + str(ip_simulate) + " IP_est: " + str(est_ip) + " molecules Found: " + str(0) + " inference error: " + str(error) + " recommed_time: " + str(recommend_time) + "\n"
        with open("./streaming-results.log", 'a') as log_file:
             log_file.write(result_str)
        model_id = random.choice(range(1))
        #model_id = 0
        data = {"timestamp": timestamp, "smiles": smiles, "inchi": inchi, "IP_simulate": ip_simulate, "model_id": model_id}
        SendData(data)
        return smiles, ip_simulate, inchi, count

if __name__ == "__main__":
        config=Config(
            executors=[
                HighThroughputExecutor(
                    #workers_per_node=16,
                    provider=LocalProvider(
                        init_blocks=1,
                        min_blocks=1,
                        max_blocks=1,
                        nodes_per_block=1,
                    ),
                )
            ]
        )
        parsl.load(config)
        consumer = KafkaConsumer('Result', bootstrap_servers=['KAFKA_HOST:9092'])
        for msg in consumer:
              value = str(msg.value)
              smiles = value.split(" ")[1]
              est_ip = value.split(" ")[5].strip("'")
              recommend_time = value.split(" ")[7].strip("'")
              recommend_str = "consumer_timestamp: " + str(int(time.time()*1000)) + " recommend_timestamp: " + str(recommend_time) + "\n"
              with open("./recommend-msg.log", 'a') as req_log:
                   req_log.write(recommend_str)
              if "model_not_ready" in value:
                 timestamp = int(time.time()*1000)
                 result_str = "timestmap: " + str(timestamp) + "\n"
                 with open("./streaming-results.log", 'a') as log_file:
                      log_file.write(result_str)
                 continue
              future = SimulationTask(smiles, est_ip, recommend_time)
              
