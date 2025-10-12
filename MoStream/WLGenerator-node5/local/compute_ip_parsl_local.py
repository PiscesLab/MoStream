import json, csv, sys, random, subprocess, requests, time, nfp
import parsl
import tensorflow as tf
import numpy as np

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
from moldesign.utils.conversions import convert_string_to_dict
from moldesign.utils.callbacks import LRLogger, EpochTimeLogger, TimeLimitCallback
from moldesign.score.nfp import make_data_loader, ReduceAtoms

from rdkit import Chem
import logging

def SendData(data):
    kafka_bootstrap = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')
    producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap], acks=0, retries=10, api_version=(0,10,0), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        future = producer.send('Simulation', data)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            print(e)
@python_app
def SimulationTask(smiles, est_ip):
        from moldesign.simulate.functions import generate_inchi_and_xyz, relax_structure
        from moldesign.simulate.specs import get_qcinput_specification
        from moldesign.store.models import MoleculeData
        from moldesign.store.recipes import apply_recipes
        from rdkit import Chem

        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        from kafka import KafkaConsumer

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
        result_str = "timestmap: " + str(timestamp) + " smiles: " + smiles + " IP_simulate: " + str(ip_simulate) + " IP_est: " + str(est_ip) + " molecules Found: " + str(0) + " inference error: " + str(error) + "\n"
        with open("./local-inference-results.log", 'a') as log_file:
             log_file.write(result_str)
        #model_id = random.choice(range(16))
        model_id = 0
        data = {"timestamp": timestamp, "smiles": smiles, "inchi": inchi, "IP_simulate": ip_simulate, "model_id": model_id}
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
        file_path = "/mnt/media/MDStream/WLGenerator/search_space/MOS-search.csv"
        smiles_list = []
        inchi_list = []
        dict_list = []
        custom_objects = nfp.custom_objects.copy()
        custom_objects['ReduceAtoms'] = ReduceAtoms
        model = tf.keras.models.load_model("/mnt/media/MDStream/WLGenerator/local/training/networks/model-1688724608288.h5", custom_objects=custom_objects, compile=True)

        print("Data Loading")
        with open(file_path, "r") as file:
             reader = csv.DictReader(file)
             for row in reader:
                 smiles_list.append(row["smiles"])
                 inchi_list.append(row["inchi"])
                 dict_data = json.loads(row["dict"])
                 dict_list.append(dict_data)
                 if (len(smiles_list) % 1000 == 0):
                     print(len(smiles_list))
        for smiles in smiles_list:
            x_tmp = [convert_string_to_dict(smiles)]
            mol_dicts = np.array(x_tmp)
            max_size = max(len(x['atom']) for x in mol_dicts)
            batch_size = len(mol_dicts)
         
            loader = make_data_loader(
                mol_dicts,
                batch_size=batch_size,
                repeat=False,
                max_size=max_size,
            )
            est_ip = np.squeeze(model.predict(loader))
            future = SimulationTask(smiles, est_ip)
              
