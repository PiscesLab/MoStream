import parsl, random
from parsl import python_app
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl.config import Config

import json, csv, time, os, sys
from moldesign.simulate.functions import generate_inchi_and_xyz, relax_structure
from moldesign.simulate.specs import get_qcinput_specification
from moldesign.store.models import MoleculeData
from moldesign.store.recipes import apply_recipes
from rdkit import Chem
import logging, qcengine

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

@python_app
def SimulationTask(smiles):

        import json, csv, time, os, sys
        from moldesign.simulate.functions import generate_inchi_and_xyz, relax_structure
        from moldesign.simulate.specs import get_qcinput_specification
        from moldesign.store.models import MoleculeData
        from moldesign.store.recipes import apply_recipes

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

        return smiles, data.oxidation_potential['xtb-vacuum']   

file_path = "./MOS-search.csv"
smiles_list = []
inchi_list = []
dict_list = []

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
                   #break

print("Data Load Finished")
count = 0
unsearched = inchi_list
while (len(unsearched) > 0):
        #print("reach here 1")
        inchi_list_tmp = random.sample(unsearched, k=16)
        #print("reach here 2")
        future_list = []
        for inchi in inchi_list_tmp:
            smiles_tmp = smiles_list[inchi_list.index(inchi)]
            #print("smiles_tmp: ", smiles_tmp)
            future_list.append(SimulationTask(smiles_tmp))
            unsearched.remove(inchi)
            #print("unsearched length: ", len(unsearched))
        for future in future_list:
            smiles, ip_simulate = future.result()
            if (float(ip_simulate) > 14): 
                 count = count + 1
            timestamp = int(time.time()*1000)
            result_str = "timestmap: " + str(timestamp) + " smiles: " + smiles + " IP_simulate: " + str(ip_simulate) + " molecules Found: " + str(count) + "\n"
            with open("./compute_ip.log", 'a') as log_file:
                 log_file.write(result_str)
        

