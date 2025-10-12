import json, csv
from moldesign.simulate.functions import generate_inchi_and_xyz, relax_structure
from moldesign.simulate.specs import get_qcinput_specification
from moldesign.store.models import MoleculeData
from moldesign.store.recipes import apply_recipes
from rdkit import Chem
import logging, qcengine
logger = logging.getLogger("qcengine")
logger.setLevel(logging.CRITICAL)

def load_json_file(file_path):
        data = []
        with open(file_path) as file:
                for line in file:
                        try:
                                item = json.loads(line)
                                data.append(item)
                        except json.JSONDecodeError as e:
                                print(f"Error decoding JSON: {e}")
        return data

file_path = "./training-data.json"
json_data = load_json_file(file_path)

dataset = []
smiles_list = []
inchi_list = []
ip_list = []
for item in json_data:
        if "identifier" in item and "oxidation_potential" in item:
                smiles = item["identifier"]["smiles"]
               	dataset_item = {
                        "smiles": item["identifier"]["smiles"],
                        "inchi": item["identifier"]["inchi"],
                        "ip": item["oxidation_potential"]["xtb-vacuum"],
                }
                smiles_list.append(item["identifier"]["smiles"])
                inchi_list.append(item["identifier"]["inchi"])
                ip_list.append(item["oxidation_potential"]["xtb-vacuum"])
                dataset.append(dataset_item)


for inchi in inchi_list:
	smiles_tmp = smiles_list[inchi_list.index(inchi)]
	ip_tmp = ip_list[inchi_list.index(inchi)]

	mol = Chem.MolFromInchi(inchi)
	smiles = Chem.MolToSmiles(mol)

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

	print("smiles: ", smiles, " smiles_tmp: ", smiles_tmp, " IP_simulate: " , data.oxidation_potential['xtb-vacuum'], " IP_tmp: ", ip_tmp, inchi_list.index(inchi))
