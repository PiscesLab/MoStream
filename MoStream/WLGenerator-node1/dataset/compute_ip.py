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

Valid = []
NonValid = []
for inchi in inchi_list:
	smiles = smiles_list[inchi_list.index(inchi)]
	ip = ip_list[inchi_list.index(inchi)]
	if (float(ip) > 14):
		Valid.append((smiles, ip))
	else:
		NonValid.append((smiles, ip))
	
	print("smiles: ", smiles, " IP_tmp: ", ip, " Valid: ", len(Valid), " NonValid: ", len(NonValid))
