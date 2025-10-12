import json, csv
from moldesign.simulate.functions import generate_inchi_and_xyz, relax_structure
from moldesign.simulate.specs import get_qcinput_specification
from moldesign.store.models import MoleculeData
from moldesign.store.recipes import apply_recipes
from rdkit import Chem
import logging, qcengine

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

print("Data Load Finished")
for inchi in inchi_list:
        smiles_tmp = smiles_list[inchi_list.index(inchi)]

        mol = Chem.MolFromInchi(inchi)
        smiles = Chem.MolToSmiles(mol)

        inchi, xyz = generate_inchi_and_xyz(smiles)

        data = MoleculeData.from_identifier(smiles=smiles)
        compute_config = {'nnodes': 1, 'cores_per_rank': 16, 'ncores': 16}

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

        print("smiles: ", smiles, " smiles_tmp: ", smiles_tmp, " IP_simulate: " , data.oxidation_potential['xtb-vacuum'])
