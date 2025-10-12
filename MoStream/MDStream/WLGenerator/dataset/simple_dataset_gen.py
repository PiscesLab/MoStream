import json
#from moldesign.store.models import MoleculeData

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
for item in json_data:
	if "identifier" in item and "oxidation_potential" in item:
		smiles = item["identifier"]["smiles"]
		dataset_item = {
			"smiles": item["identifier"]["smiles"],
			"inchi": item["identifier"]["inchi"],
			"ip": item["oxidation_potential"]["xtb-vacuum"],
		}
		dataset.append(dataset_item)

dataset_path = "./training-data-simple.txt"
with open(dataset_path, 'w') as file:
	for data in dataset:
		file.write(str(data) + "\n")
