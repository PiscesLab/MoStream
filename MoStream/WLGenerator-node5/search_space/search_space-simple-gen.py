import json, csv, sys, random, subprocess, requests, time

from threading import Timer
from subprocess import call, check_output

def load_search_dataset():
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
        return smiles_list, inchi_list

def dataset_prepare(smiles_list_search, inchi_list_search):
        dataset_path = "./MOS-search-simple.txt"
        with open(dataset_path, 'w') as file:
                for smiles in smiles_list_search:
                       inchi = inchi_list_search[smiles_list_search.index(smiles)]
                       dataset_item = {"smiles": smiles, "inchi": inchi}
                       file.write(str(dataset_item) + "\n")


if __name__ == "__main__":
        smiles_list_search, inchi_list_search = load_search_dataset()
        
        dataset_prepare(smiles_list_search, inchi_list_search) 

        
