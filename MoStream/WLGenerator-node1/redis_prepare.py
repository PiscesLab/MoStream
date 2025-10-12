import json, csv, sys, random, subprocess, requests, time, redis

from threading import Timer
from subprocess import call, check_output

def load_train_dataset():
        file_path = "/mnt/media/MDStream/WLGenerator/dataset/training-data-simple.txt"
        smiles_list = []
        inchi_list = []
        ip_list = []
        with open(file_path) as file:
                for line in file:
                    if ("smiles" in line):
                       smiles_list.append(line.split(" ")[1].split(",")[0].split("'")[1])
                       inchi_list.append(line.split(" ")[3].split("'")[1])
                       ip_list.append(line.split(" ")[5].split("}")[0])
        return smiles_list, inchi_list, ip_list

def load_search_dataset():
        file_path = "./search_space/MOS-search.csv"
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
        return smiles_list, inchi_list, ip_list

def redis_insert(r, key, value):
        r.set(key, value)
      
def redis_database_prepare(smiles_list, inchi_list, value, r):
        if (len(value) > 1):
           for inchi in inchi_list:
              smiles = smiles_list[inchi_list.index(inchi)]
              ip = value[inchi_list.index(inchi)]
              redis_insert(r, smiles, ip)
        else:
           for inchi in inchi_list:
              smiles = smiles_list[inchi_list.index(inchi)]
              redis_insert(r, smiles, 1)

if __name__ == "__main__":
        smiles_list_train, inchi_list_train, ip_list_train = load_train_dataset()
        #smiles_list_search, inchi_list_search, _ = load_search_dataset()
        
        redis_host = '128.110.96.26'
        redis_port = 7485
    
        r = redis.Redis(host=redis_host, port=redis_port)
       
        redis_database_prepare(smiles_list_train, inchi_list_train, ip_list_train, r)
        #redis_database_prepare(smiles_list_search, inchi_list_search, [1], r) 

        
