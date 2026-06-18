import numpy as np
import pickle as pkl
import random, time

from pyflink.datastream.functions import AllWindowFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import CountWindow
from pyflink.common.typeinfo import Types
from typing import List, Any, Optional, Tuple, Dict, Union, Iterable

class RankFunction(AllWindowFunction):

    def __init__(self):
        print("rank reach_init")
        self.searched = []
        print("rank finished init")

    def open(self, runtime_context: RuntimeContext):
        print("rank reach open")
        self.state = runtime_context.get_map_state(MapStateDescriptor('mol_dicts', Types.STRING(), Types.LIST(Types.DOUBLE()))) 
        print("rank finished open")
  
    def apply(self, window: CountWindow, inputs: Iterable[tuple]) -> List:
        #print("rank inputs: ", inputs)
        # update state and inference
        smiles_list = []
        value_list = []
        for est in inputs:
            chunk_id = int(est[0])
            est_smiles = est[1]
            est_ip = float(est[2])
            #print("chunk_id: ", chunk_id, " est_ip: ", est_ip)
            if "model_not_ready" in est_smiles:
               result = ["smiles: " + "model_not_ready" + " ucb: " + str(0) + " est_ip: " + str(0) + " timestamp: " + str(int(time.time() * 1000)) + "$"]
               return result
            #self.state.put(est_smiles, [est_ip])
            smiles_list.append(est_smiles)
            value_list.append(est_ip)
            #if self.state.contains(est_smiles):
            #   ip_list = self.state.get(est_smiles)
            #   ip_list.append(est_ip)
            #   self.state.put(est_smiles, ip_list)
            #else:
            #   self.state.put(est_smiles, [est_ip])
         
        # ranking
        molecules = {}
        #smiles_list = list(self.state.keys())
        #value_list = list(self.state.values())
        #print("smiles_list: ", smiles_list)
        #print("value_list: ", value_list)
        for smiles in smiles_list:
            #y_pred = self.state.get(smiles)
            y_pred = value_list[smiles_list.index(smiles)]
            #print("y_pred: ", y_pred)
            y_mean = np.mean(y_pred)
            y_std = np.std(y_pred)
            ucb = y_mean + 1 * y_std
            #ucb = abs(14-ucb)
            molecules[smiles] = ucb
        
        #sorted_smiles = sorted(molecules, key=molecules.get, reverse=False)
        sorted_smiles = sorted(molecules, key=molecules.get, reverse=True)
        
        # output recommend
        result = []
        count = 0
        for smiles in sorted_smiles:
            #if (np.mean(self.state.get(smiles)) > 14):
            #if (np.mean(self.state.get(smiles)) > 0.5):
               #if (smiles not in self.searched):
               if (smiles not in self.searched) and (count < 10):
                  self.searched.append(smiles)
                  line = "smiles: " + smiles + " ucb: " + str(molecules[smiles]) + " est_ip: " + str(molecules[smiles]) + " timestamp: " + str(int(time.time() * 1000)) + "$"
                  #line = "smiles: " + smiles + " ucb: " + str(molecules[smiles]) + " est_ip: " + str(np.mean(self.state.get(smiles))) + "$"
                  result.append(line)
                  count = count + 1
        return result 
