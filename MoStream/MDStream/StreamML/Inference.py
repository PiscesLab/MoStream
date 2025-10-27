import nfp, json
import tensorflow as tf
import numpy as np
import pickle as pkl
import redis, random, time, math

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from tensorflow.python.keras import callbacks as cb
from typing import List, Any, Optional, Tuple, Dict, Union
from moldesign.utils.conversions import convert_string_to_dict
from moldesign.utils.callbacks import LRLogger, EpochTimeLogger, TimeLimitCallback
from moldesign.score.nfp import make_data_loader, ReduceAtoms

def sigmod(x):
    return 1 / (1 + math.exp(-x))

def load_search_space_all():
        file_path = "/mnt/media/MDStream/StreamML/search_space/MOS-search-simple.txt"
        smiles_list = []
        with open(file_path) as file:
                for line in file:
                    if ("smiles" in line):
                       smiles_list.append(line.split(" ")[1].split(",")[0].split("'")[1])
        return smiles_list

def load_search_space(chunk_id):
        file_path = "/mnt/media/MDStream/StreamML/search_space/MOS-search-simple.txt"
        smiles_list = []
        #inchi_list = []
        with open(file_path, 'r') as file:
                for line_number, line in enumerate(file, start=1):
                    if ("smiles" in line) and ((line_number >= chunk_id*500) and (line_number < (chunk_id+1)*500)):
                       smiles_list.append(line.split(" ")[1].split(",")[0].split("'")[1])
                       #inchi_list.append(line.split(" ")[3].split("'")[1])
                       if (len(smiles_list)>=500):
                          return smiles_list
        return smiles_list

class InferFunction(KeyedProcessFunction):

    def __init__(self):
        print("infer reach_init")
        #self.search_space = load_search_space_all()
        self.state = None
        self.chunk_id_list = []
        #self.model_paras = None
        print("infer finished init")

    def open(self, runtime_context: RuntimeContext):
        print("infer reach open")
        self.state = runtime_context.get_state(ValueStateDescriptor('search_space', Types.LIST(Types.STRING())))
        #self.state = runtime_context.get_state(ValueStateDescriptor('search_space', Types.STRING()))
        print("infer finished open")
  
    def process_element(self, new_tuple, ctx: 'KeyedProcessFunction.Context') -> List:
        #self.state.update(random.sample(self.search_space, k=500))
        #tmp_search_space = random.sample(self.search_space, k=100)
        chunk_id = int(new_tuple[0])
        weights_json_str = new_tuple[1]
        model_id = new_tuple[2]
        #infra_json_str = None
        infra_json_str = new_tuple[3]
        if chunk_id not in self.chunk_id_list:
           self.chunk_id_list.append(chunk_id)
        print("infer chunk_id: ", chunk_id, "chunk_id_list: ", len(self.chunk_id_list))
        #self.state.update(load_search_space(chunk_id))
        #tmp_search_space = self.state
        #tmp_search_space = load_search_space(chunk_id)
        if (self.state.value() == None):
           self.state.update(load_search_space(chunk_id))
        tmp_search_space = self.state.value()
        #if (chunk_id != 2231):
        #   tmp_search_space = self.search_space[chunk_id*500:(chunk_id+1)*500]
        #else:
        #   tmp_search_space = self.search_space[1115000:1115319]
        #print("infer weights: ", weights)
 
        if (weights_json_str is None):
            result = [str(chunk_id) + "$" + "model_not_ready weights_none" + "$" + str(0)]
            return result

        if ("haaah" in weights_json_str):
            result = [str(chunk_id) + "$" + "model_not_ready" + "$" + str(0)]
            return result
        
        #self.model_paras = weights
        #self.state.update(weights)
        custom_objects = nfp.custom_objects.copy()
        custom_objects['ReduceAtoms'] = ReduceAtoms
        
        # Load mpnn model
        model = tf.keras.models.model_from_json(infra_json_str, custom_objects=custom_objects)
        weights_list = json.loads(weights_json_str)
        weights = [np.array(arr) for arr in weights_list]
        #model = tf.keras.models.load_model("/mnt/media/MDStream/StreamML/networks/model-local.h5", custom_objects=custom_objects, compile=True)
        model.set_weights(weights)
        print("infer model loading finished")
        
        # Load search space (inference chunks from redis)
        #smiles_search = random.choice(self.state.value())
        #smiles_search = random.choice(tmp_search_space)

        # prepare inference args:
        # model: MPNN to evaluate
        # mol_dicts: List of molecules as MPNN-ready disctionary objections
        # batch_size: Number of molecules per batch
        # max_size: Maximum size of the molecules
        #x_tmp = [convert_string_to_dict(smiles_search)]
        #mol_dicts = np.array(x_tmp)
        x_tmp = []
        for smiles_search in tmp_search_space:
            x_tmp.append(convert_string_to_dict(smiles_search))
        mol_dicts = np.array(x_tmp)
        max_size = max(len(x['atom']) for x in mol_dicts)
        batch_size = len(mol_dicts)
        
        loader = make_data_loader(
            mol_dicts,
            batch_size=batch_size,
            repeat=False,
            max_size=max_size,
        )
        # save model
        print("saved model parameters")
        #model_name = '/mnt/media/MDStream/StreamML/saved_networks/model-' + str(model_id) + '.h5'
        #model.save(model_name)

        # predicted IP
        pred_y = np.squeeze(model.predict(loader))
        #sigmod_pred_y = np.array([sigmod(x) for x in pred_y])
        #print("infer chunk_id: ", chunk_id, "search_smiles: ", smiles_search, " pred_y: ", pred_y)
        #result = [str(chunk_id) + "$" + smiles_search + "$" + str(pred_y)]
        result = []
        for smiles_search in tmp_search_space:
            line = str(chunk_id) + "$" + smiles_search + "$" + str(pred_y[tmp_search_space.index(smiles_search)])
            #line = str(chunk_id) + "$" + smiles_search + "$" + str(sigmod_pred_y[tmp_search_space.index(smiles_search)])
            result.append(line)
        #self.state.update(weights_json_str)
        return result
