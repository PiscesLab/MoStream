import nfp
import tensorflow as tf
import numpy as np
import pickle as pkl

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from tensorflow.python.keras import callbacks as cb
from typing import List, Any, Optional, Tuple, Dict, Union
from moldesign.utils.conversions import convert_string_to_dict
from moldesign.utils.callbacks import LRLogger, EpochTimeLogger, TimeLimitCallback
from moldesign.score.nfp import make_data_loader, ReduceAtoms

class TrainFunction(KeyedProcessFunction):

    def __init__(self):
        print("reach_init")
        self.state = None
        self.num_epochs = 2
        self.batch_size = 5
        self.validation_split = 0.1
        self.learning_rate = 1e-3
        self.random_state = 1

    def open(self, runtime_context: RuntimeContext):
        print("reach open")
        self.state = runtime_context.get_state(ValueStateDescriptor('model_dataset', Types.LIST(Types.STRING())))
        print("finished open")
  
    def process_element(self, new_tuple, ctx: 'KeyedProcessFunction.Context') -> List:
        
        # retrieve the current dataset
        current_dataset = self.state.value()
        new_x = new_tuple[0]
        new_y = new_tuple[1]
        model_id = new_tuple[2]
        print("model_id", model_id)
 
        if (current_dataset is None):
            current_dataset = [str(new_x) + "," + str(new_y) + "," + str(model_id)]
            print("current_dataset0: ", current_dataset, len(current_dataset))
        elif (len(current_dataset) < self.batch_size):
             current_dataset.append(str(new_x) + "," + str(new_y) + str(model_id))
             print("current_dataset1: ", current_dataset, len(current_dataset))
        else:
             current_dataset.pop(0)
             current_dataset.append(str(new_x) + "," + str(new_y) + str(model_id))
             print("current_dataset2: ", current_dataset, len(current_dataset))
        # Update NPMM Model State
        self.state.update(current_dataset)
        # Make the data loaders
        x_tmp = []
        y_tmp = []
        for item in current_dataset:
            x_tmp.append(convert_string_to_dict(item.split(",")[0]))
            y_tmp.append(float(item.split(",")[1]))
        mol_dicts = np.array(x_tmp)
        max_size = max(len(x['atom']) for x in mol_dicts)
        y = np.array(y_tmp)
 
        custom_objects = nfp.custom_objects.copy()
        custom_objects['ReduceAtoms'] = ReduceAtoms 

        # Make the training and validation splits
        rng = np.random.RandomState(self.random_state)
        train_split = rng.rand(len(x_tmp)) > self.validation_split
        train_X = mol_dicts[train_split]
        train_y = y[train_split]
        valid_X = mol_dicts[~train_split]
        valid_y = y[~train_split]
        
        if (len(current_dataset) < self.batch_size):
           return ["haah"]

        # Make the loaders
        steps_per_epoch = len(train_X) // self.batch_size
        train_loader = make_data_loader(train_X, train_y, repeat=True, batch_size=self.batch_size, max_size=max_size, drop_last_batch=True, shuffle_buffer=32768)
        valid_steps = len(valid_X) // self.batch_size
        valid_loader = make_data_loader(valid_X, valid_y, batch_size=self.batch_size, max_size=max_size, drop_last_batch=True)

        # Make a copy of the model
        model = tf.keras.models.load_model("/mnt/media/MDStream/StreamML/networks/model.h5", custom_objects=custom_objects)
        try:
            scaler_layer = model.get_layer('scale')
            outputs = np.array(y_tmp)
            scaler_layer.set_weights([outputs.std()[None, None], outputs.mean()[None]])
        except ValueError:
            pass

        print("Finish model loading")

        # Configure the LR schedule
        init_learn_rate = self.learning_rate
        final_learn_rate = init_learn_rate * 1e-3
        decay_rate = (final_learn_rate / init_learn_rate) ** (1. / (self.num_epochs - 1))

        def lr_schedule(epoch, lr):
            return lr * decay_rate

        # Compile the model then train
        model.compile(
            tf.optimizers.Adam(init_learn_rate),
            'mean_squared_error',
            metrics=['mean_absolute_error'],
            steps_per_execution=1
        )
        
        print("Start model fit")
        history = model.fit(
            train_loader,
            epochs=self.num_epochs,
            shuffle=False,
            verbose=False,
            steps_per_epoch=len(train_X),
            validation_data=valid_loader,
            validation_steps=valid_steps,
            validation_freq=1
        )
        print("Finish model fit")

        # Convert weights to numpy arrays (avoids mmap issues)
        weights = []
        for v in model.get_weights():
            v = np.array(v)
            if np.isnan(v).any():
                raise ValueError('Found some NaN weights.')
            weights.append(v)
        
        # Once we are finished training call "clear_session"
        tf.keras.backend.clear_session()
        return weights
              
             
               
