import nfp, random, json, h5py
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
        #print("reach_init")
        self.state = None
        self.num_epochs = 1
        self.batch_size = 16
        self.validation_split = 0.1
        self.learning_rate = 1e-3
        self.random_state = 1
        self.model_paras = None
        self.model = None        # loaded once in open(), reused every iteration
        self.infra_json_str = None
        print("finished init")

    def open(self, runtime_context: RuntimeContext):
        print("train reach open")
        self.state = runtime_context.get_state(ValueStateDescriptor('training_dataset', Types.LIST(Types.STRING())))
        # Load architecture-only from h5 — weights are skipped because the h5 was saved
        # with an older nfp where EdgeUpdate had 10 weights; current nfp 0.3.12 builds it
        # with 4. Loading weights would crash. The model trains from scratch on each batch.
        model_path = "/home/namdo/applications/MoStream/MoStream/MDStream/StreamML/networks/model.h5"
        #model_path = "/mnt/media/MDStream/StreamML/networks/model.h5"  # CloudLab NFS path
        custom_objects = nfp.custom_objects.copy()
        custom_objects['ReduceAtoms'] = ReduceAtoms
        with h5py.File(model_path, 'r') as f:
            model_config = f.attrs['model_config']
            if isinstance(model_config, bytes):
                model_config = model_config.decode('utf-8')
        self.model = tf.keras.models.model_from_json(model_config, custom_objects=custom_objects)
        self.infra_json_str = model_config
        print("train finished open")
  
    def process_element(self, new_tuple, ctx: 'KeyedProcessFunction.Context') -> List:
        # retrieve the current dataset
        current_dataset = self.state.value()
        new_x = new_tuple[0]
        new_y = new_tuple[1]
        model_id = int(new_tuple[2])
        #print("model_id", model_id)
 
        if (current_dataset is None):
            current_dataset = [str(new_x) + "," + str(new_y) + "," + str(model_id)]
        elif (len(current_dataset) < self.batch_size):
             current_dataset.append(str(new_x) + "," + str(new_y) + str(model_id))
        else:
             current_dataset.pop(0)
             current_dataset.append(str(new_x) + "," + str(new_y) + str(model_id))
        
        # Update NPMM Model State
        self.state.update(current_dataset)
        # Make the data loaders
        x_tmp = []
        y_tmp = []
        for item in current_dataset:
            x_tmp.append(convert_string_to_dict(item.split(",")[0]))
            #y_new = float(item.split(",")[1])
            #if (y_new > 14):
            #   y_new = float(1)
            #else:
            #   y_new = float(0)
            y_tmp.append(float(item.split(",")[1]))
            #y_tmp.append(y_new)
        mol_dicts = np.array(x_tmp)
        max_size = max(len(x['atom']) for x in mol_dicts)
        y = np.array(y_tmp)
 
        # Make the training and validation splits
        rng = np.random.RandomState(self.random_state)
        train_split = rng.rand(len(x_tmp)) > self.validation_split
        train_X = mol_dicts[train_split]
        train_y = y[train_split]
        valid_X = mol_dicts[~train_split]
        valid_y = y[~train_split]
        print("train_y: ", train_y)

        if (len(current_dataset) < self.batch_size):
           result = [str(model_id) + "$"+ "haaah" + "$" + str(model_id) + "$" + "haaah"]
           #print("result_list", result)
           return result

        if len(train_X) == 0 or len(valid_X) == 0:
           print(f"Skipping training: train_X={len(train_X)}, valid_X={len(valid_X)} after split")
           result = [str(model_id) + "$"+ "haaah" + "$" + str(model_id) + "$" + "haaah"]
           return result

        # Make the loaders
        # Use max(1, ...) so steps_per_epoch is never 0 when sample count < batch_size
        train_batch = min(self.batch_size, len(train_X))
        valid_batch = min(self.batch_size, len(valid_X))
        steps_per_epoch = 1
        train_loader = make_data_loader(train_X, train_y, repeat=True, batch_size=train_batch, max_size=max_size, drop_last_batch=False, shuffle_buffer=32768)
        valid_steps = 1
        valid_loader = make_data_loader(valid_X, valid_y, batch_size=valid_batch, max_size=max_size, drop_last_batch=False)

        # Reuse model loaded once in open() — avoids weight mismatch from clear_session()
        # resetting TF layer state between iterations.
        model = self.model
        infra_json_str = self.infra_json_str

        # Restore weights from previous training iteration
        if self.model_paras is not None:
            weights_list = json.loads(self.model_paras)
            weights = [np.array(arr) for arr in weights_list]
            model.set_weights(weights)

        try:
            scaler_layer = model.get_layer('scale')
            outputs = np.array(y_tmp)
            scaler_layer.set_weights([outputs.std()[None, None], outputs.mean()[None]])
        except ValueError:
            pass

        #print("Finish model loading")

        # Configure the LR schedule
        init_learn_rate = self.learning_rate
        #final_learn_rate = init_learn_rate * 1e-3
        #decay_rate = (final_learn_rate / init_learn_rate) ** (1. / (self.num_epochs - 1))

        #def lr_schedule(epoch, lr):
        #    return lr * decay_rate

        # Compile the model then train
        model.compile(
            tf.optimizers.Adam(init_learn_rate),
            'mean_squared_error',
            metrics=['mean_absolute_error'],
            steps_per_execution=1
        )
        
        #print("Start model fit")
        history = model.fit(
            train_loader,
            epochs=self.num_epochs,
            shuffle=False,
            verbose=False,
            steps_per_epoch=steps_per_epoch,
            validation_data=valid_loader,
            validation_steps=valid_steps,
            validation_freq=1
        )
        print("Finish model fit")

        #print("history keys: ", history.history.keys())
 
        # Load the loss value and MAE from the history object
        train_loss = history.history['loss']
        print("model_id: ", model_id, " train_loss: ", train_loss)
        #test_loss = history.history['val_loss']
        #print("model_id: ", model_id, " train_loss: ", test_loss)
        train_mae = history.history['mean_absolute_error']
        print("model_id: ", model_id, " train_mae: ", train_mae)
        #test_mae = history.history['val_mean_absolute_error']
        #print("model_id: ", model_id, " test_mae: ", test_mae)


        # Convert weights to numpy arrays (avoids mmap issues)
        weights = []
        for v in model.get_weights():
            v = np.array(v)
            if np.isnan(v).any():
                raise ValueError('Found some NaN weights.')
            weights.append(v.tolist())
        
        #print("weights length: ", len(model.get_weights()), len(weights))
        # Save model parameters
        #infra_json_str = model.to_json()
        weights_json_str = json.dumps(weights)
        # Update model parameters state
        self.model_paras = weights_json_str
        chunk_id = random.choice(range(2231))
        result = [str(chunk_id) + "$"+ weights_json_str + "$" + str(model_id) + "$" + infra_json_str]
        return result
              
