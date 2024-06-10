import argparse
import os
import yaml
import tensorflow as tf
from datetime import datetime, timedelta
from glob import glob

from data_prep.dataHander import RankDataHandler
from models.ctr_model import CTRModel
from models.inputs import DenseFeature, SparseFeature, SequenceFeature, TagFeature

class RankModelTrainer:
    """ A class for training and managing ranking models.
    
    Attributes:
        data_handler (RankDataHandler): Handles data operations like reading and preprocessing.
        model (tf.keras.Model): The TensorFlow Keras model to be trained and used.
        model_name (str): The name of the model, used for saving/loading.
        current_date (datetime): The reference date used for data partitioning.
        num_partitions (int): Number of data partitions to consider for training.
        label_name (str): The name of the label column in the dataset.
        model_path (str): Directory path where the model and its versions are stored.
    """
    def __init__(self, table_name, model, model_name, current_date, num_partitions, label_name):
        """ Initialize the model trainer with configuration parameters.
        
        Parameters:
            table_name (str): Name of the table in the database from which to pull data.
            model (tf.keras.Model): Compiled TensorFlow Keras model.
            model_name (str): Name identifier for the model.
            current_date (str or int): A date in 'YYYYMMDD' format or an integer representing days back from today.
            num_partitions (int): Number of past data partitions to use for training.
            label_name (str): Name of the target variable in the dataset.
        """
        self.data_handler = RankDataHandler(table_name)
        self.model = model
        self.model_name = model_name
        self.current_date = datetime.strptime(current_date, '%Y%m%d') if isinstance(current_date, str) else datetime.now() - timedelta(days=current_date)
        self.num_partitions = num_partitions
        self.label_name = label_name
        self.model_path = f"/data/{model_name}"

    def _generate_partition_specs(self, is_train=True):
        """ Generate partition specifications for data retrieval based on the training phase.

        Parameters:
            is_train (bool): Flag indicating whether the data is for training or testing.
        
        Returns:
            list[str]: A list of partition specifiers in 'ds=YYYYMMDD' format.
        """
        if is_train:
            return [f"ds={(self.current_date - timedelta(days=x)).strftime('%Y%m%d')}" for x in range(self.num_partitions)]
        else:
            return [f"ds={self.current_date.strftime('%Y%m%d')}"]

    def read_data(self, batch_size, is_train=True):
        """ Read data from the data handler based on partition specifications and training mode.
        
        Parameters:
            batch_size (int): The number of samples per batch.
            is_train (bool): Whether to retrieve training data or testing data.
        
        Returns:
            Data generator: Yields batches of (features, labels) suitable for model training/testing.
        """
        partition_specs = self._generate_partition_specs(is_train)
        return self.data_handler.read_data(partition_specs, self.model.inputs, [self.label_name], batch_size)

    def fit(self, epochs, batch_size, learning_rate, verbose=1, evaluate=False):
        """ fit the model using data read from partitions.
        
        Parameters:
            epochs (int): Number of epochs to train the model.
            batch_size (int): Number of samples per batch.
            learning_rate (float): The learning rate for the optimizer.
            verbose (int): Verbosity mode (0, 1, or 2).
            evaluate (bool): Flag to determine if the model should be evaluated after training.
        """
        train_data = self.read_data(batch_size, is_train=True)
        self.model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=learning_rate), loss='binary_crossentropy', metrics=['AUC'])
        checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(filepath=os.path.join(self.model_path, "ckpt_{epoch}"), save_weights_only=True)
        test_data = self.read_data(batch_size, is_train=False) if evaluate else None
        self.model.fit(train_data, epochs=epochs, validation_data=test_data, verbose=verbose, callbacks=[checkpoint_callback])

    def evaluate(self, batch_size):
        """ Evaluate the model on the test data.
        
        Parameters:
            batch_size (int): Number of samples per batch used for evaluation.
        
        Returns:
            float: The loss and metric values for the evaluation.
        """
        test_data = self.read_data(batch_size, is_train=False)
        return self.model.evaluate(test_data)

    def save(self, version):
        """ Save the model and clean up old versions.
        
        Parameters:
            version (str): Version identifier for the model, typically a date in 'YYYYMMDD' format.
        """
        if not os.path.exists(self.model_path):
            os.makedirs(self.model_path)
        self.model.save(os.path.join(self.model_path, version))
        self._cleanup_old_versions()

    def _cleanup_old_versions(self, days_to_keep=30):
        """ Remove older model versions to free up space.
        
        Parameters:
            days_to_keep (int): The number of days to retain saved model versions.
        """
        now = datetime.now()
        for folder in os.listdir(self.model_path):
            folder_date = datetime.strptime(folder, '%Y%m%d')
            if (now - folder_date).days > days_to_keep:
                os.system(f"rm -rf {os.path.join(self.model_path, folder)}")

    def load(self):
        """ Load the latest model version. """
        latest_version = max(glob(f"{self.model_path}/*"), key=os.path.getmtime)
        print(latest_version)
        self.model = tf.keras.models.load_model(latest_version)


def parse_feature_config(feature_configs):
    feature_objects = []
    for feature_config in feature_configs:
        feature_type = feature_config.pop('type')
        if feature_type == 'DenseFeature':
            feature_objects.append(DenseFeature(**feature_config))
        elif feature_type == 'SparseFeature':
            feature_objects.append(SparseFeature(**feature_config))
        elif feature_type == 'SequenceFeature':
            feature_objects.append(SequenceFeature(**feature_config))
        elif feature_type == 'TagFeature':
            feature_objects.append(TagFeature(**feature_config))
        else:
            raise ValueError(f"Unsupported feature type: {feature_type}")
    return feature_objects

def load_config(config_file):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return config

def build_ctr_model_from_config(config_file):
    config = load_config(config_file)
    features = parse_feature_config(config['features'])
    model_config = config['model']
    print(model_config)
    model = CTRModel(
        features=features,
        dnn_config=model_config['dnn_config'],
        l2_reg_embedding=model_config.get('l2_reg_embedding', 0),  # Default L2 regularization if not specified
        tag_pooling=model_config.get('tag_pooling', 'sum'),  # Default tag pooling if not specified
        seq_pooling=model_config.get('seq_pooling', None),  # Default sequence pooling if not specified
        fm_config=model_config['fm_config'],
        din_config=model_config['din_config']
    )
    return model.model

def parse_arguments():
    parser = argparse.ArgumentParser(description='Train a CTR (Click-Through Rate) prediction model based on provided configurations.')
    parser.add_argument('--config_file', type=str, required=True, help='Path to the configuration file. The configuration file should be in YAML format and contains all necessary details like model architecture, data paths, and training parameters.')
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_arguments()
    model = build_ctr_model_from_config(args.config_file)
    print(model.summary())

    train_config = load_config(args.config_file)['train']
    trainer = RankModelTrainer(table_name=train_config['table_name'],
                               model=model,
                               model_name=train_config['model_name'],
                               current_date=train_config.get('current_date', 0),  # Default to today if not specified
                               num_partitions=train_config['num_partitions'],
                               label_name=train_config['label_name'])
    if train_config['incremental_training']:
        trainer.load()

    trainer.fit(epoch=train_config['epoch'],
                  batch_size=train_config['batch_size'],
                  learning_rate=train_config['learning_rate'],
                  verbose=train_config['verbose'],
                  evaluate=True)
    trainer.save(version=datetime.now().strftime('%Y%m%d'))