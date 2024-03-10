import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense

from .layers import SequencePoolingLayer, AttentionSequencePoolingLayer, FM, DNN, PredictionLayer
from .layers.utils import concat_func

from .inputs import DenseFeature, SparseFeature, SequenceFeature, TagFeature, \
    build_inputs, create_embedding_layer, create_hashing_layer, get_feature_embeddings

class CTRModel():
    """
    CTRModel encapsulates a Click-Through Rate (CTR) prediction model with
    various feature transformations including pooling, attention, and factorization machines.

    Attributes:
        features (list): List of feature configurations (DenseFeature, SparseFeature, etc.).
        
        dnn_config (dict): Configuration for the DNN layer with the following keys:
            - 'hidden_units' (tuple): Tuple of integers specifying the number of units in each DNN layer.
            - 'activation' (str): Activation function to use in the DNN layers.
            - 'l2_reg' (float): L2 regularization factor.
            - 'dropout_rate' (float): Dropout rate for the DNN layers.
            - 'use_bn' (bool): Whether to use batch normalization.
            - 'output_activation' (str): Activation function for the DNN output layer.
        l2_reg_embedding (float): Regularization factor for the embeddings.
        tag_pooling (str): Pooling mode for TagFeature (e.g., 'sum', 'mean', 'max').
        seq_pooling (str): Pooling mode for SequenceFeature (e.g., 'sum', 'mean', 'max').
        fm_config (dict): Configuration for the FM layer with the following keys:
            - 'use_sparsefeat' (bool): Whether to use sparse features for FM.
            - 'feature_names' (list): List of feature names to include in FM.
            - 'embedding_dim' (int): Size of the embedding vectors.
        din_config (dict): Configuration for the DIN attention layer with the following keys:
            - 'att_hidden_units' (tuple): Tuple of integers specifying the number of units in each attention layer.
            - 'att_activation' (str): Activation function for the attention layers.
            - 'weight_normalization' (bool): Whether to use weight normalization in the attention layers.
            - 'return_score' (bool): Whether to return the attention score.
    """
    def __init__(self, features, dnn_config, l2_reg_embedding=0, tag_pooling='sum', seq_pooling=None, fm_config=None, din_config=None):
        self.features = features
        self.dnn_config = dnn_config
        self.l2_reg_embedding = l2_reg_embedding
        self.tag_pooling = tag_pooling
        self.seq_pooling = seq_pooling
        self.fm_config = fm_config
        self.din_config = din_config
        
        # Create input layers
        self.inputs = build_inputs(self.features)
        
        # Create Embedding and Hashing layers
        self.embedding_layers = create_embedding_layer(self.features, l2_reg=l2_reg_embedding)
        self.hashing_layers = create_hashing_layer(self.features)
        
        # Get feature embeddings
        self.embeddings = get_feature_embeddings(self.features, self.inputs, self.embedding_layers, self.hashing_layers)

        self.build_model()

    def combine_features(self):
        features_dict = {}
        
        # Combine Sparse, Dense, and Pooled features in a single pass
        for feat in self.features:
            if isinstance(feat, (SparseFeature, DenseFeature)):
                features_dict[feat.name] = self.embeddings[feat.name]
            else:
                pooling_mode = self.tag_pooling if isinstance(feat, TagFeature) else self.seq_pooling
                if pooling_mode:
                    sequence_pooling_layer = SequencePoolingLayer(mode=pooling_mode, supports_masking=feat.length_name is None)
                    if feat.length_name:
                        pooled_output = sequence_pooling_layer([self.embeddings[feat.name], self.inputs[feat.length_name]])
                    else:
                        pooled_output = sequence_pooling_layer(self.embeddings[feat.name])
                    features_dict[feat.name] = pooled_output

        # Apply FM and DIN if configured
        if self.fm_config:
            features_dict['fm_output'] = self.get_fm_output()

        if self.din_config:
            features_dict['din_output'] = self.get_din_attention_output()

        # Flatten the dictionary into a list
        combined_features = [feature for feature in features_dict.values()]

        return combined_features

    def get_din_attention_output(self):
        group_features = {}
        for feat in self.features:
            if isinstance(feat, SequenceFeature):
                group_name = feat.group_name
                group_features.setdefault(group_name, []).append(feat)

        din_outputs = []
        for group_name, feats in group_features.items():
            attention_sequence_pooling_layer = AttentionSequencePoolingLayer(att_hidden_units=self.din_config['att_hidden_units'],
                                                                             att_activation=self.din_config['att_activation'], 
                                                                             weight_normalization=self.din_config['weight_normalization'],
                                                                             return_score=self.din_config['return_score'])
            
            keys = concat_func([self.embeddings[feat.sparse_feature_name] for feat in feats])
            query = concat_func([self.embeddings[feat.sparse_feature_name] for feat in feats])
            
            if feats[0].length_name:
                keys_length = self.inputs[feats[0].length_name]
                din_output = attention_sequence_pooling_layer([query, keys, keys_length])
            else:
                attention_sequence_pooling_layer.supports_masking = True
                din_output = attention_sequence_pooling_layer([query, keys])
            
            din_outputs.append(din_output)
        return concat_func(din_outputs, axis=-1)

    def get_fm_output(self):
        # Adjust the dimension of embeddings for FM module if necessary
        fm_inputs = []
        for feat in self.features:
            if (self.fm_config['use_sparsefeat'] and isinstance(feat, SparseFeature)) or (feat.name in self.fm_config['feature_names']):
                embedding = self.embeddings[feat.name]
                # For the FM module, ensure all embeddings have the same dimension
                if feat.embedding_dim != self.fm_config['embedding_dim']:
                    embedding = Dense(self.fm_config['embedding_dim'], use_bias=False)(embedding)
                fm_inputs.append(embedding)

        fm_output = FM()(concat_func(fm_inputs, axis=1))
        fm_output = tf.expand_dims(fm_output, axis=1)
        return fm_output
    
    def build_model(self):
        # Combine features
        combined_features = self.combine_features()
        
        # Apply DNN
        self.dnn_layer = DNN(hidden_units=self.dnn_config['hidden_units'],
                              activation=self.dnn_config['activation'],
                              l2_reg=self.dnn_config['l2_reg'],
                              dropout_rate=self.dnn_config['dropout_rate'],
                              use_bn=self.dnn_config['use_bn'],
                              output_activation=self.dnn_config['output_activation'])
        
        dnn_out = self.dnn_layer(concat_func(combined_features))
        
        logits = Dense(1, use_bias=False)(dnn_out)

        pctr = PredictionLayer(task='binary', name='pctr')(logits)

        self.model = Model(inputs=self.inputs, outputs=pctr)