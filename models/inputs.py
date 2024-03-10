import tensorflow as tf
from tensorflow.keras.regularizers import l2
from collections import OrderedDict

class DenseFeature:
    """Represents a dense (continuous-valued) feature.

    Args:
        name (str): The name of the feature.
        dimension (int): The dimensionality of the feature. Defaults to 1.
        dtype (str): The data type of the feature. Defaults to "float32".
    """
    def __init__(self, name, dimension=1, dtype="float32"):
        self.name = name
        self.dimension = dimension
        self.dtype = dtype


class SparseFeature:
    """Represents a sparse (categorical) feature.

    Args:
        name (str): The name of the feature.
        vocabulary_size (int): The size of the feature's vocabulary or hash function count.
        embedding_dim (int): The dimensionality of the embedding. Defaults to 16.
        hash_count (int): The number of hashing functions. 0 if not used.
        dtype (str): The data type of the feature. Defaults to "int32".
        embedding_name (str): The name of the embedding variable. If None, defaults to the feature name.
        mask_zero (bool): Indicates whether the value 0 is a padding value.
    """
    def __init__(self, name, vocabulary_size, embedding_dim=16, hash_count=0, dtype="int32", embedding_name=None, mask_zero=False):
        self.name = name
        self.vocabulary_size = vocabulary_size
        self.embedding_dim = embedding_dim
        self.hash_count = hash_count
        self.dtype = dtype
        self.embedding_name = embedding_name or name
        self.mask_zero = mask_zero


class SequenceFeature:
    """Represents a sequence feature where each element is a categorical variable.

    Args:
        name (str): The name of the feature.
        sparse_feature_name (str): The name of the corresponding sparse feature.
        length_name (str): The name of the feature that represents the length of the sequence.
        maxlen (int): The maximum length of the sequence.
        group_name (str): The name of the group this feature belongs to. Defaults to "default".
    """
    def __init__(self, name, sparse_feature_name, length_name, maxlen, group_name="default"):
        self.name = name
        self.sparse_feature_name = sparse_feature_name
        self.length_name = length_name
        self.maxlen = maxlen
        self.group_name = group_name


class TagFeature(SparseFeature):
    """Represents a tag feature, similar to a sparse feature but for multiple tags.

    Args:
        name (str): The name of the feature.
        vocabulary_size (int): The size of the feature's vocabulary or hash function count.
        embedding_dim (int): The dimensionality of the embedding. Defaults to 16.
        hash_count (int): Number of times to hash the feature. 0 means no hashing.
        length_name (str): The name of the feature that represents the length of the sequence.
        maxlen (int): The maximum length of the tag sequence.
        dtype (str): The data type of the feature. Defaults to "int32".
        mask_zero (bool): Indicates whether the value 0 is a padding value.
    """
    def __init__(self, name, vocabulary_size, embedding_dim=16, hash_count=0, length_name=None, maxlen=None, dtype="int32", mask_zero=False):
        super().__init__(name, vocabulary_size, embedding_dim, hash_count, dtype, mask_zero)
        self.length_name = length_name
        self.maxlen = maxlen


def build_inputs(features, prefix=''):
    """Constructs a dictionary of TF Keras Input layers for different feature types.

    Args:
        features (list): A list of feature instances (DenseFeature, SparseFeature, SequenceFeature, TagFeature).
        prefix (str): A prefix to prepend to each input name.

    Returns:
        OrderedDict: An ordered dictionary mapping input names to their respective Keras Input layers.

    Raises:
        ValueError: If an unknown feature type is encountered or if SequenceFeature's dtype cannot be determined.
    """
    
    inputs = OrderedDict()
    feature_dict = {feature.name: feature for feature in features}  # Create a dict for easy lookup of features by name.

    for feature in features:
        key = f"{prefix}{feature.name}"

        # Create Input for DenseFeature.
        if isinstance(feature, DenseFeature):
            inputs[key] = tf.keras.Input(shape=(feature.dimension,), name=key, dtype=feature.dtype)
        
        # Create Input for SparseFeature.
        elif isinstance(feature, SparseFeature):
            inputs[key] = tf.keras.Input(shape=(1,), name=key, dtype=feature.dtype)

        # Create Input for SequenceFeature.
        elif isinstance(feature, SequenceFeature):
            # Retrieve dtype from the corresponding SparseFeature.
            sparse_feature = feature_dict.get(feature.sparse_feature_name)
            if not sparse_feature:
                raise ValueError(f"Sparse feature '{feature.sparse_feature_name}' not found for sequence feature '{feature.name}'.")
            inputs[key] = tf.keras.Input(shape=(feature.maxlen,), name=key, dtype=sparse_feature.dtype)

        # Create Input for TagFeature.
        elif isinstance(feature, TagFeature):
            # For TagFeature, we also need to specify the maximum sequence length.
            inputs[key] = tf.keras.Input(shape=(feature.maxlen,), name=key, dtype=feature.dtype)

        else:
            # Raise an error if the feature type is not recognized.
            raise ValueError(f"Unknown feature type for feature '{feature.name}'.")

    return inputs

def create_embedding_layer(features, prefix='', l2_reg=0):
    """Creates embedding layers for SparseFeature and TagFeature instances.
    
    For each feature, if multiple hashes are used (indicated by `hash_count`), 
    this function creates a separate embedding layer for each hash. Each embedding 
    layer is given a unique name that includes the hash index to ensure that the 
    embeddings for each hash are kept distinct.

    Args:
        features (list): A list of feature instances (SparseFeature, TagFeature).
        prefix (str): A prefix used in naming the embedding layers.
        l2_reg (float): L2 regularization factor.

    Returns:
        dict: A dictionary mapping feature names to their respective embedding layers with L2 regularization.
    """

    embedding_layers = {}

    for feature in features:
        if isinstance(feature, (SparseFeature, TagFeature)):
            # Define the embedding layer for the feature
            for i in range(feature.hash_count+1):
                embedding_layers[f"{feature.embedding_name}_{i}"] = tf.keras.layers.Embedding(
                    input_dim=feature.vocabulary_size,
                    output_dim=feature.embedding_dim,
                    embeddings_regularizer=l2(l2_reg),
                    mask_zero=feature.mask_zero,  # Only set this to True if you want to mask input sequences with value 0.
                    name=f"{prefix}emb_{feature.embedding_name}_{i}"
                )

    return embedding_layers

def create_hashing_layer(features, salt=None, prefix=''):
    """Creates multiple hashing layers for SparseFeature and TagFeature instances based on hash_count.
    
    If a feature requires hashing and specifies a hash_count greater than one, 
    this function will create multiple hashing layers for that feature, one for 
    each hash function. The hash functions will have different salts based on the 
    initial salt provided, to ensure that they produce different hash mappings. 
    Each hashing layer is named with a unique identifier that includes the hash index.

    Args:
        features (list): A list of feature instances (SparseFeature, TagFeature) that require hashing.
        salt (int): An optional initial hash seed.
        prefix (str): A prefix used in naming the hashing layers.

    Returns:
        dict: A dictionary mapping feature names (with hash index) to their respective hashing layers.
    """

    hashing_layers = {}

    for feature in features:
        if isinstance(feature, (SparseFeature, TagFeature)):
            # hash_count determines how many different hash functions to apply
            hash_count = feature.hash_count if feature.hash_count else 1
            for i in range(hash_count):
                # Define the hashing layer for the feature
                hashing_layers[f"{feature.embedding_name}_{i}"] = tf.keras.layers.Hashing(
                    num_bins=feature.vocabulary_size,
                    salt=salt + i if salt is not None else None,
                    mask_value=0 if feature.mask_zero else None,  # Use mask_value if mask_zero is True
                    name=f"{prefix}hash_{feature.embedding_name}_{i}"
                )

    return hashing_layers

def get_feature_embeddings(features, inputs, embedding_layers, hashing_layers):
    """Get embeddings for all inputs, applying hashing if needed.

    Args:
        features (list): A list of feature instances (DenseFeature, SparseFeature, SequenceFeature, TagFeature).
        inputs (dict): A dictionary of Input layers, with feature names as keys.
        embedding_layers (dict): A dictionary with feature names as keys and their respective Embedding layers.
        hashing_layers (dict): A dictionary with feature names as keys and their respective Hashing layers, if applicable.

    Returns:
        OrderedDict: An ordered dictionary mapping feature names to their corresponding embeddings or inputs.

    Note:
        - DenseFeature returns a tensor with shape (bs, 1, dimension).
        - SparseFeature returns a tensor with shape (bs, 1, embedding_dim).
        - SequenceFeature returns a tensor with shape (bs, maxlen, embedding_dim).
        - TagFeature returns a tensor with shape (bs, maxlen, embedding_dim).
    """
    
    embeddings = OrderedDict()
    feature_dict = {feature.name: feature for feature in features}

    for feature in features:
        input_layer = inputs[feature.name]

        if isinstance(feature, DenseFeature):
            embeddings[feature.name] = tf.expand_dims(input_layer, axis=1)
            continue

        # For SequenceFeature, use the corresponding SparseFeature's settings
        if isinstance(feature, SequenceFeature):
            corresponding_feature = feature_dict[feature.sparse_feature_name]
            feature.embedding_name = corresponding_feature.embedding_name
            feature.hash_count = corresponding_feature.hash_count
            feature.embedding_dim = corresponding_feature.embedding_dim

        # Apply hashing if needed and retrieve embeddings
        if feature.hash_count > 0:
            hashed_embeddings = [
                embedding_layers[f"{feature.embedding_name}_{i}"](hashing_layers[f"{feature.embedding_name}_{i}"](input_layer))
                for i in range(feature.hash_count)
            ]
            # If there are multiple hashes, sum the hashed embeddings
            embeddings[feature.name] = tf.keras.layers.Add()(hashed_embeddings) if feature.hash_count > 1 else hashed_embeddings[0]
        else:
            embeddings[feature.name] = embedding_layers[f"{feature.embedding_name}_0"](input_layer)

    return embeddings