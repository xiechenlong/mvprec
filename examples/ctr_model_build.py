import yaml
from models.ctr_model import CTRModel
from models.inputs import DenseFeature, SparseFeature, SequenceFeature, TagFeature

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

if __name__ == '__main__':
    config_file_path = 'examples/ctr_config.yaml'
    ctr_model = build_ctr_model_from_config(config_file_path)
    print(ctr_model.summary())