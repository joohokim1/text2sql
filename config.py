import yaml

_config = None

def load_config(file_path='application.yml'):
    global _config
    if _config is None:
        with open(file_path, 'r') as file:
            _config = yaml.safe_load(file)
    return _config

def get_config(prop):
    config = load_config()
    keys = prop.split('.')
    value = config
    try:
        for key in keys:
            value = value[key]
        return value
    except KeyError:
        raise KeyError(f"Property '{prop}' not found in the configuration.")
    
# bigquery_dataset = get_config('bigquery.dataset')

