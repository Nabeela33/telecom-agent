import yaml
from google.cloud import storage

def load_mapping(bucket_name: str, file_path: str) -> str:
    """Load mapping text file either from GCS or locally."""
    if bucket_name:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        return blob.download_as_text()
    else:
        with open(file_path, "r") as f:
            return f.read()

def load_yaml_config(bucket_name, file_path):
    """Load YAML config from GCS or local filesystem."""
    if bucket_name:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        return yaml.safe_load(blob.download_as_text())
    else:
        with open(file_path, "r") as f:
            return yaml.safe_load(f)

def get_control_config(control_type, product_name, config_data):
    try:
        control_section = config_data["controls"].get(control_type, {})
        return control_section.get(product_name, {})
    except Exception as e:
        raise ValueError(f"Invalid configuration: {e}")
