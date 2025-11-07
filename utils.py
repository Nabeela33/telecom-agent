import yaml
from google.cloud import storage

def load_mapping(bucket_name: str, file_name: str) -> str:
    if "/" in bucket_name:
        bucket_name, prefix = bucket_name.split("/", 1)
        blob_name = f"{prefix}/{file_name}"
    else:
        blob_name = file_name
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_text()

def load_yaml_config(bucket_name, file_path):
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
