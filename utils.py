import yaml
from google.cloud import storage

def load_mapping(bucket_name: str, file_path: str) -> str:
    """Load mapping text file either from GCS or local."""
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
    """
    Retrieve configuration for a specific control type and product.
    Falls back to 'default' if product-specific config not found.
    """
    try:
        controls = config_data.get("controls", {})
        control_section = controls.get(control_type, {})

        if product_name in control_section:
            return control_section[product_name]

        # Fallback to default config
        if "default" in control_section:
            return control_section["default"]

        # No config found
        available_products = list(control_section.keys())
        raise KeyError(
            f"No configuration found for '{control_type}' → '{product_name}'. "
            f"Available products: {available_products}"
        )

    except Exception as e:
        raise ValueError(f"Error reading control mapping: {e}")
