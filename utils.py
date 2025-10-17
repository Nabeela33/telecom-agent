import pandas as pd
from google.cloud import storage
import io

def load_mapping(bucket_name, mapping_file):
    """Load mapping file (csv, xlsx, or txt) from GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(mapping_file)
    data = blob.download_as_bytes()

    if mapping_file.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(data))
    elif mapping_file.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(data))
    elif mapping_file.endswith(".txt"):
        df = pd.read_csv(io.BytesIO(data), sep="|", engine="python", header=None)
        df.columns = [f"col_{i}" for i in range(len(df.columns))]
    else:
        raise ValueError("Unsupported mapping file format")

    return df
