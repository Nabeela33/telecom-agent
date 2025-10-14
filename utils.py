from google.cloud import storage
import pandas as pd

def load_mapping(bucket_name, file_name):
    """
    Load mapping file (Excel or CSV) from GCS
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data_bytes = blob.download_as_bytes()
    
    if file_name.lower().endswith(".xlsx") or file_name.lower().endswith(".xls"):
        df = pd.read_excel(data_bytes)
    elif file_name.lower().endswith(".csv"):
        df = pd.read_csv(pd.io.common.BytesIO(data_bytes))
    else:
        raise ValueError("Unsupported mapping file format")
    
    return df
