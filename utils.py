import pandas as pd
from google.cloud import storage
from io import BytesIO, StringIO

def load_mapping(bucket_name, file_name):
    """
    Load mapping file from GCS (.csv, .txt, or .xlsx)
    Returns pandas DataFrame
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_bytes()

    if file_name.endswith(".xlsx"):
        df = pd.read_excel(BytesIO(content))
    elif file_name.endswith(".csv") or file_name.endswith(".txt"):
        try:
            df = pd.read_csv(StringIO(content.decode("utf-8")), sep="\t")
        except Exception:
            df = pd.read_csv(StringIO(content.decode("utf-8")), sep=",")
    else:
        raise ValueError(f"Unsupported mapping file format: {file_name}")
    
    return df
