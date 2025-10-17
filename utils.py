from google.cloud import storage
import pandas as pd
import io

def load_mapping(bucket_name: str, file_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    content = blob.download_as_bytes()
    
    if file_name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(content))
    elif file_name.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(content))
    elif file_name.endswith(".txt"):
        # Simple text file to DataFrame with one column
        lines = content.decode("utf-8").splitlines()
        return pd.DataFrame(lines, columns=["column"])
    else:
        raise ValueError("Unsupported mapping file format")
