from google.cloud import storage
import pandas as pd
import io

def load_mapping(bucket_name, file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    content = blob.download_as_text()

    if file_name.endswith(".csv"):
        return pd.read_csv(io.StringIO(content))
    elif file_name.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(blob.download_as_bytes()))
    elif file_name.endswith(".txt"):
        # Each line as a row
        lines = content.splitlines()
        return pd.DataFrame({"column": lines})
    else:
        raise ValueError("Unsupported mapping file format")
