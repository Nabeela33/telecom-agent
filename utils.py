from google.cloud import storage

def load_mapping(bucket_name: str, file_name: str) -> str:
    """Load text mapping file from GCS."""
    if "/" in bucket_name:
        bucket_name, prefix = bucket_name.split("/", 1)
        blob_name = f"{prefix}/{file_name}"
    else:
        blob_name = file_name

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_text()
