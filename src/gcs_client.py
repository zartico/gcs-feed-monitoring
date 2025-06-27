# import packages
from google.cloud import storage
from datetime import datetime
from src.utils import extract_actual_date

def list_gcs_metadata(bucket_name, prefix = "", debug = False): 
    """
    List files in a Google Cloud Storage bucket with metadata.
    
    Args:
        bucket_name (str): The name of the GCS bucket.
        prefix (str): Optional prefix to filter files.
        debug (bool): If True, print debug information.

    Returns:
        list: A list of dictionaries containing file metadata.
    """
    storage_client = storage.Client()

    file_list = storage_client.list_blobs(bucket_name, prefix = prefix)
    
    metadata = []
    for file in file_list:

        #Skip virtual folders for now (possibly flag in the future)
        if "$folder$" in file.name:
            continue

        actual_date = extract_actual_date(file.name)
        #print(f"[DEBUG] Checking file: {file.name}")
        if debug and actual_date is None:
            print(f"[DEBUG] Could not extract date from: {file.name}")

        metadata.append({
            "name": file.name,
            "size": file.size,
            "updated": file.updated,
            "actual_date": actual_date,
        })

    return metadata

def group_by_date(metadata):
    """ 
    Group file metadata by the date they were last updated.
    ** Not In Use **

    Args:
        metadata (list): List of file metadata dictionaries.

    Returns:
        dict: A dictionary where keys are dates and values are lists of file metadata.
    """
    grouped = {}
    for item in metadata:
        date_key = item["updated"].strftime("%Y-%m-%d")
        if date_key not in grouped:
            grouped[date_key] = []
        grouped[date_key].append(item)
    
    return grouped
