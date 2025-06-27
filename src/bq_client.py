from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import date 
from src.config import PROJECT_ID, DATASET_ID, TABLE_ID

client = bigquery.Client(project = PROJECT_ID)

def ensure_dataset_and_table_exist():
    """
    Ensure the BigQuery dataset and table exist, creating them if necessary. 
    """

    # Define dataset and table references
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)

    # Check/create dataset
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        print(f"[ERROR] Dataset {DATASET_ID} does not exist. Please ask your admin to create it.")
        return None  # Exit early

    # Check/create table
    try:
        client.get_table(table_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("datafeed", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("filesize", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("filecount", "INT64", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"[INFO] Created table {TABLE_ID}.")
    
    return table_ref

def upsert_feed_metrics(feed_label: str, event_date: date, file_count: int, file_size: float):
    """
    Upsert feed metrics into BigQuery table.
    
    Args:
        feed_label (str): The label of the feed (e.g., "Geolocation", "Web", "Media").
        event_date (date): The date of the event.
        file_count (int): The number of files processed.
        file_size (float): The total size of files in MB.
    """
    # Ensure dataset and table exist, if not no upsert will be performed
    table_ref = ensure_dataset_and_table_exist()
    if table_ref is None:
        print("[WARN] Skipping BigQuery insert due to missing dataset.")
        return
    
    # Prepare the MERGE query for both updating and inserting data
    query = f"""
    MERGE `{table_ref}` T
    USING (
      SELECT @event_date AS date,
             @feed_label AS datafeed,
             @file_count AS filecount,
             @file_size AS filesize
    ) S
    ON T.date = S.date AND T.datafeed = S.datafeed
    WHEN MATCHED THEN
      UPDATE SET
        T.filecount = S.filecount,
        T.filesize = S.filesize
    WHEN NOT MATCHED THEN
      INSERT (date, datafeed, filecount, filesize)
      VALUES (S.date, S.datafeed, S.filecount, S.filesize)
    """

    # Configure the query job with parameters
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("event_date", "DATE", event_date.isoformat()),
            bigquery.ScalarQueryParameter("feed_label", "STRING", feed_label),
            bigquery.ScalarQueryParameter("file_count", "INT64", file_count),
            bigquery.ScalarQueryParameter("file_size", "FLOAT64", file_size),
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Waits for job to complete
        print(f"[INFO] MERGE complete for {feed_label} on {event_date}")
    except Exception as e:
        print(f"[ERROR] BigQuery MERGE failed for {feed_label} on {event_date}: {e}")



def query_historical_baseline(feed_label: str, start_date: date, end_date: date):
    """
    Query historical baseline data for a specific feed label between start and end dates.
    
    Args:
        feed_label (str): The label of the feed to query.
        start_date (date): The start date for the query.
        end_date (date): The end date for the query.
    
    Returns:
        list: A list of dictionaries containing the historical data.
    """
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Query to calculate average file count and size for the specified feed label (start_date to end_date)
    query = f"""
    SELECT AVG(filecount) AS avg_count, AVG(filesize) AS avg_size
    FROM `{table_ref}`
    WHERE datafeed = @feed_label AND date BETWEEN @start_date AND @end_date
    """
    
    # Configure the query job with parameters
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("feed_label", "STRING", feed_label),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat())
        ]
    )

    query_job = client.query(query, job_config=job_config)
    result = query_job.result()

    row = next(result, None)
    if row:
        #print(row[0], row[1])
        return row[0], row[1] # average file count and size in MB
    else:
        print(f"[WARN] No historical data for {feed_label} between {start_date} and {end_date}")
        return 0.0, 0.0

