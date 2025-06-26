from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import date 
from config import PROJECT_ID, DATASET_ID, TABLE_ID

client = bigquery.Client(project = PROJECT_ID)

def ensure_dataset_and_table_exist():
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

    table_ref = ensure_dataset_and_table_exist()
    if table_ref is None:
        print("[WARN] Skipping BigQuery insert due to missing dataset.")
        return

    rows_to_insert = [{
        "date": event_date.isoformat(),
        "datafeed": feed_label,
        "filesize": file_size,
        "filecount": file_count,
    }]

    errors = client.insert_rows_json(table_ref, rows_to_insert)
    
    if errors:
        print(f"Error inserting rows: {errors}")
    else:
        print(f"Successfully upserted data for {feed_label} on {event_date}.")


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
    query = f"""
    SELECT AVG(filecount) AS avg_count, AVG(filesize) AS avg_size
    FROM `{table_ref}`
    WHERE datafeed = @feed_label AND date BETWEEN @start_date AND @end_date
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("feed_label", "STRING", feed_label),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat())
        ]
    )
    # query = """
    # SELECT AVG(filecount) AS avg_count, AVG(filesize) AS avg_size
    # FROM `prj-prod-gd-api-load-t51k.AziraMonitoring.raw_data_monitoring`
    # WHERE datafeed = 'Web Impressions' AND date BETWEEN '2025-05-20' AND '2025-06-19'
    # """
    # job_config = bigquery.QueryJobConfig()
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()
    #rows = list(result)
    #print("Rows:", rows)

    print("[DEBUG] Running historical baseline query with:")
    print("feed_label:", repr(feed_label), type(feed_label))
    print("start_date:", repr(start_date), type(start_date))
    print("end_date:", repr(end_date), type(end_date))  
    print(f"  feed_label: {feed_label}")
    print(f"  start_date: {start_date}")
    print(f"  end_date: {end_date}")
    print(f"  query: {query}")
    print(f"[DEBUG] Querying table: {table_ref}")


    #query_job = client.query(query, job_config=job_config)
    #result = query_job.result()
    row = next(result, None)
    if row:
        print(row[0], row[1])
        return row[0], row[1]
    else:
        print(f"[WARN] No historical data for {feed_label} between {start_date} and {end_date}")
        return 0.0, 0.0

