from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.main import run  # or however it's imported

def pipeline_wrapper(**kwargs):
    slack_webhook = kwargs.get('slack_webhook', None)
    run(slack_webhook=slack_webhook)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='gcs_feed_monitoring',
    default_args=default_args,
    start_date=datetime(2025, 6, 25),
    schedule_interval='0 22 * * *',  # Every day at 10PM
    catchup=False,
    tags=['sandbox', 'monitoring'],
) as dag:

    run_pipeline = PythonOperator(
        task_id='run_gcs_feed_monitoring',
        python_callable=pipeline_wrapper,
        op_kwargs={'slack_webhook': None},  # Pulls from env if None
    )