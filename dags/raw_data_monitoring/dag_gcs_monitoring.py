from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta
import sys
import pytz

# Directory path src to import run function
sys.path.append("/home/airflow/gcs/dags/raw_data_monitoring/src")

@dag(
    dag_id="gcs_feed_monitoring",
    schedule=None,
    start_date=datetime.now(pytz.timezone("US/Central")) - timedelta(days=10),
    catchup=False,
    tags=["raw-data-monitoring", "gcs", "slack"],
)
def GCSMonitoringDag():

    @task(task_id="run_monitoring_script")
    def run_function():
        from main import run  # Local import inside task context
        slack_webhook = Variable.get("slack_webhook", default_var=None)
        run(slack_webhook=slack_webhook)

    run_function()

dag = GCSMonitoringDag()