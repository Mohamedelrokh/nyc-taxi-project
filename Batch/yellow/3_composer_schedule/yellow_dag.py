from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dataflow_job_every_hour",
    default_args=default_args,
    description="Run Dataflow job every hour",
    schedule_interval="0 * * * *",  # Runs at the start of every hour
    catchup=False,
)

dataflow_task = DataflowCreatePythonJobOperator(
    task_id="run_dataflow_job",
    py_file="gs://yellow-trips/scripts/process_data7.py",  # Ensure script is in GCS
    job_name="process",
    options={
        "runner": "DataflowRunner",
        "project": "di-malrokh-sandbox-malrokh",
        "region": "us-central1",
        "staging_location": "gs://yellow-trips/staging/",
        "temp_location": "gs://yellow-trips/temp/",
    },
    location="us-central1",
    dag=dag,
)
