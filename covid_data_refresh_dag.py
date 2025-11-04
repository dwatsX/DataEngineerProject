from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# DAG Configuration

PROJECT_ID = "sr-data-engineer-project"
REGION = "us-central1"
CLUSTER_NAME = "cluster-2c27"
PYSPARK_URI = "gs://covid-data-pipeline-101/dataproc/jobs/transform_covid_cases.py"

INPUT_PATH = "gs://covid-data-pipeline-101/raw/RAW_us_confirmed_cases.csv"
OUTPUT_PATH = "gs://covid-data-pipeline-101/processed/covid_cases/"

# Define default arguments for the DAG

default_args = {
    "owner": "david_watson",
    "depends_on_past": False,
    "email": ["watsodave@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG

with DAG(
    dag_id="covid_data_refresh",
    default_args=default_args,
    description="DAG to orchestrate daily Dataproc job for COVID dataset refresh",
    schedule_interval="@daily", # runs every day
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["dataproc", "covid", "batch", "etl"],
) as dag:

    # Submit the PySpark job to Dataproc

    dataproc_job = {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_URI,
            "args": [
                f"--input={INPUT_PATH}",
                f"--output={OUTPUT_PATH}",
            ],
        },
    }

    run_dataproc = DataprocSubmitJobOperator(
        task_id="run_covid_transform",
        job=dataproc_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Run workflow
    run_dataproc