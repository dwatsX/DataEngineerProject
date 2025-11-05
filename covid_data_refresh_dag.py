from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# DAG Configuration

PROJECT_ID = "sr-data-engineer-project"
REGION = "us-central1"
CLUSTER_NAME = "cluster-2c27"

# Paths
PYSPARK_URI = "gs://covid-data-pipeline-101/dataproc/jobs/transform_covid_cases.py"
INPUT_PATH = "gs://covid-data-pipeline-101/raw/RAW_us_confirmed_cases.csv"
OUTPUT_PATH = "gs://covid-data-pipeline-101/processed/covid_cases/"
BQ_DATASET = "covid_data_usa"
BQ_CONFIRMED_TABLE = "daily_confirmed"

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
    description="DAG to orchestrate daily Dataproc job for COVID dataset refresh and load into BigQuery",
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
    
    # Load country-level data into BigQuery
    load_country_to_bq = GCSToBigQueryOperator(
        task_id="load_country_to_bq",
        bucket="covid-data-pipeline-101",
        source_objects=["processed/covid_cases/usa_daily/*.parquet"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{BQ_CONFIRMED_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )    

    # Run workflow
    run_dataproc >> load_country_to_bq
