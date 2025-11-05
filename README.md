# DataEngineerProject - David Watson, 2025

Project Overview:

This project demonstrates an end-to-end data engineering pipeline on Google Cloud Platform (GCP) using Cloud Storage, Dataproc (PySpark), Composer (Airflow), BigQuery, and Power BI.
The pipeline ingests, transforms, and visualizes a large COVID-19 dataset to uncover trends in confirmed cases in the USA over time.

Workflow Overview:

1. Raw Data (.csv in GCS)
2. Cloud Composer (Airflow DAG)
3. Dataproc (PySpark transformation)
4. Cleansed data (Parquet to GCS to BigQuery)
5. Power BI (Dashboard visualization)

Components:

1. Data Ingestion
   - Raw data (RAW_us_confirmed_cases.csv) is stored in a Google Cloud Storage bucket (gs://covid-data-pipeline-101/raw)
   - Composer (Airflow) orchestrates ingestion and transformation via a scheduled DAG
  
2. Orchestration
   - A daily-scheduled Airflow DAG (covid_data_refresh_dag.py) runs the Dataproc job automatically
   - DAG schedule: @daily
   - Key operator: DataprocSubmitJobOperator
   - Parameters include project ID, cluster name, and input/output GCS paths

3. Data Transformation (Dataproc + PySpark)
   - A Dataproc cluster executes a PySpark script (transform_covid_cases.py) stored in GCS.
   - The script:
       - Reads the wide-format dataset from GCS
       - Unpivots date columns into a normalized time-series format
       - Cleanses and aggregates daily confirmed case totals for the USA
       - Writes the results as Parquet files to GCS: gs://covid-data-pipeline-101/processed/covid-cases/usa_daily/

4. Data Storage (BigQuery - Automated load)
   - After transformation, the Composer DAG automatically imports the Parquet output into BigQuery using GCSToBigQueryOperator
   - Table created: covid_data_usa.daily_confirmed
   - Write mode: WRITE_TRUNCATE (replaces the table on each run for a full refresh)
   - Ensures BigQuery always reflects the lastest processed data with no manual intervention
  
5. Visualization (Power BI)
   - Power BI Desktop connects to BigQuery using the official connector
   - Interactive visual of total confirmed cases in the USA, as a time-series line chart
