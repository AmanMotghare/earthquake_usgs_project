from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

default_args = {
    'owner': 'aman',
    'retries': 1,
    'retry_delay': timedelta(seconds=50)
}

dag = DAG(
    dag_id="earthquake_usgs_dataproc",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 11, 16),
    catchup=False,
    description="Dag for data extraction, transformation and loading it to bigquery. (USING DATAPROC)"
)

CLUSTER_NAME = "bq-airflow-spark-cluster"
REGION = "asia-east1"
PROJECT_ID = "earthquake-usgs-project"

# Define Dataproc Job

spark_job = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://earthquake_usgs_bucket/daily_data/codes/all_day_datproc.py",
        "args": [
        ],
    }
}

data_cleaning_task = DataprocSubmitJobOperator(
    task_id="earthquake_data_etl_dataproc",
    job=spark_job,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag
)

data_cleaning_task
