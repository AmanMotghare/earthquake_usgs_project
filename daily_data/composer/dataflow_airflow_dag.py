from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType

default_args = {
    'owner': 'Airflow_DF',
    'retries': 0,
    'retry_delay': timedelta(seconds=50),
}

dag = DAG(
    dag_id='DF_earthquake_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 11, 16),
    catchup=False,
    description="DAG for data ingestion and transformation and update. (USING DATAFLOW)"
)

dataflow_task = BeamRunPythonPipelineOperator(
    task_id="earthquake_data_etl_dataflow",
    dag=dag,
    gcp_conn_id="google_cloud_default",
    runner=BeamRunnerType.DataflowRunner,
    py_file="gs://earthquake_usgs_bucket/daily_data/codes/all_day_dataFlow.py",
    pipeline_options={
        'project': 'earthquake-usgs-project',
        'region': 'asia-east1',
        'runner': 'DataflowRunner'
    },
)

dataflow_task
