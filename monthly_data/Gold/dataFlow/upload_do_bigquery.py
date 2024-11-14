import json
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

# Setting up GCP Credentials
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = (r"D:\BrainWorks\Pyspark "
                                         r"Assignments\earthquake_ingestion\gcs_service_account_key\earthquake-usgs"
                                         r"-project-0d36682ec064.json")

# Setting up the parameters for writing to BigQuery.
table = 'earthquake-usgs-project.earthquake_db.earthquake_data_dataflow'
dataset = 'earthquake-usgs-project.earthquake_db'
project = 'earthquake-usgs-project'
staging_location_path = f"gs://dataproc-staging-bucket-earthquake/Dataflow/gold/staging_location/"
temp_location_path = f"gs://dataproc-staging-bucket-earthquake/Dataflow/gold/temp_location/"

# Setting up Google Cloud Options
options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project
google_cloud_options.job_name = 'write-cleaned-data-to-bigquery'
google_cloud_options.region = 'asia-east1'
google_cloud_options.staging_location = staging_location_path
google_cloud_options.temp_location = temp_location_path

# Set to Dataflow runner
options.view_as(StandardOptions).runner = 'DataflowRunner'


class PrintType(beam.DoFn):
    def process(self, record, *args, **kwargs):
        print(f"Data type: {type(record)}")  # Initial type should be <class 'str'>
        yield record


class addTimeStampColumn(beam.DoFn):
    def process(self, record, *args, **kwargs):
        from datetime import date, datetime

        insert_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record = json.loads(record)  # (str to dict)
        record['insert_date'] = insert_date
        yield record


schema = {
    "fields": [
        {"name": 'mag', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {"name": 'place', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'time', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {"name": 'updated', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {"name": 'tz', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {"name": 'url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'detail', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'felt', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {"name": 'cdi', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {"name": 'mmi', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {"name": 'alert', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'tsunami', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {"name": 'sig', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {"name": 'net', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'ids', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'sources', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'types', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'nst', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {"name": 'dmin', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {"name": 'rms', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {"name": 'gap', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {"name": 'magType', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'geometry_longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {"name": 'geometry_latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {"name": 'geometry_depth', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {"name": 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {"name": 'insert_date', 'type': 'DATETIME', 'mode': 'NULLABLE'}
    ]
}

with beam.Pipeline(options=options) as p:
    bucketFilePath = r"gs://earthquake_usgs_bucket/Dataflow/silver/241111/earthquake_monthly_data-00000-of-00001.json"

    # READ FILE FROM GCP BUCKET
    readFromBucket = (
            p | "read from Bucket" >> beam.io.ReadFromText(bucketFilePath)
            | "Add Column" >> beam.ParDo(addTimeStampColumn())
    )

    # WRITE FILE TO BIGQUERY
    writeToBigQuery = (
            readFromBucket | "Write To BigQuery" >> beam.io.gcp.bigquery.WriteToBigQuery(
        table=table,
        schema=schema,
        project=project,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )
