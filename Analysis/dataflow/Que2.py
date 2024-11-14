# Que 2 : Find the average magnitude by the region


import os
import pyarrow as pa
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

# Setting up GCP Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (r"D:\BrainWorks\Pyspark "
                                                r"Assignments\earthquake_ingestion\gcs_service_account_key"
                                                r"\earthquake-usgs-project-0d36682ec064.json")

project_name = f"earthquake-usgs-project"
dataset_name = f"earthquake_db"
table_name = f"earthquake_data_dataflow"
table_id = f"{project_name}.{dataset_name}.{table_name}"

# Setting up required options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_name
google_cloud_options.job_name = "bq-table-analysis-using-dataflow-que2"
google_cloud_options.region = "asia-east1"
google_cloud_options.temp_location = r"gs://earthquake_usgs_bucket/Dataflow/analysis/temp/"


# Setting up runner
options.view_as(StandardOptions).runner = "DataflowRunner"

query = f"""
SELECT 
    region, 
    avg(mag) as average_magnitude
FROM `{table_id}`
GROUP BY region
"""

outputFilePath = "gs://earthquake_usgs_bucket/Dataflow/analysis/analyzed_data/que2/que2"

parquet_schema = pa.schema([
    ("region", pa.string()),
    ("average_magnitude", pa.float64())
])

with beam.Pipeline(options=options) as p:
    readData = (
            p
            | "read an analyze the data from bq table" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)

    )

    writeToParquet = (
            readData
            | "write the outputFile to GCS in parquet Format" >> beam.io.WriteToParquet(outputFilePath,
                                                                                        schema=parquet_schema,
                                                                                        file_name_suffix=".parquet")
    )

    writeToJson = (
        readData
        | "write the outputFile to GCS in json Format" >> beam.io.WriteToText(outputFilePath,
                                                                              file_name_suffix=".json")
    )
