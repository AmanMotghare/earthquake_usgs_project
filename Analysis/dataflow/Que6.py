# Que 5 : Find average earthquakes happen on the same day.

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

# Setting up google cloud Options
options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_name
google_cloud_options.region = "asia-east1"
google_cloud_options.job_name = "bq-table-analysis-using-dataflow-que6"
google_cloud_options.temp_location = r"gs://earthquake_usgs_bucket/Dataflow/analysis/temp/"

# Setting up runner
options.view_as(StandardOptions).runner = "DataflowRunner"

query = f"""
WITH earthquake_count as(
    SELECT 
            region,
            FORMAT_DATETIME("%b-%d-%Y",time) as day,
            count(*) as total_earthquakes
    FROM `{table_id}`
    GROUP BY 1,2
)
SELECT
    region,
    day,
    avg(total_earthquakes) as average_earthquakes
FROM earthquake_count
GROUP BY 1,2
"""

# Define Question no
question_No = 6

#  Location to store analyzed data
outputFilePath = f"gs://earthquake_usgs_bucket/Dataflow/analysis/analyzed_data/que{question_No}/que{question_No}"


# Define Schema for parquet  file
parquet_schema = pa.schema([
    ("region", pa.string()),
    ("day", pa.string()),
    ("average_earthquakes", pa.int64())
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

