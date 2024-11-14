# Que 7 : Find the region name, which had the highest magnitude earthquake last week

import os
import pyarrow as pa
import apache_beam as beam
from datetime import datetime,timedelta
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
google_cloud_options.job_name = "bq-table-analysis-using-dataflow-que7"
google_cloud_options.temp_location = r"gs://earthquake_usgs_bucket/Dataflow/analysis/temp/"

# Setting up runner
options.view_as(StandardOptions).runner = "DataflowRunner"

# Define a date range for last week

end_date = datetime.now().date()
start_date = end_date - timedelta(days=7)

# Writing a query.
query = f"""
WITH last_week_data as(
    SELECT
        region,
        mag,
        FORMAT_DATETIME("%Y-%m-%d",time) as day
    FROM `{table_id}`
    WHERE FORMAT_DATETIME("%Y-%m-%d",time) BETWEEN '{start_date}' and '{end_date}' 
)
SELECT
* 
FROM last_week_data
WHERE mag = (SELECT MAX(mag) FROM last_week_data)
"""

# Define Question no
question_No = 7

#  Location to store analyzed data
outputFilePath = f"gs://earthquake_usgs_bucket/Dataflow/analysis/analyzed_data/que{question_No}/que{question_No}"


# Define Schema for parquet  file
parquet_schema = pa.schema([
    ("region", pa.string()),
    ("mag", pa.float64()),
    ("day", pa.string())
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

