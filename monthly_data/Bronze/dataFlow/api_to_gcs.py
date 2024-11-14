import json
import os
from datetime import date
import apache_beam as beam
from apache_beam.io import WriteToText
import dataFlow_utils

# Setting up GCS filePath
date = date.today()
date = date.strftime('%y%m%d')
gcsFilePath = f"gs://earthquake_usgs_bucket/Dataflow/Bronze/{date}/earthquake_data/"

# Setting Credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ("D:\BrainWorks\Pyspark "
                                                "Assignments\earthquake_ingestion\gcs_service_account_key\earthquake"
                                                "-usgs-project-0d36682ec064.json")

class FetchAPIdata(beam.DoFn):
    # Function to Fetch data from API
    def process(self, element):
        apiURL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
        data = dataFlow_utils.fetch_response_from_api(apiURL)
        data = json.dumps(data)  # Converting to json-string to write on gcs
        yield data


with beam.Pipeline() as p:
    readFromApI = (
            p | "CreateEmptyPipeline" >> beam.Create([None])
            | "FetchDataFromAPI" >> beam.ParDo(FetchAPIdata())
            # | "print" >> beam.Map(print)
    )

    writeToBucket = (
            readFromApI | "WriteToGCS" >> WriteToText(gcsFilePath, file_name_suffix='.json')
    )

