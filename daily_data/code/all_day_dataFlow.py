import requests
import os, json
from datetime import date
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions


# A function to fetch data from API.
def fetch_response_from_api(url):
    response = requests.get(url)

    if response.status_code == 200:
        print("Data Fetched From API successfully!")
        data = response.json()  # Converts response to JSON-encoded content (Python Dictionary)
        return data
    else:
        print(f"\n!!ERROR!! Failed to retrieve data from the API")
        return None


# # Setting up GCP Credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (r"D:\BrainWorks\Pyspark "
#                                                 r"Assignments\earthquake_ingestion\gcs_service_account_key"
#                                                 r"\earthquake-usgs-project-0d36682ec064.json")

# Setting up GCS filePath
date = date.today()
date = date.strftime('%y%m%d')
gcsFilePath = f"gs://earthquake_usgs_bucket/daily_data/dataflow/bronze/{date}/"

# Setting up the parameters for writing to BigQuery.
table = f'earthquake-usgs-project.earthquake_db_daily_data.earthquake_data_dataflow_{date}'
dataset = 'earthquake-usgs-project.earthquake_db_daily_data'
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



class FetchAPIdata(beam.DoFn):
    # Function to Fetch data from API
    def process(self, element):
        apiURL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
        data = fetch_response_from_api(apiURL)
        data = json.dumps(data)  # Converting to json-string to write on gcs
        yield data


class ParseJSON(beam.DoFn):
    def process(self, element):
        # Assuming each element is a JSON string
        yield json.loads(element)


class FlattenData(beam.DoFn):

    def epoch_to_timestamp(self, epoch):
        import time
        if epoch > 1e10:
            epoch /= 1000
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(epoch))
        return timestamp

    def extract_region_and_country(self, place):
        if "of " in place:
            splitted_data = place.split("of ")
            region_country = splitted_data[1].split(',')

            if len(region_country) > 1:
                region_country = {'region': region_country[0].strip(), 'country': region_country[1].strip()}
            else:
                region_country = {'region': region_country[0].strip(), 'country': None}
        else:
            region_country = {'region': place.strip(), 'country': None}

        return region_country

    def process(self, element, *args, **kwargs):
        # Extracting Features
        features = element["features"]

        for feature in features:
            flattenRecord = {
                'mag': feature['properties'].get('mag', ''),
                'place': feature['properties'].get('place', ''),
                'region': self.extract_region_and_country(feature['properties'].get('place', '')).get('region', ''),
                'country': self.extract_region_and_country(feature['properties'].get('place', '')).get('country', ''),
                'time': self.epoch_to_timestamp(feature['properties'].get('time', '')),
                'updated': self.epoch_to_timestamp(feature['properties'].get('updated', '')),
                'tz': feature['properties'].get('tz', ''),  # Should be int64
                'url': feature['properties'].get('url', ''),
                'detail': feature['properties'].get('detail', ''),
                'felt': feature['properties'].get('felt', ''),  # Should be int64
                'cdi': feature['properties'].get('cdi', ''),  # Should be decimal128
                'mmi': feature['properties'].get('mmi', ''),  # Should be decimal128
                'alert': feature['properties'].get('alert', ''),
                'status': feature['properties'].get('status', ''),
                'tsunami': feature['properties'].get('tsunami', ''),  # Should be int64
                'sig': feature['properties'].get('sig', ''),  # Should be int64
                'net': feature['properties'].get('net', ''),  # Should be int64
                'code': feature['properties'].get('code', ''),
                'ids': feature['properties'].get('ids', ''),
                'sources': feature['properties'].get('sources', ''),
                'types': feature['properties'].get('types', ''),
                'nst': feature['properties'].get('nst', ''),  # Should be int64
                'dmin': feature['properties'].get('dmin', ''),  # Should be decimal256
                'rms': feature['properties'].get('rms', ''),  # Should be decimal256
                'gap': feature['properties'].get('gap', ''),  # Should be decimal256
                'magType': feature['properties'].get('magType', ''),
                'type': feature['properties'].get('type', ''),
                'title': feature['properties'].get('title', ''),
                'geometry_longitude': feature['geometry']['coordinates'][0],  # Should be decimal256
                'geometry_latitude': feature['geometry']['coordinates'][1],  # Should be decimal256
                'geometry_depth': feature['geometry']['coordinates'][2],  # Should be decimal256
                'id': feature['id']
            }

            # Convert the flattened record to JSON string before yielding
            json_record = json.dumps(flattenRecord)  # Yield each flattened record as JSON string

            # Print the type of the data being yielded
            # print(f"Data type after json.dumps: {type(json_record)}")  # Check the data type
            yield json_record  # Yield the JSON string



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


def printMessage_bronze():
    print("DATA FILE ADDED TO BRONZE LAYER!!")


def printMessage_silver():
    print("DATA FILE ADDED TO SILVER LAYER!!")


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

bronze_filePath = f"gs://earthquake_usgs_bucket/daily_data/dataflow/bronze/{date}/"
silver_filepath = f"gs://earthquake_usgs_bucket/daily_data/dataflow/silver/{date}/"


# Creating a pipeline to read data from api and store it to bronze layer.
# Write to Bronze
with beam.Pipeline() as p:
    readFromApI = (
        p
        | "CreateEmptyPipeline" >> beam.Create([None])
        | "FetchAPIData" >> beam.ParDo(FetchAPIdata())
        | "WriteToBronze" >> WriteToText(gcsFilePath, file_name_suffix='.json')
    )

print("Data successfully written to the Bronze layer.")

# Read from Bronze and Write to Silver
with beam.Pipeline() as p:
    processBronzeToSilver = (
        p
        | "ReadFromBronze" >> beam.io.ReadFromText(bronze_filePath)
        | "ParseJSON" >> beam.ParDo(ParseJSON())
        | "FlattenData" >> beam.ParDo(FlattenData())
        | "WriteToSilver" >> WriteToText(silver_filepath, file_name_suffix='.json')
    )

print("Data successfully transformed and written to the Silver layer.")



with beam.Pipeline(options=options) as p:

    # READ FILE FROM GCP BUCKET
    readFromBucket = (
        p
        | "Read from Bucket" >> beam.io.ReadFromText(silver_filepath)
        | "Add Timestamp Column" >> beam.ParDo(addTimeStampColumn())
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
print("Data successfully transformed and written to the BQ.")

# -------------------------------------------------------------------------------------------------------- #
#                                                                                                          #
# -------------------------------------------------------------------------------------------------------- #

