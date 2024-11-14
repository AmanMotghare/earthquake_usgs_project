import json
from datetime import date
import apache_beam as beam
import os
import pyarrow as pa
from apache_beam.io import WriteToParquet, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# Setting up GCP Credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ("D:\BrainWorks\Pyspark "
                                                "Assignments\earthquake_ingestion\gcs_service_account_key\earthquake"
                                                "-usgs-project-0d36682ec064.json")


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


with beam.Pipeline() as p:
    filePath = r"gs://earthquake_usgs_bucket/Dataflow/Bronze/241111/earthquake_data/-00000-of-00001.json"

    readData = (
            p | "Read File from GCS Bucket" >> beam.io.ReadFromText(filePath)
            | "ParseJSON" >> beam.ParDo(ParseJSON())
            | "Flatten the Data" >> beam.ParDo(FlattenData())
    )

    folderName = date.today().strftime('%y%m%d')
    gcs_output_path = f"gs://earthquake_usgs_bucket/Dataflow/silver/{folderName}/earthquake_monthly_data"

    writeToGCS = (
            readData | "write to GCS" >> WriteToText(gcs_output_path, file_name_suffix='.json')
    )
