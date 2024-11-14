import pyspark_utils
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType

# Initializing a SparkSession Object
spark = SparkSession.builder.master('local[*]').appName("Ingest Earthquake data from API to GCS").getOrCreate()
print(f"SparkSession object has been initiated : {spark}")

# Fetching data from API
monthlyData_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
monthly_Data = pyspark_utils.fetchData_fromAPI(monthlyData_url)

# Extracting required (feature) Data from the monthly_Data.
monthly_Data_features = monthly_Data['features']  # features is a list of dictionaries

# Flattening and structure the JSON data for DataFrame conversion

flattenData = []

for feature in monthly_Data_features:
    # Extracting the 'properties' from the features
    properties = feature['properties']

    # Extracting the 'geometry' from the features
    geometry = feature['geometry']

    # Extracting the 'id' from the features
    id = feature['id']

    flat_record = {
        'mag': properties['mag'],
        'place': properties['place'],
        'time': properties['time'],
        'updated': properties['updated'],
        'tz': properties['tz'],
        'url': properties['url'],
        'detail': properties['detail'],
        'felt': properties['felt'],
        'cdi': properties['cdi'],
        'mmi': properties['mmi'],
        'alert': properties['alert'],
        'status': properties['status'],
        'tsunami': properties['tsunami'],
        'sig': properties['sig'],
        'net': properties['net'],
        'code': properties['code'],
        'ids': properties['ids'],
        'sources': properties['sources'],
        'types': properties['types'],
        'nst': properties['nst'],
        'dmin': properties['dmin'],
        'rms': properties['rms'],
        'gap': properties['gap'],
        'magType': properties['magType'],
        'type': properties['type'],
        'title': properties['title'],
        'geometry_longitude': geometry['coordinates'][0],
        'geometry_latitude': geometry['coordinates'][1],
        'geometry_depth': geometry['coordinates'][2],
        'id': id
    }

    flattenData.append(flat_record)

# Defining a Schema
schema = StructType([
    StructField('mag', StringType(), True),
    StructField('place', StringType(), True),
    StructField('time', LongType(), True),
    StructField('updated', LongType(), True),
    StructField('tz', IntegerType(), True),
    StructField('url', StringType(), True),
    StructField('detail', StringType(), True),
    StructField('felt', IntegerType(), True),
    StructField('cdi', StringType(), True),
    StructField('mmi', StringType(), True),
    StructField('alert', StringType(), True),
    StructField('status', StringType(), True),
    StructField('tsunami', IntegerType(), True),
    StructField('sig', IntegerType(), True),
    StructField('net', StringType(), True),
    StructField('code', StringType(), True),
    StructField('ids', StringType(), True),
    StructField('sources', StringType(), True),
    StructField('types', StringType(), True),
    StructField('nst', IntegerType(), True),
    StructField('dmin', StringType(), True),
    StructField('rms', StringType(), True),
    StructField('gap', StringType(), True),
    StructField('magType', StringType(), True),
    StructField('type', StringType(), True),
    StructField('title', StringType(), True),
    StructField('geometry_longitude', StringType(), True),
    StructField('geometry_latitude', StringType(), True),
    StructField('geometry_depth', StringType(), True),
    StructField('id', StringType(), True)
])

# Creating a Dataframe
df = spark.createDataFrame(flattenData, schema=schema)
folderName = date.today()
folderName = folderName.strftime('%y%m%d')
gcs_output_path = f"gs://earthquake_usgs_analysis/Pyspark/bronze/{folderName}/earthquake_monthly_data.json"

# Write DataFrame to GCS
df.write.mode("overwrite").json(gcs_output_path)

# stop the spark session
spark.stop()
