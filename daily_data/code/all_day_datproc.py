import requests
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, LongType, FloatType
from pyspark.sql.functions import col, to_timestamp, split, lit
from pyspark.sql.types import IntegerType, DecimalType, DoubleType


def fetchData_fromAPI(url):
    """
        Fetches data from a given API URL.

        Parameters:
        url (str): The API endpoint URL from which data is to be fetched.

        Returns:
        dict or None:
            - If the request is successful, the response is returned as a JSON-encoded Python dictionary.
            - If the request fails, None is returned.
        """
    response = requests.get(url)

    if response.status_code == 200:
        print("Data Fetched From API successfully!")
        data = response.json()  # Converts response to JSON-encoded content (Python Dictionary)
        return data
    else:
        print(f"\n!!ERROR!! Failed to retrieve data from the API")
        return None


# Initializing a SparkSession Object
spark = SparkSession.builder.master('local[*]').appName("Ingest Earthquake data from API to GCS").getOrCreate()
print(f"SparkSession object has been initiated : {spark}")

# Fetching data from API
dailyData_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
daily_Data = fetchData_fromAPI(dailyData_url)

# Extracting required (feature) Data from the monthly_Data.
monthly_Data_features = daily_Data['features']  # features is a list of dictionaries

# Flatten and structure the JSON data for DataFrame conversion

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
earthquake_df = spark.createDataFrame(flattenData, schema=schema)
folderName = date.today()
folderName = folderName.strftime('%y%m%d')
gcs_output_path = f"gs://earthquake_usgs_bucket/daily_data/pyspark/bronze/{folderName}/"

# Write DataFrame to GCS (Below code creates multiple files due to partitions.)
# df.write.mode("overwrite").json(gcs_output_path)

# To get every data in a single file we will use coalesce(1)
earthquake_df.coalesce(1).write.mode("overwrite").json(gcs_output_path)

print("File added to BRONZE LAYER in GCS Bucket !")

# -------------------------------------------------------------------------------------------------------- #
#                                                                                                          #
# -------------------------------------------------------------------------------------------------------- #

# Cast Column Types according to the provided schema.
earthquake_df = (
    earthquake_df
    .withColumn('mag', col('mag').cast(DecimalType(10, 2)))
    .withColumn('felt', col('felt').cast(IntegerType()))
    .withColumn('cdi', col('cdi').cast(DecimalType()))
    .withColumn('mmi', col('mmi').cast(DecimalType()))
    .withColumn('tsunami', col('tsunami').cast(IntegerType()))
    .withColumn('sig', col('sig').cast(IntegerType()))
    .withColumn('nst', col('nst').cast(IntegerType()))
    .withColumn('dmin', col('dmin').cast(DecimalType()))
    .withColumn('rms', col('rms').cast(DecimalType()))
    .withColumn('gap', col('gap').cast(DecimalType()))
    .withColumn('geometry_depth', col('geometry_depth').cast(DoubleType()))
    .withColumn('geometry_latitude', col('geometry_latitude').cast(DoubleType()))
    .withColumn('geometry_longitude', col('geometry_longitude').cast(DoubleType()))
)

earthquake_df.printSchema()

# Transforming Data
# Columns like “time”, “updated” - convert its value from epoch to timestamp
earthquake_df = (earthquake_df
                 .withColumn('time', to_timestamp(col('time') / 1000))
                 .withColumn('updated', to_timestamp(col('updated') / 1000))
                 )

# Generate column “region” & "country - based on existing “place” column

# Creating "region"
earthquake_df = earthquake_df.withColumn("region", split(col("place"), " of ").getItem(1))
earthquake_df = earthquake_df.withColumn("region", split(col("region"), ", ").getItem(0))

# Creating "country"
earthquake_df = earthquake_df.withColumn("country", split(col("place"), ", ").getItem(1))

print("Transformations Completed !")

# Writing DataFrame to GCS Bucket
date = date.today()
folderName = date.strftime("%y%m%d")
filePath = f"gs://earthquake_usgs_bucket/daily_data/pyspark/silver/{folderName}/"

# Writing transformed data to GCS (Silver Layer)
earthquake_df.coalesce(1).write.mode("overwrite").parquet(filePath)

print("File Added to GCS bucket SILVER LAYER.")

# -------------------------------------------------------------------------------------------------------- #
#                                                                                                          #
# -------------------------------------------------------------------------------------------------------- #

# Setting up required params
project_name = 'earthquake-usgs-project'
dataset_name = "earthquake_db_daily_data"
table_name = f"earthquake_data_pyspark_{folderName}"

silverDataFilePath = f"gs://earthquake_usgs_bucket/daily_data/pyspark/silver/{folderName}/"

# Reading File from Silver Layer
earthquake_df = spark.read.parquet(silverDataFilePath)
earthquake_df.printSchema()

# Current Date Time
insert_date = datetime.now()

# Add new column to the dataframe
earthquake_df = earthquake_df.withColumn('insert_date', to_timestamp(lit(insert_date)))

# Write data to Bigquery
(earthquake_df
 .write
 .format('bigquery')
 .option('table', f"{project_name}.{dataset_name}.{table_name}")
 .option("temporaryGcsBucket", "dataproc_bq_earthquake-usgs-project")  # CREATE THIS TEMP BUCKET Manually
 .mode('append')
 .save()
 )

print(f"BigQuery Table Created!")
#
# # CLUSTER CREATION
# gcloud dataproc clusters create bq-airflow-spark-cluster \
# --enable-component-gateway \
# --region asia-east1 \
# --master-machine-type e2-standard-2 \
# --master-boot-disk-size 30 \
# --num-workers 2 \
# --worker-machine-type e2-standard-2 \
# --worker-boot-disk-size 30 \
# --image-version 2.0-debian10 \
# --optional-components JUPYTER \
# --initialization-actions gs://goog-dataproc-initialization-actions-asia-east1/connectors/connectors.sh \
# --metadata spark-bigquery-connector-version=0.21.0 \
# --temp-bucket=dataproc_bq_earthquake-usgs-project  # THIS IS MANDATORY AND CREATE THIS MANUALLY FIRST.


# # DATAPROC JOB SUBMIT

# gcloud dataproc jobs submit pyspark gs://earthquake_usgs_bucket/dataproc-pyspark-jobs/all_day_files/all_day_datproc.py \
# --cluster=bq-airflow-spark-cluster \
# --region=asia-east1 \
# --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar \
# -- dataproc_bq_earthquake-usgs-project


