from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, split
from pyspark.sql.types import IntegerType, DecimalType, DoubleType
from datetime import date

# Initializing a SparkSession Object
spark = SparkSession.builder.master('local[*]').appName("Transforming and Writing Data to Silver Layer").getOrCreate()
print(f"SparkSession object has been initiated : {spark} \n")

#  Creating a Dataframe and Reading Data From Bronze Layer in GCS
gcs_bronzePath = ("gs://earthquake_usgs_bucket/Pyspark/bronze/241111/earthquake_monthly_data.json/part-00000-af82e54a"
                  "-12ab-470f-b420-8f0008b0479d-c000.json")

earthquake_df = spark.read.json(gcs_bronzePath)

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
filePath = f"gs://earthquake_usgs_bucket/Pyspark/Silver/{folderName}/earthquake_monthly_data"

# Writing transformed data to GCS (Silver Layer)
earthquake_df.coalesce(1).write.mode("overwrite").parquet(filePath)

print("File Added to GCS bucket.")

# Stop the sparkSession
spark.stop()
print("Spark Session Stopped.")

# Command to submit pyspark job on dataproc
# gcloud dataproc jobs submit pyspark gs://dataproc-test-files/dataProc_read_transform_upload_job.py
# --cluster=cluster-5c0e
# --region=asia-east1