import os
from pyspark.sql import SparkSession

# Initializing a spark session
spark = (SparkSession.builder.master('yarn')
         .appName('bigquery_to_parquet')
         .getOrCreate())
print(f"Spark Session Initialized!! {spark}")

# BigQuery table id
bigquery_table = "earthquake-usgs-project.earthquake_db.earthquake_data_pyspark"

# Read the data from Bigquery
earthquake_data = (spark.read
                   .format("bigquery")
                   .option("table", bigquery_table)
                   .load())

# Write the data to parquet format in gcs.
gcs_path = "gs://earthquake_usgs_bucket/dataproc-pyspark-jobs/earthquake_data.parquet"
earthquake_data.coalesce(1).write.mode("overwrite").parquet(gcs_path)
