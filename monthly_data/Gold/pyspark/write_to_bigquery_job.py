from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initializing a spark session
spark = SparkSession.builder.master('yarn').appName("Write data to BIGQUERY using Pyspark").getOrCreate()
print("Spark Session Created!")

# Setting up required params
project_name = 'earthquake-usgs-project'
dataset_name = "earthquake_db"
table_name = "earthquake_data_pyspark"

silverDataFilePath = (r"gs://earthquake_usgs_bucket/Pyspark/Silver/241111/earthquake_monthly_data/part-00000-3bc0ed25"
                      r"-d4fe-4cf3-8a22-1298c3d9e639-c000.snappy.parquet")

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

# Create DataProc Cluster Command
# gcloud dataproc clusters create bq-spark-cluster \
# --enable-component-gateway \
# --region $REGION \
# --master-machine-type e2-standard-2 \
# --master-boot-disk-size 30 \
# --num-workers 2 \
# --worker-machine-type e2-standard-2 \
# --worker-boot-disk-size 30 \
# --image-version 2.0-debian10 \
# --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
# --metadata spark-bigquery-connector-version=0.21.0

# WRITE THE ENTIRE ABOVE CODE TO CLOUD EDITOR.

# Submit this job to DataProc Cluster
# Command :

# export REGION = asia-east1
# gcloud dataproc jobs submit pyspark write_to_bigquery_job.py \
# --cluster=bq-spark-cluster \
# --region=$REGION \
# --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar \
# -- dataproc_bq_earthquake-usgs-project
