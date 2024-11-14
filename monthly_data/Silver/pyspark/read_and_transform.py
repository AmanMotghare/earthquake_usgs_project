from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, split
from pyspark.sql.types import IntegerType, DecimalType, DoubleType


def main():
    # Initializing a SparkSession Object
    spark = SparkSession.builder.master('local[*]').appName("Reading and Transforming data from GCS").getOrCreate()
    print(f"SparkSession object has been initiated : {spark} \n")

    # Reading file from downloaded local storage
    filePath = ("D:\BrainWorks\Pyspark Assignments\earthquake_ingestion\monthly_data\Silver\downloads\earthquake_data"
                ".json")

    # Creating a Dataframe by reading a JSON file from local storage.
    earthquake_df = spark.read.json(filePath)

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

    earthquake_df.show(5, truncate=False)


if __name__ == '__main__':
    main()
