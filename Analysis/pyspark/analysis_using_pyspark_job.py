import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initializing a spark session
spark = (SparkSession.builder.master('local[*]')
         .appName('analyzing data in bigquery using pyspark.')
         .getOrCreate())

print(f"Spark Session Initialized!! {spark}")

# BigQuery table id
bigquery_table = "earthquake-usgs-project.earthquake_db.earthquake_data_pyspark"

# Load BigQuery table to spark df
earthquake_data = spark.read.format("bigquery").option("table", bigquery_table).load()

# # Que1 : Count the number of earthquakes by region
# earthquake_data = earthquake_data.groupBy(col("region")).count()

# --------------------------------------------------------------------------------------- #

# # Que2 : Find the average magnitude by the region
# earthquake_data = earthquake_data.groupBy(col("region")).agg(avg(col("mag"))).alias("Average Magnitude")

# --------------------------------------------------------------------------------------- #

# # Que3 : Find how many earthquakes happen on the same day.
# earthquake_data = earthquake_data.select("time")
# earthquake_data = earthquake_data.select(to_date(col("time"), 'yyyy-MM-dd').alias("date"))
# earthquake_data = earthquake_data.groupBy(col("date")).count()

# --------------------------------------------------------------------------------------- #

# # Que4 : Find how many earthquakes happen on same day and in same region.
# earthquake_data = earthquake_data.select(to_date(col("time"), 'yyyy-MM-dd').alias("date"), "region")
# earthquake_data = earthquake_data.groupBy("date", "region").count()

# --------------------------------------------------------------------------------------- #

# # Que5 : Find average earthquakes happen on the same day.
# earthquake_data = earthquake_data.select(to_date(col("time"), 'yyyy-MM-dd').alias("date"))
# earthquake_data = earthquake_data.groupBy("date").agg(count("*").alias("earthquake_count"))
# earthquake_data = earthquake_data.agg(avg(col("earthquake_count")).alias("avg_earthquakes_per_day"))

# --------------------------------------------------------------------------------------- #

# # Que6 : Find average earthquakes happen on same day and in same region
# earthquake_data = earthquake_data.select(to_date(col("time"), 'yyyy-MM-dd').alias("date"), "region")
# earthquake_data = earthquake_data.groupBy("date", "region").agg(count("*").alias("total_earthquakes"))
# earthquake_data = earthquake_data.groupBy("date", "region").agg(
#     avg(col("total_earthquakes")).alias("average_earthquakes"))

# --------------------------------------------------------------------------------------- #

# # Que7 : Find the region name, which had the highest magnitude earthquake last week.
# earthquake_data = earthquake_data.select("region",
#                                          "mag",
#                                          to_date(col("time"), "yyyy-MM-dd").alias("day"))
#
# # Finding out last week's date.
# today_date = datetime.today().date()
# last_week_date = today_date - timedelta(days=7)
#
# earthquake_data = earthquake_data.filter(col("day") == last_week_date)
# earthquake_data = earthquake_data.groupBy("region").agg(max(col("mag")).alias("maximum_magnitude"))

# --------------------------------------------------------------------------------------- #

# # Que 8 : Find the region name, which is having magnitudes higher than 5.
# earthquake_data = earthquake_data.select("region", "mag")
# earthquake_data = earthquake_data.filter(col("mag") > 5)

# --------------------------------------------------------------------------------------- #


# Que9 : Find out the regions which are having the highest frequency and intensity of earthquakes
earthquake_data = earthquake_data.select("region", "mag")

earthquake_frequency = earthquake_data.groupBy("region").agg(count("*").alias("earthquake_frequency"))
average_magnitude = earthquake_data.groupBy("region").agg(avg(col("mag")).alias("average_magnitude"))
earthquake_data = earthquake_frequency.join(average_magnitude, on="region", how="left")
earthquake_data = earthquake_data.orderBy(col("earthquake_frequency").desc(), "average_magnitude")

# # See data from BigQuery Table.
earthquake_data.printSchema()
earthquake_data.show(10, truncate=False)
