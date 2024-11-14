# Que 1 : Count the number of earthquakes by region

from bigquery_utils import bigquery_query_runner

# Required Parameters

# GCP Credentials filepath
filepath = ('D:\BrainWorks\Pyspark Assignments\earthquake_ingestion\gcs_service_account_key\earthquake-usgs-project'
            '-0d36682ec064.json')
project_id = 'earthquake-usgs-project'

# Writing a query.
sql_query = """
SELECT region, 
    count(*) as total_earthquakes
FROM earthquake-usgs-project.earthquake_db.earthquake_data_dataflow
GROUP BY region
"""

result = bigquery_query_runner(filepath, project_id, sql_query)

for row in result:
    print(f"Region : {row.get('region')}\n"
          f"Total EarthQuakes : {row.get('total_earthquakes')}")
    print("---------------------------------")
