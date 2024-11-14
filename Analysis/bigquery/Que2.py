# Que 2 : Find the average magnitude by the region

from bigquery_utils import bigquery_query_runner

# Required Parameters

# GCP Credentials filepath
filepath = (r'D:\BrainWorks\Pyspark Assignments\earthquake_ingestion\gcs_service_account_key\earthquake-usgs-project'
            r'-0d36682ec064.json')

project_id = 'earthquake-usgs-project'

# Writing a query.
sql_query = """
SELECT region, 
    avg(mag) as average_magnitude
FROM earthquake-usgs-project.earthquake_db.earthquake_data_dataflow
GROUP BY region
"""

result = bigquery_query_runner(filepath, project_id, sql_query)

for row in result:
    print(f"Region : {row.get('region')}\n"
          f"Average Magnitude : {row.get('average_magnitude')}")
    print("---------------------------------")
