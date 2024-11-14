# Que 3 : Find how many earthquakes happen on the same day.

from bigquery_utils import bigquery_query_runner

# Required Parameters

# GCP Credentials filepath
filepath = (r'D:\BrainWorks\Pyspark Assignments\earthquake_ingestion\gcs_service_account_key\earthquake-usgs-project'
            r'-0d36682ec064.json')

project_id = 'earthquake-usgs-project'

# Writing a query.
sql_query = """
SELECT 
        FORMAT_DATETIME("%b-%d-%Y",time) as day, 
        count(EXTRACT(DAY FROM time)) as total_earthquakes
FROM earthquake-usgs-project.earthquake_db.earthquake_data_dataflow
GROUP BY 1
"""

result = bigquery_query_runner(filepath, project_id, sql_query)

for row in result:
    print(f"Day : {row.get('day')}\n"
          f"total_earthquakes : {row.get('total_earthquakes')}")
    print("---------------------------------")
