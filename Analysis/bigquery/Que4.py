# Que 4 :  Find how many earthquakes happen on same day and in same region

from bigquery_utils import bigquery_query_runner

# Required Parameters

# GCP Credentials filepath
filepath = (r'D:\BrainWorks\Pyspark Assignments\earthquake_ingestion\gcs_service_account_key\earthquake-usgs-project'
            r'-0d36682ec064.json')

project_id = 'earthquake-usgs-project'

# Writing a query.
sql_query = """
SELECT 
        region,
        FORMAT_DATETIME("%b-%d-%Y",time) as day,
        count(*) as total_earthquakes
FROM earthquake-usgs-project.earthquake_db.earthquake_data_dataflow
GROUP BY 1,2
"""

result = bigquery_query_runner(filepath, project_id, sql_query)

for row in result:
    print(f"Day : {row.get('day')}\n"
          f"Region : {row.get('region')}\n"
          f"Total Earthquakes : {row.get('total_earthquakes')}")
    print("---------------------------------")
