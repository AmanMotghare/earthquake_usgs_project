# Que 8 : Find the region name, which is having magnitudes higher than 5.


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
        mag
FROM earthquake-usgs-project.earthquake_db.earthquake_data_dataflow
WHERE mag > 5
"""

result = bigquery_query_runner(filepath, project_id, sql_query)

print("---------------------------------")
for row in result:
    print(f"Region : {row.get('region')}\n"
          f"Magnitude: {row.get('mag')}")
    print("---------------------------------")
