# Que 9 : Find out the regions which are having the highest frequency and intensity of earthquakes


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
        COUNT(*) AS earthquake_frequency,
        avg(mag) AS average_magnitude
FROM earthquake-usgs-project.earthquake_db.earthquake_data_dataflow
GROUP BY region
ORDER BY earthquake_frequency DESC, average_magnitude DESC
"""

result = bigquery_query_runner(filepath, project_id, sql_query)

print("---------------------------------")
for row in result:
    print(f"Region : {row.get('region')}\n"
          f"Earthquake Frequency: {row.get('earthquake_frequency')}\n"
          f"Average Magnitude: {row.get('average_magnitude')}")
    print("---------------------------------")
