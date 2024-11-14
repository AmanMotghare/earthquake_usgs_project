# Que 7 : Find the region name, which had the highest magnitude earthquake last week
from bigquery_utils import bigquery_query_runner
from datetime import date, datetime, timedelta

# Required Parameters

# GCP Credentials filepath
filepath = (r'D:\BrainWorks\Pyspark Assignments\earthquake_ingestion\gcs_service_account_key\earthquake-usgs-project'
            r'-0d36682ec064.json')
project_id = 'earthquake-usgs-project'

# Define a date range for last week

end_date = datetime.now().date()
start_date = end_date - timedelta(days=7)
print(end_date, "            ", start_date)

# Writing a query.
sql_query = f"""
WITH last_week_data as(
    SELECT
        region,
        mag,
        FORMAT_DATETIME("%Y-%m-%d",time) as day
    FROM earthquake-usgs-project.earthquake_db.earthquake_data_dataflow
    WHERE FORMAT_DATETIME("%Y-%m-%d",time) BETWEEN '{start_date}' and '{end_date}' 
)
SELECT
* 
FROM last_week_data
WHERE mag = (SELECT MAX(mag) FROM last_week_data)
"""

result = bigquery_query_runner(filepath, project_id, sql_query)

for row in result:
    print(f"Region : {row.get('region')}\n"
          f"MAGNITUDE : {row.get('mag')}\n"
          f"Day: {row.get('day')}")
    print("---------------------------------")
