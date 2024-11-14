import os
from google.cloud import bigquery


def bigquery_query_runner(filePath, project_id, sql_query):
    """
    Executes a SQL query in BigQuery and returns the results.

    :param filePath: Path to the JSON key file for GCP service account credentials.
    :param project_id: The GCP project ID where the BigQuery dataset is located.
    :param sql_query: The SQL query string to execute in BigQuery.
    :return: A BigQuery QueryJob object, which can be used to fetch query results.
    """

    # Setting up GCP Credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = filePath

    # Initializing Bigquery Client
    client = bigquery.Client(project=project_id)
    print(f"Bigquery Client Established!\n")

    # Sending a query and Storing a result
    try:
        result = client.query(sql_query)
        return result.result()

    except Exception as e:
        print(f"!! QUERY EXECUTION FAILED !! \n"
              f"REASON : {e}")
        return None
