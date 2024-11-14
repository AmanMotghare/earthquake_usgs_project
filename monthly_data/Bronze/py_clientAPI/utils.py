import requests
import os
from google.cloud import storage

# URLs for earthquake data
monthly_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
daily_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


def get_monthly_earthquake_data():
    # Fetch data from the URL
    response_monthly = requests.get(monthly_url)  # request sent to get a data

    # Checking if data has fetched properly
    # if status code is 200, it means the response is successful.
    if response_monthly.status_code == 200:
        print('Monthly Data Received.')

        # getting the Monthly data
        monthly_data = response_monthly.json()  # Returns the json-encoded content of a response (in python-dictionary)
        # print(type(monthly_data)) # <class 'dict'>
        return monthly_data
    else:
        print('Failed to retrieve Monthly data.')
        return None


def add_raw_historical_to_gcs(project, bucket_name, destination_path, data, credentials_key_path, content_type):
    """
    :param project: GCP Project Name
    :param bucket_name: Name of a bucket where data will be stored.
    :param destination_path: Destination path to upload a data file, with name.
    :param data: Response json data
    :param credentials_key_path: Path of a GOOGLE_APPLICATION_CREDENTIALS
    """

    try:
        # Setting up GCP Credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_key_path

        # Creating connection with GCS using Client
        client = storage.Client(project=project)
        # print(f"Client : {client}")

        # Getting the GCS bucket where the file will be stored.
        bucket = client.bucket(bucket_name)
        # print(f'Bucket : {bucket}')

        # Creating a blob (object) in the bucket
        blob = bucket.blob(destination_path)

        # Upload the data
        blob.upload_from_string(data, content_type=content_type)

        print(f"File Uploaded Successfully to GCS Bucket at PATH: {destination_path}")
    except Exception as e:
        print(f"Error uploading file: {str(e)}")


def get_daily_earthquake_data():
    # Fetch data from URL
    response_daily = requests.get(daily_url)  # request sent to get a data

    # Checking if data has fetched properly
    # if status code is 200, it means the response is successful.
    if response_daily.status_code == 200:
        print('Monthly Data Received.')

        # Getting the Daily Data.
        daily_data = response_daily.json()  # Converts JSON string to a Python dictionary
        return daily_data
    else:
        print('Failed to retrieve Daily data.')
        return None
