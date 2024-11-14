from google.cloud import storage
import os


def main():
    # Setting Up GCP Credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ("D:\BrainWorks\Pyspark "
                                                    "Assignments\earthquake_ingestion\gcs_service_account_key\earthquake-usgs-439310-ec24b3809353.json")

    # Required Id's & FilePath
    filePath = ("Pyspark/bronze/241022/earthquake_monthly_data.json/part-00000-1f84f06e"
                "-c3df-4046-bab9-91f3131c8be3-c000.json")
    project_id = "earthquake-usgs-439310"
    bucket_name = "earthquake_usgs_analysis"

    # Reading file using Python Client API

    # Setting up Client Connection
    client = storage.Client(project_id)
    print("Client Connection Established!")

    # Fetching data from GCS Bucket
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filePath)
    print("Bucket & File(blob) Found!")

    # download the blob to local folder
    download_filePath = ("D:\BrainWorks\Pyspark Assignments\earthquake_ingestion\monthly_data\Silver\downloads"
                         "\earthquake_data.json")

    blob.download_to_filename(download_filePath)
    print(f"File Downloaded Successfully at location {download_filePath}")


if __name__ == "__main__":
    main()
