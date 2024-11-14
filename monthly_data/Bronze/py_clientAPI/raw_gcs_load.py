import utils
import json
from datetime import date

# Getting load_date and generating folder name from load_date (YYYYMMDD format)
load_date = date.today()
folderName = load_date.strftime("%Y%m%d")  # Format date as YYYYMMDD

# Getting the JSON data (data type : dict)
monthly_data = utils.get_monthly_earthquake_data()

# Converting dict to JSON string
monthly_data = json.dumps(monthly_data)

# Required GCS Details
project = r'earthquake-usgs-project'
bucket_name = r'earthquake_usgs_bucket'
destination_path = f'clientAPI/bronze/{folderName}/earthquake_monthly_data.json'
data = monthly_data
credentials_key_path = ('D:\BrainWorks\Pyspark Assignments\earthquake_ingestion\gcs_service_account_key\earthquake'
                        '-usgs-project-0d36682ec064.json')
content_type = 'application/json'

# Loading Raw data to GCS
utils.add_raw_historical_to_gcs(project=project,
                                bucket_name=bucket_name,
                                destination_path=destination_path,
                                data=data,
                                credentials_key_path=credentials_key_path,
                                content_type=content_type)
