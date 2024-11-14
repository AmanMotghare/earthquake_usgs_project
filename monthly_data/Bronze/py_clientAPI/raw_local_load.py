import os
import json


def upload_apiData_to_local_storage(destination_dir, fileName, apiData):
    """
    This function uploads the API data to a local storage as a JSON file.

    :param destination_dir: Directory where the file should be stored.
    :param fileName: The name of the file to be created (including .json extension).
    :param apiData: The API data to be written to the file (in JSON format).
    """

    # Construct the full destination file path
    destinationFilePath = f'{destination_dir}\\{fileName}'

    # Ensure the directory exists
    os.makedirs(destination_dir, exist_ok=True)

    # Writing the data to the file in JSON format
    with open(destinationFilePath, 'w') as file:
        json.dump(apiData, file)

    print(f'File Created Successfully at {destinationFilePath}!')
