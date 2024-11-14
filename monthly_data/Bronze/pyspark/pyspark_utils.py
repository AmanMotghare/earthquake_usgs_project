import requests
def fetchData_fromAPI(url):
    response = requests.get(url)

    if response.status_code == 200:
        print("Data Fetched From API successfully!")
        data = response.json()  # Converts response to JSON-encoded content (Python Dictionary)
        return data
    else:
        print(f"\n!!ERROR!! Failed to retrieve data from the API")
        return None
