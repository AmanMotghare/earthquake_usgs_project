import requests


def fetch_response_from_api(url):
    response = requests.get(url)

    if response.status_code == 200:
        print("Data Fetched From API successfully!")
        data = response.json()  # Converts response to JSON-encoded content (Python Dictionary)
        return data
    else:
        print(f"\n!!ERROR!! Failed to retrieve data from the API")
        return None
