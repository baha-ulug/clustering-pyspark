import requests
from dotenv import load_dotenv
import os
def download_data():   
    load_dotenv()  # Loads environment variables from .env file
    URL = os.environ.get("URL")
    local_filename = "dataset/eda.csv"
    response = requests.get(URL)
    if response.status_code == 200:
        with open(local_filename, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully to the path {local_filename}.")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

if __name__ == '__main__':
    download_data()



