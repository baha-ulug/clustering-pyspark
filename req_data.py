import requests
from dotenv import load_dotenv
import os
load_dotenv()

URL = os.environ.get("URL")
local_filename = "dataset/eda.csv"

response = requests.get(URL)

if response.status_code == 200:
    with open(local_filename, "wb") as file:
        file.write(response.content)
    print("File downloaded successfully.")
else:
    print(f"Failed to download the file. Status code: {response.status_code}")
