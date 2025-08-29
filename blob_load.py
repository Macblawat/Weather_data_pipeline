import os
from azure.storage.blob import BlobServiceClient


from dotenv import load_dotenv
load_dotenv()

AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")   
CONTAINER_NAME = "wather-data-raw" 
if not AZURE_CONN_STR:
    raise ValueError("connection string not found.")

try:
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    local_folder = "weather_data"

    for filename in os.listdir(local_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(local_folder, filename)
            blob_client = container_client.get_blob_client(filename)

            with open(file_path, "rb") as data:
                blob_client.upload_blob(data)
                print(f"Uploaded {filename} to Azure Blob Storage")
except Exception as e:
    print(f" Loading data error: {e}")
    