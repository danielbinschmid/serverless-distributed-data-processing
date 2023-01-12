import os
import time
from azure.storage.blob import BlobServiceClient

# Connect to the Azure storage account
blob_service_client = BlobServiceClient.from_connection_string("CONNECTION_STRING")

# Get the name of the container
container_name = 'CONTAINER_NAME'

while True:
    # Open file with list of files to upload
    with open('filelist.txt', 'r') as f:
        file_list = f.read().splitlines()
        # Iterate through files and upload them to the container
        for file_name in file_list:
            with open(file_name, 'rb') as data:
                blob_service_client.get_blob_client(container=container_name, blob=file_name).upload_blob(data)
            print(f'Uploaded {file_name} to {container_name}')
    # Wait 10 seconds before uploading the next set of files
    time.sleep(10)
