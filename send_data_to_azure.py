import requests
from azure.storage.blob import BlobServiceClient
from urllib.parse import urlparse
import os

def generate_clickbench_urls(start_num=0, end_num=99):
    """
    Generates clickbench URLs based on the pattern
    """
    base_url = "https://db.in.tum.de/teaching/ws2425/clouddataprocessing/data"
    urls = [f"{base_url}/clickbench.{i:02d}.csv" for i in range(start_num, end_num + 1)]
    return urls

def download_and_upload_to_azure(urls, connection_string, container_name):
    """
    Downloads files from given URLs and uploads them to Azure Blob Storage
    """
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    for url in urls:
        try:
            filename = os.path.basename(urlparse(url).path)
            print(f"Processing {filename}...")
            
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            blob_client = container_client.get_blob_client(filename)
            
            print(f"Uploading {filename} to Azure...")
            blob_client.upload_blob(response.raw, overwrite=True)
            
            print(f"Successfully uploaded {filename}")
            
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {filename}: {str(e)}")
        except Exception as e:
            print(f"Error uploading {filename}: {str(e)}")

# 🔴 REMOVE hardcoded connection string
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "cbdp-files"

if not connection_string:
    raise ValueError("Azure Storage connection string is missing! Set the AZURE_STORAGE_CONNECTION_STRING environment variable.")

urls = generate_clickbench_urls(0, 99) 
download_and_upload_to_azure(urls, connection_string, container_name)
