# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "24113e54-6f3f-4157-8c30-a3c5e66de623",
# META       "default_lakehouse_name": "VPC_Dev_Fablh_data",
# META       "default_lakehouse_workspace_id": "297572de-b7d7-4285-a88e-1388e2598d4a"
# META     }
# META   }
# META }

# CELL ********************

%run m365_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import requests
import pandas as pd
import logging
from delta.tables import DeltaTable
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Getting app secret keys from key vault
client_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-sharepoint-data-access-client-id')
tenant_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-sharepoint-data-access-tenant-id')
client_secret_value = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-sharepoint-data-access-client-secret')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tag_location = "VerdantasProjectsCopilot2024/Project documents"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

col_to_create = "ClientFolderID"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

client_folder_id = 'Hydrowo'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add custom Tags

# CELL ********************

def get_url_response(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_site_id(base_url, headers, hostname, site_name):

    url = f'{base_url}/sites/{hostname}:/sites/{site_name}'
    response = get_url_response(url, headers)
    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_list_id(base_url, headers, site_id, req_drive_name):

    url = f'{base_url}/sites/{site_id}/lists'    
    list_data = get_url_response(url, headers)
    
    for lst in list_data.get('value', []):
        if lst.get('name') == req_drive_name or lst.get('displayName') == req_drive_name:
            return lst.get('id')

    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

access_token = get_access_token(tenant_id, client_id, client_secret_value)
        
headers = get_headers(access_token)

base_url = 'https://graph.microsoft.com/v1.0'
hostname = 'hullinc.sharepoint.com'
site_name, drive_name = tag_location.split('/')

site_data = get_site_id(base_url, headers, hostname, site_name)
site_id = site_data['id']
list_id = get_list_id(base_url, headers, site_id, drive_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the API endpoint for creating a column
url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns'

# Define the data for the custom text column
data = {
    "name": col_to_create,
    "text": {
        "allowMultipleLines": True
    }
}

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

# Make the POST request to add the column
response = requests.post(url, headers=headers, data=json.dumps(data))

# Check if the request was successful
if response.status_code == 201:
    print("Custom column added successfully.")
else:
    print(f"Failed to add custom column. Status code: {response.status_code}")
    print(response.text)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'

# Set up the headers, including authorization
headers = {
    'Authorization': f'Bearer {access_token}'
}

# Make the GET request to retrieve the list of items
response = requests.get(url, headers=headers)

items = response.json()
item_df = pd.DataFrame(items['value'])
display(item_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

items.keys()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/106/fields'

# Define the data to update the custom column with a new value
data = {
    col_to_create: client_folder_id
}

# Set up the headers, including authorization and content type
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

# Make the PATCH request to update the field
response = requests.patch(url, headers=headers, data=json.dumps(data))

# Check if the request was successful
if response.status_code == 200:
    print("Custom column value updated successfully.")
else:
    print(f"Failed to update custom column. Status code: {response.status_code}")
    print(response.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Move file from local to Sharepoint

# CELL ********************

def get_site_id(base_url, headers, hostname, site_name):

    url = f'{base_url}/sites/{hostname}:/sites/{site_name}'
    response = get_url_response(url, headers)
    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_url_response(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_drive_id(base_url, headers, site_id, req_drive_name):

    url = f'{base_url}/sites/{site_id}/drives'
    drives_data = get_url_response(url, headers)

    for drive in drives_data.get('value', []):
        if drive.get('name') == req_drive_name:
            return drive.get('id')

    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_folders_from_drive(base_url, headers, drive_id):

    url = f'{base_url}/drives/{drive_id}/root/children'
    folders_data_dict = {}
    
    while url:

        folders_data = get_url_response(url, headers)
        
        for item in folders_data.get('value', []):
            if 'folder' in item:
                folders_data_dict[item['name']] = item['id']
        
        url = folders_data.get('@odata.nextLink', None)
    
    return folders_data_dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

access_token = get_access_token(tenant_id, client_id, client_secret_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Headers including the access token for authorization
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Base URL for Graph API
base_url = "https://graph.microsoft.com/v1.0"
hostname = 'hullinc.sharepoint.com'
site_name = 'VerdantasProjectsCopilot2024'

site_data = get_site_id(base_url, headers, hostname, site_name)
site_id = site_data['id']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src_req_drive_name = 'Documents'
src_drive_id = get_drive_id(base_url, headers, site_id, src_req_drive_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tgt_req_drive_name = 'Project documents'
tgt_drive_id = get_drive_id(base_url, headers, site_id, tgt_req_drive_name)
tgt_drive_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folders_id_mapper = get_folders_from_drive(base_url, headers, src_drive_id)
folders_id_mapper

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folders_id_mapper2 = get_folders_from_drive(base_url, headers, tgt_drive_id)
folders_id_mapper2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### For Moving a File ###
# url = f'{base_url}/sites/{site_id}/drives/{src_drive_id}/items/01LC474IRJQFDJLPZBYZB37HAXVOX4XKLR/children'
# url = f'{base_url}/sites/{site_id}/drives/{src_drive_id}/items/01LC474ISOW2KLCOY5EFHYZUE6S2RYEINO/children'
url = f'{base_url}/sites/{site_id}/drives/{src_drive_id}/items/01LC474IR2SRZEIYIQAFAKXUFO4DTY6YPR/children'

# POST request to move the file
response = requests.get(url, headers=headers)
pd.DataFrame(response.json()['value'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### For Moving a File ###
url = f'{base_url}/sites/{site_id}/drives/{tgt_drive_id}/items/01LC474IQGR5XBHQDUDVHJ2VK4OOXZQA6S/children'
# url = f'{base_url}/sites/{site_id}/drives/{src_drive_id}/items/01LC474ISOW2KLCOY5EFHYZUE6S2RYEINO/children'
# url = f'{base_url}/sites/{site_id}/drives/{src_drive_id}/items/01LC474IR2SRZEIYIQAFAKXUFO4DTY6YPR/children'

# POST request to move the file
response = requests.get(url, headers=headers)
pd.DataFrame(response.json()['value'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Site, drive, and file information
file_item_id = '016RSD35YMTMEL6QAKYVC2T2RLPKRELRAQ'
target_drive_id = drive_id
target_folder_id = '016RSD35ZU5YG5EXGPORA3PIQBBPY36OJA'

### For Moving a File ###
url = f'{base_url}/sites/{site_id}/drives/{drive_id}/items/{target_folder_id}/children'

# POST request to move the file
response = requests.get(url, headers=headers)
response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Site, drive, and file information
src_file_item_id = '01LC474IVB4DKAQWU6OZAZVXKCGVLA5FFX'
target_drive_id = tgt_drive_id
target_folder_id = '01LC474IX63DBHIBJ6RFEYHTFU46QOFAIR'

### For Moving a File ###
copy_url = f'https://graph.microsoft.com/beta/drives/{src_drive_id}/items/{src_file_item_id}/microsoft.graph.copy'

# Move request payload
copy_payload = {
    "parentReference": {
        "driveId": tgt_drive_id,
        "id": target_folder_id
    },
    # "name": "new-filename.ext"  # Optional: Rename the file during move
}

# POST request to move the file
copy_response = requests.post(copy_url, headers=headers, json=copy_payload)

if copy_response.status_code == 202:
    print("Copy operation initiated, tracking status...")

    # Get the URL to track the status from the Location header
    status_url = copy_response.headers.get('Location')

    if status_url:
        # Poll the status URL to track the operation progress
        while True:
            status_response = requests.get(status_url, headers=headers)
            status_data = status_response.json()

            if status_response.status_code == 200:
                print("File move operation completed successfully.")
                break
            elif status_response.status_code == 202:
                print("Move operation still in progress, waiting for completion...")
                time.sleep(2)  # Wait for a couple of seconds before polling again
            else:
                print(f"Error tracking move operation: {status_response.status_code}, {status_response.text}")
                break
    else:
        print("No status URL provided to track the operation.")
else:
    print(f"Error moving file: {move_response.status_code}, {move_response.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# URL to list items in the target folder after copying
list_items_url = f'https://graph.microsoft.com/v1.0/drives/{target_drive_id}/items/{target_folder_id}/children'

# GET request to list files in the target folder
list_items_response = requests.get(list_items_url, headers=headers)

if list_items_response.status_code == 200:
    files_list = list_items_response.json()
    print("Files in the target folder:")
    for item in files_list['value']:
        print(f"Name: {item['name']}, ID: {item['id']}")
else:
    print(f"Error listing items: {list_items_response.status_code}, {list_items_response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# URL to update metadata of the copied file
update_metadata_url = f'https://graph.microsoft.com/v1.0/drives/{target_drive_id}/items/{new_copied_file_id}'

# Payload to update file properties (Note: Some properties like modified date may not be writable)
update_payload = {
    "lastModifiedDateTime": "2023-09-01T12:34:56Z",  # Example: set last modified time
    "lastModifiedBy": {
        "user": {
            "email": "original-user@example.com",
            "displayName": "Original User"
        }
    }
}

# PATCH request to update file properties
update_response = requests.patch(update_metadata_url, headers=headers, json=update_payload)

if update_response.status_code == 200:
    print("File metadata updated successfully.")
else:
    print(f"Error updating file metadata: {update_response.status_code}, {update_response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Uploading file from local to SharePoint - new


# CELL ********************

def get_site_id(base_url, headers, hostname, site_name):

    url = f'{base_url}/sites/{hostname}:/sites/{site_name}'
    response = get_url_response(url, headers)
    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_url_response(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_drive_id(base_url, headers, site_id, req_drive_name):

    url = f'{base_url}/sites/{site_id}/drives'
    drives_data = get_url_response(url, headers)

    for drive in drives_data.get('value', []):
        if drive.get('name') == req_drive_name:
            return drive.get('id')

    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

access_token = get_access_token(tenant_id, client_id, client_secret_value)
headers = get_headers(access_token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Base URL for Graph API
base_url = "https://graph.microsoft.com/v1.0"
hostname = 'hullinc.sharepoint.com'
site_name = 'VerdantasProjectsCopilot2024'

site_data = get_site_id(base_url, headers, hostname, site_name)
site_id = site_data['id']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tgt_req_drive_name = 'Project documents'
tgt_drive_id = get_drive_id(base_url, headers, site_id, tgt_req_drive_name)
tgt_drive_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_folder_id = '01LC474IX63DBHIBJ6RFEYHTFU46QOFAIR'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import os

file_path = './builtin/01_VRS-SF330_Project_Linden_Street.pdf'

upload_session_url = f"https://graph.microsoft.com/v1.0/drives/{tgt_drive_id}/items/{target_folder_id}/children/01_VRS-SF330_Project_Linden_Street.pdf/createUploadSession"

# Function to create an upload session
def create_upload_session(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }
    response = requests.post(upload_session_url, headers=headers)
    response.raise_for_status()
    upload_url = response.json().get('uploadUrl')
    return upload_url

# Function to upload the file in chunks
def upload_file_in_chunks(upload_url, file_path):
    chunk_size = 327680  # 320 KB per chunk
    file_size = os.path.getsize(file_path)

    with open(file_path, 'rb') as f:  # Reading from built-in Fabric path
        chunk_number = 0
        while True:
            start = chunk_number * chunk_size
            end = min(start + chunk_size, file_size) - 1
            f.seek(start)
            chunk_data = f.read(chunk_size)

            headers = {
                'Content-Length': f'{len(chunk_data)}',
                'Content-Range': f'bytes {start}-{end}/{file_size}'
            }

            response = requests.put(upload_url, headers=headers, data=chunk_data)
            if response.status_code in [200, 201, 202]:  # Check for success
                print(f"Uploaded chunk {chunk_number + 1}")
            else:
                print(f"Failed to upload chunk {chunk_number + 1}: {response.status_code}")
                print(response.json())
                break

            if end == file_size - 1:
                print("Upload completed.")
                break

            chunk_number += 1

# Main execution
try:
    upload_url = create_upload_session(access_token)
    upload_file_in_chunks(upload_url, file_path)
except Exception as e:
    print(f"An error occurred: {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import os

file_path = "C:/Users/maitrid/Downloads/01_VRS-SF330_Resume_Jakubowski, Joe.pdf"

upload_session_url = f"https://graph.microsoft.com/v1.0/drives/{tgt_drive_id}/items/{target_folder_id}/children/01_VRS-SF330_Resume_Jakubowski, Joe.pdf/createUploadSession"

# Function to create an upload session
def create_upload_session(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }
    response = requests.post(upload_session_url, headers=headers)
    response.raise_for_status()
    upload_url = response.json().get('uploadUrl')
    return upload_url

# Function to upload the file in chunks
def upload_file_in_chunks(upload_url, file_path):
    chunk_size = 327680  # 320 KB per chunk
    file_size = os.path.getsize(file_path)

    with open(file_path, 'rb') as f:  # Reading from built-in Fabric path
        chunk_number = 0
        while True:
            start = chunk_number * chunk_size
            end = min(start + chunk_size, file_size) - 1
            f.seek(start)
            chunk_data = f.read(chunk_size)

            headers = {
                'Content-Length': f'{len(chunk_data)}',
                'Content-Range': f'bytes {start}-{end}/{file_size}'
            }

            response = requests.put(upload_url, headers=headers, data=chunk_data)
            if response.status_code in [200, 201, 202]:  # Check for success
                print(f"Uploaded chunk {chunk_number + 1}")
            else:
                print(f"Failed to upload chunk {chunk_number + 1}: {response.status_code}")
                print(response.json())
                break

            if end == file_size - 1:
                print("Upload completed.")
                break

            chunk_number += 1

# Main execution
upload_url = create_upload_session(access_token)
upload_file_in_chunks(upload_url, file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_path = 'C:/Users/maitrid/Downloads/01_VRS-SF330_Resume_Jakubowski, Joe.pdf'  # Change to your actual path

try:
    with open(file_path, 'rb') as f:
        print("File opened successfully!")
except FileNotFoundError as e:
    print(f"File not found: {e}")
except Exception as e:
    print(f"An error occurred: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
