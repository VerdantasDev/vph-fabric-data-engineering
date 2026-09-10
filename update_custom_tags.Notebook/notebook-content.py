# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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
client_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-teams-data-access-client-id')
tenant_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-teams-data-access-tenant-id')
client_secret_value = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-teams-data-access-client-secret')

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

hostname = 'vdtdevops.sharepoint.com'
site_path = '/sites/VDT_Copilot_POC'

site_id = get_site_id(access_token, headers, hostname, site_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

list_id = get_list_id(base_url, headers, site_id, req_drive_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

list_id = '23ff1413-2e5e-40b5-8869-a112b8fef333'

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
    "name": "CustomColumnName2",
    "text": {
        "allowMultipleLines": False,
        "maxLength": 255
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

url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/29/fields'

# Define the data to update the custom column with a new value
data = {
    "CustomTextColumn": "Architechture"
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

# ## Move file from Sharepoint to Sharepoint

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
hostname = 'vdtdevops.sharepoint.com'
site_name = 'VDT_Copilot_POC'

site_data = get_site_id(base_url, headers, hostname, site_name)
site_id = site_data['id']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

req_drive_name = 'Documents'
drive_id = get_drive_id(base_url, headers, site_id, req_drive_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folders_id_mapper = get_folders_from_drive(base_url, headers, drive_id)
folders_id_mapper

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f'{base_url}/drives/{drive_id}/root/children'
folders_data = get_url_response(url, headers)
folders_data

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
file_item_id = '016RSD35YMTMEL6QAKYVC2T2RLPKRELRAQ'
target_drive_id = drive_id
target_folder_id = '016RSD35ZU5YG5EXGPORA3PIQBBPY36OJA'

### For Moving a File ###
copy_url = f'https://graph.microsoft.com/beta/drives/{drive_id}/items/{file_item_id}/microsoft.graph.copy'

# Move request payload
copy_payload = {
    "parentReference": {
        "driveId": target_drive_id,
        "id": target_folder_id
    },
    # "name": "new-filename.ext"  # Optional: Rename the file during move
}

# POST request to move the file
copy_response = requests.post(copy_url, headers=headers, json=copy_payload)

if move_response.status_code == 202:
    print("Move operation initiated, tracking status...")

    # Get the URL to track the status from the Location header
    status_url = move_response.headers.get('Location')

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

# ## Uploading file from local to SharePoint

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

def upload_small_file_to_folder(access_token, site_id, drive_id, folder_path, file_path):
    file_name = os.path.basename(file_path)
    
    # The folder path should be relative to the root of the document library (drive)
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{folder_path}/{file_name}:/content"
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/octet-stream'
    }

    with open(file_path, 'rb') as file_data:
        response = requests.put(url, headers=headers, data=file_data)
    
    if response.status_code == 201:
        print("File uploaded successfully to the folder!")
    else:
        print(f"Failed to upload file: {response.status_code}, {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
