# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import os
import re
import sys
import requests
import pandas as pd
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql.functions import * #lit, col, when, size

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_access_token(tenant_id, client_id, client_secret_value):

    scope = 'https://graph.microsoft.com/.default'
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret_value,
        'scope': scope
    }

    response = requests.post(url, data=data)
    response.raise_for_status()

    access_token = response.json().get('access_token')
    return {'Authorization': 'Bearer ' + access_token}


def get_secrets_from_KV(key_vault_uri, key):
    return mssparkutils.credentials.getSecret(key_vault_uri, key)


def convert_tag(tags):
    result = {}
    for k, v in tags.items():
        unicode_str = re.sub(r'_x([0-9A-F]{4})_', r'\\u\1', k)
        decoded_str = unicode_str.encode('ascii').decode('unicode_escape')
        result[decoded_str] = v
    return result


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

# site_name -- Client Folder ID	
# req_drive_name -- documents, shared-documents

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
 

url = "https://graph.microsoft.com/v1.0/sites"
# Empty list to store all sites
all_sites = []
# Initial API request
response = requests.get(url, headers=headers)
# Loop to handle pagination
while response.status_code == 200:
    data = response.json()
    # Append current page's data to all_sites
    all_sites.extend(data.get('value', []))
    # Check if there is a next page
    if '@odata.nextLink' in data:
        # Set the next URL to the @odata.nextLink value
        url = data['@odata.nextLink']
        # Make a new request to get the next page
        response = requests.get(url, headers=headers)
    else:
        # No more pages, break the loop
        break


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
display(pd.DataFrame(all_sites))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = "https://graph.microsoft.com/v1.0/sites"

response = get_url_response(url, headers)
data = response.json()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_site_id(base_url, headers, hostname, site_name):

    # url = f'{base_url}/sites/{hostname}:/sites/{site_name}'
    url = f'https://graph.microsoft.com/v1.0/sites/{site_name}'
    
    response = get_url_response(url, headers)
    return response


def get_drive_id(base_url, headers, site_id, req_drive_name):

    url = f'{base_url}/sites/{site_id}/drives'
    drives_data = get_url_response(url, headers)

    for drive in drives_data.get('value', []):
        if drive.get('name') == req_drive_name:
            return drive.get('id')

    return None


def get_list_id(base_url, headers, site_id, req_drive_name):

    url = f'{base_url}/sites/{site_id}/lists'    
    list_data = get_url_response(url, headers)
    
    for lst in list_data.get('value', []):
        if lst.get('name') == req_drive_name or lst.get('displayName') == req_drive_name:
            return lst.get('id')

    return None


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


def get_subfolders(base_url, headers, drive_id, folder_id):

    folders_data_dict = {}
    url = f'{base_url}/drives/{drive_id}/items/{folder_id}/children'

    while url:            
            contents = get_url_response(url, headers)

            for item in contents.get('value', []):
                if 'folder' in item:
                    folders_data_dict[item.get('name')] = item.get('id')
            
            url = contents.get('@odata.nextLink', None)
    
    return folders_data_dict


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_items_in_folder(base_url, headers, site_id, drive_id, list_id, folder_id, drive_name):

    url = f'{base_url}/drives/{drive_id}/items/{folder_id}/children'
    child_details = []

    while url:

        contents = get_url_response(url, headers)
        
        for content in contents.get('value', []):
            
            file_path = content.get('parentReference').get('path')
            replace_string = f'/drives/{drive_id}/root:'
            if drive_id in file_path:
                file_path = file_path.replace(replace_string, drive_name)


            fold_dict = {
                'display_name': content.get('name'),
                'created_datetime': content.get('createdDateTime'),
                'last_modified_datetime': content.get('lastModifiedDateTime'),
                'id': content.get('id'),
                'sharepoint_url': content.get('webUrl'),
                'path':file_path,
                'parent_folder_name':content.get('parentReference').get('name')
            }

            if 'folder' in content:
                fold_dict['type'] = 'folder'
                fold_dict['child_count'] = content['folder'].get('childCount', 0)
            else:
                fold_dict['type'] = 'file'
                fold_dict['child_count'] = 0

            child_details.append(fold_dict)
        
        url = contents.get('@odata.nextLink', None)

    return child_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_all_files_from_folder(base_url, headers, site_id, drive_id, list_id, folder_id, req_drive_name):

    child_details = get_items_in_folder(base_url, headers, site_id, drive_id, list_id, folder_id, req_drive_name)
    child_df = spark.createDataFrame(child_details)

    if child_df.count() > 0:

        folders_df = child_df.filter(col('type') == 'folder').withColumn('parsed', lit(False))
        all_files_df = child_df.filter(col('type') == 'file')

        while folders_df.filter(col('parsed') == False).count() > 0:

            unprocessed_folders = folders_df.filter(col('parsed') == False).collect()

            for folder_row in unprocessed_folders:
                if folder_row['parsed']:
                    continue

                if folder_row['child_count'] == 0:
                    folders_df = folders_df.withColumn('parsed', when(col('id') == folder_row['id'], lit(True)).otherwise(col('parsed')))
                    continue
                
                folder_id = folder_row['id']
                child_details_temp = get_items_in_folder(base_url, headers, site_id, drive_id, list_id, folder_id, req_drive_name)
                child_df_temp = spark.createDataFrame(child_details_temp)

                if child_df_temp.count() > 0:

                    folders_df_temp = child_df_temp.filter(col('type') == 'folder').withColumn('parsed', lit(False))
                    folders_df = folders_df.union(folders_df_temp)
                    all_files_df = all_files_df.union(child_df_temp.filter(col('type') == 'file'))

                folders_df = folders_df.withColumn('parsed', when(col('id') == folder_row['id'], lit(True)).otherwise(col('parsed')))
    
    elif child_df.count() == 0:
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=child_df.schema)
    
    return all_files_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hostname = 'hullinc.sharepoint.com'
base_url = 'https://graph.microsoft.com/v1.0'
sites_to_scrape = ['VerdantasProjectsCopilot2024/Project documents/VPC_OLT DEMO data']

all_file_status_delta_table_path = "Tables/br_m365_sharepoint_all_file_status"
download_file_status_delta_table_path = "Tables/br_m365_sharepoint_download_file_status"

key_vault_uri = 'https://vpc-dev-keyvault.vault.azure.net/'

client_id = get_secrets_from_KV(key_vault_uri,'vpc-sharepoint-data-access-client-id')
client_secret = get_secrets_from_KV(key_vault_uri,'vpc-sharepoint-data-access-client-secret')
tenant_id = get_secrets_from_KV(key_vault_uri,'vpc-sharepoint-data-access-tenant-id')

headers = get_access_token(tenant_id, client_id, client_secret)

schema=None

for path in sites_to_scrape:
    print(path)

    data_tag = path.split('/')[-1]
    site_name, folder_path = path.split('/', maxsplit = 1)
    folders = folder_path.split('/')

    site_data = get_site_id(base_url, headers, hostname, site_name)
    site_id = site_data['id']

    req_drive_name = folders[0]
    drive_id = get_drive_id(base_url, headers, site_id, req_drive_name)
    list_id = get_list_id(base_url, headers, site_id, req_drive_name)

    if drive_id is not None and list_id is not None:

        folders_id_mapper = get_folders_from_drive(base_url, headers, drive_id)

        if len(folders)>1:
                
            hierarchy_list = folders[1:]
            
            for i in range(len(hierarchy_list)):

                folder_name = hierarchy_list[i]
                if i==0:
                    folder_id = folders_id_mapper[folder_name]
                else:
                    folder_id = subfolders_dict[folder_name]

                subfolders_dict = get_subfolders(base_url, headers, drive_id, folder_id)
                dir_df = get_items_in_folder(base_url, headers, site_id, drive_id, list_id, folder_id, req_drive_name)
                dir_df = spark.createDataFrame(dir_df)
                child_details = get_all_files_from_folder(base_url, headers, site_id, drive_id, list_id, folder_id, req_drive_name)
                # display(dir_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dir_df.filter(dir_df.type == 'folder')\
.withColumn('Child_items', dir_df.child_count)\
.select('id', 'display_name', col('Child_items'), 'path', 'sharepoint_url', 'created_datetime', 'last_modified_datetime')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp = child_details.groupby(child_details.parent_folder_name).agg(count(child_details.parent_folder_name).alias('file_count'))

display(
    child_details.alias('df_1').join(temp, on = ['parent_folder_name'], how='inner')\
                 .select('id', 'display_name', 'path', 'sharepoint_url', 'parent_folder_name', 'created_datetime', 'last_modified_datetime', 'file_count')
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
