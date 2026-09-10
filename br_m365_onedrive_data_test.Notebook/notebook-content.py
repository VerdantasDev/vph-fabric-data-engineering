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

import os
import requests
import pandas as pd
import logging
from delta.tables import DeltaTable
import re
import sys
sys.path.insert(0, '/lakehouse/default/Files/Configs')
from m365_utils import *
from m365_config import *
from datetime import datetime
import base64

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

# CELL ********************

def get_user_id(headers, email_id):

    url = f"https://graph.microsoft.com/v1.0/users?$filter=mail eq '{email_id}'"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        users = response.json()
        if users['value']:
            user_id = users['value'][0]['id']
            return user_id
        else:
            return None  # Email not found
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")

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

path_to_scrape = [{'email': "maitri.dixit@vdtdevops.com",
                   'path':'Documents/OneDrive Data Retrieval'}]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_subfolders(headers, user_id):

    folders_data_dict = {}
    url = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/children'

    # Make the GET request
    response = requests.get(url, headers=headers)
    contents = response.json()
    
    # Process the items in the response
    for item in contents.get('value', []):
        if 'folder' in item:
            folders_data_dict[item.get('name')] = item.get('id')
    
    # Check if there is a next page of results
    url = contents.get('@odata.nextLink', None)
    
    return folders_data_dict


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f'https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/children'
response = requests.get(url, headers=headers)
contents = response.json()
drive_id = contents['value'][0]['parentReference']['driveId']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

drive_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ps = path_to_scrape[0]
email_id = ps['email']
path = ps['path']
user_id = get_user_id(headers, email_id)
drive_name, folder_path = path.split('/', maxsplit = 1)
folders = get_subfolders(headers, user_id)
req_folder_name = folder_path.split('/')[0]
item_id = folders[req_folder_name]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f'https://graph.microsoft.com/v1.0/{user_id}/drives/{drive_id}/items/{item_id}/children'
response = requests.get(url, headers=headers)
contents = response.json()
df = pd.DataFrame(contents['value'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("onedrive") \
    .getOrCreate()
spark_df = spark.createDataFrame(df)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_m365_onedrive_sample"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
