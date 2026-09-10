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

%run ntbk_deltek_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ntbk_deltek_config

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
from pyspark.sql import SparkSession
from tqdm import tqdm
import time

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

df_spo = load_table(lakehouse_path, "tes1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

split_col = F.split(df_spo['OfficeLocation'], ',')

# Create new columns 'city' and 'state' by accessing the array elements
df_spo = df_spo.withColumn('OfficeLocationCity', F.trim(split_col.getItem(0))) \
       .withColumn('OfficeLocationState', F.trim(split_col.getItem(1)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_spo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add custom Tags

# CELL ********************

def update_column_value(site_id, list_id, list_item_id, data, headers):
    url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{list_item_id}/fields'
    response = requests.patch(url, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        print(f"Custom column value updated successfully for item ID: {list_item_id}.")
        return True
    else:
        print(f"Failed to update custom column for item ID: {list_item_id}. Status code: {response.status_code}")
        print(response.text)
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare data for SharePoint update
def prepare_data(row):
    return {
        'EmployeeNumber': row['EmployeeNumber'],
        'Department': row['Department'],
        'Location_x003a_City': row['OfficeLocationCity'],
        'Location_x003a_State': row['OfficeLocationState'],
        'PracticeGroup': row['PracticeGroup'],
        'Area': row['BusinessUnit']
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a Spark session
spark = SparkSession.builder \
    .appName("SharePoint Data Update") \
    .getOrCreate()

access_token = get_access_token(tenant_id, client_id, client_secret_value)
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

failed_updates = []
# Filter the DataFrame to exclude rows where list_item_id is 88
df_filtered = df_spo.filter(df_spo['list_item_id'] == 88)

# Iterate over the filtered DataFrame
for index, row in tqdm(enumerate(df_filtered.toLocalIterator()), total=df_filtered.count(), desc="Updating columns"):
    list_item_id = row['list_item_id']
    list_id = row['list_id']
    site_id = row['site_id']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = prepare_data(row)
data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# data = {'EmployeeNumber': 'V6A160002'}
# update_column_value(site_id, list_id, list_item_id, data, headers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a Spark session
spark = SparkSession.builder \
    .appName("SharePoint Data Update") \
    .getOrCreate()

access_token = get_access_token(tenant_id, client_id, client_secret_value)
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

failed_updates = []

for index, row in tqdm(enumerate(df_spo.toLocalIterator()), total=df_spo.count(), desc="Updating columns"):
    list_item_id = row['list_item_id']
    list_id = row['list_id']
    site_id = row['site_id']
    
    # Prepare data for the current row
    data = prepare_data(row)
    
    # Attempt to update the column value
    if not update_column_value(site_id, list_id, list_item_id, data, headers):
        failed_updates.append(row)
    
    # Optionally, refresh the token periodically (e.g., every 10 updates)
    if (index + 1) % 10 == 0:
        access_token = get_access_token(tenant_id, client_id, client_secret_value)
        headers['Authorization'] = f'Bearer {access_token}'

# After processing all rows, check for failed updates
if failed_updates:
    print(f"\nTotal failed updates: {len(failed_updates)}")
    # You can log or retry failed updates as needed
else:
    print("\nAll updates completed successfully.")

# Stop Spark session
spark.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pd.DataFrame(failed_updates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Extra

# CELL ********************

url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns'

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

# Make the POST request to add the column
response = requests.get(url, headers=headers)
data = response.json()
display(pd.DataFrame(data['value']))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
