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

%run m365_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import libraries
import os
import requests
import pandas as pd
import sys
from datetime import datetime
from delta.tables import DeltaTable
# sys.path.insert(0, '/lakehouse/default/Files/Configs')
# from m365_utils import *
# from m365_config import *

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

access_token = get_access_token(tenant_id, client_id, client_secret_value)
headers = get_headers(access_token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_df = spark.read.format("delta").load("abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Tables/br_m365_sharepoint_download_file_status").filter("download_status = 0")

file_url_df = source_df.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

today = datetime.today()
today_str = today.strftime("%Y-%mm-%d")
year_str = today.year
month_str = today.strftime("%m")
day_str = today.strftime("%d") 
os.makedirs(f'/lakehouse/default/Files/bronze/microsoft365/sharepoint/{year_str}/{month_str}/{day_str}',exist_ok=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_urls = file_url_df.to_dict(orient='records')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for file_info in file_urls:
    try:
        timestamp = int(datetime.now().timestamp())

        site_id = file_info['site_id']
        drive_id = file_info['drive_id']
        file_id = file_info['id']
        file_name = file_info['display_name']
        file_suffix = '.' + file_name.split('.')[-1]
        save_file_name = file_name.replace(file_suffix, '').replace('.', '') + '||' + str(timestamp) + file_suffix

        modified_date = file_info['last_modified_datetime']

        file_download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}/content"

        response = requests.get(file_download_url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Create directory if it doesn't exist
        directory_path = f'/lakehouse/default/Files/bronze/microsoft365/sharepoint/{year_str}/{month_str}/{day_str}'
        os.makedirs(directory_path, exist_ok=True)

        # Save the file
        full_path = os.path.join(directory_path, save_file_name)
        with open(full_path, 'wb') as file:
            file.write(response.content)

        # Update DataFrame
        file_url_df.loc[file_url_df['id'] == file_id, 'download_status'] = 1
        file_url_df.loc[file_url_df['id'] == file_id, 'bronze_save_path'] = full_path

    except requests.exceptions.RequestException as e:
        print(f"Failed to download file with ID {file_info['id']}: {e}")
    except IOError as e:
        print(f"Failed to save file with ID {file_info['id']}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_url_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

file_url_df['custom_tags'] = file_url_df['custom_tags'].apply(lambda x: str(x))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

spark_df = spark.createDataFrame(file_url_df)

columns_to_merge = [col(column) for column in spark_df.columns if column != 'custom_tags']
columns_to_merge_str = [column for column in spark_df.columns if column != 'custom_tags']

# Create a DataFrame excluding 'custom_tags' column
spark_df_filtered = spark_df.select(columns_to_merge)

delta_table = DeltaTable.forPath(spark, 'abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Tables/br_m365_sharepoint_download_file_status')

# Create a similar DataFrame for delta_table, excluding 'custom_tags' column
delta_table_df = delta_table.toDF().select(*columns_to_merge_str)

# Convert both DataFrames to have the same column names
spark_df_filtered = spark_df_filtered.select(*columns_to_merge_str)
delta_table_df = delta_table_df.select(*columns_to_merge_str)

# Perform the merge operation
delta_table.alias("delta_table").merge(
    spark_df_filtered.alias("spark_df_filtered"),
    "delta_table.id = spark_df_filtered.id"
).whenMatchedUpdate(
    condition="true",  # Ensure this is set to update all rows; adjust based on your needs
    set={
        column: col(f"spark_df_filtered.{column}") for column in columns_to_merge_str
    }
).execute()

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
