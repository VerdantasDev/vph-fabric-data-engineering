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
import sys
import json
import requests
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql.functions import when, col, lit

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


def get_url_response(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.content


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    today = datetime.today()

    year_str = today.year
    month_str = today.strftime("%m")
    day_str = today.strftime("%d")

    error_log = []
    url = 'https://graph.microsoft.com/v1.0'
    src_table_path = "Tables/br_m365_sharepoint_download_file_status"
    tgt_table_path = 'Tables/br_m365_sharepoint_download_file_status'
    directory_path = f'/lakehouse/default/Files/bronze/microsoft365/sharepoint/{year_str}/{month_str}/{day_str}'

    os.makedirs(directory_path, exist_ok=True)

    key_vault_uri = 'https://vpc-dev-keyvault.vault.azure.net/'

    client_id = get_secrets_from_KV(key_vault_uri,'vpc-sharepoint-data-access-client-id')
    client_secret = get_secrets_from_KV(key_vault_uri,'vpc-sharepoint-data-access-client-secret')
    tenant_id = get_secrets_from_KV(key_vault_uri,'vpc-sharepoint-data-access-tenant-id')

    headers = get_access_token(tenant_id, client_id, client_secret)

    source_df = spark.read.load(src_table_path)
    # source_df = source_df.filter("download_status = 0")
    source_df = source_df.withColumn('custom_tags', source_df.custom_tags.cast('string'))

    file_urls = [row.asDict() for row in source_df.collect()]

    for file_info in file_urls:
        try:
            timestamp = int(datetime.now().timestamp())

            site_id = file_info['site_id']
            drive_id = file_info['drive_id']
            file_id = file_info['id']
            modified_date = file_info['last_modified_datetime']

            file_name = file_info['display_name']
            file_suffix = '.' + file_name.split('.')[-1]
            save_file_name = file_name.replace(file_suffix, '').replace('.', '') + '||' + str(timestamp) + file_suffix

            file_download_url = f"{url}/sites/{site_id}/drives/{drive_id}/items/{file_id}/content"
            content = get_url_response(file_download_url, headers)

            full_path = os.path.join(directory_path, save_file_name)

            # with open(full_path, 'wb') as file:
            #     file.write(content)

            source_df = source_df.withColumn('download_status', when(source_df.id == file_id, lit(1)).otherwise(source_df.download_status))\
                                .withColumn('bronze_save_path', when(source_df.id == file_id, lit(full_path)).otherwise(source_df.bronze_save_path))
        except Exception as e:
            print('Error', e)
            error_log.append(file_name)

    merge_keys = [column for column in source_df.columns if column != 'custom_tags']

    if DeltaTable.isDeltaTable(spark, tgt_table_path):
        
        tgt_table = DeltaTable.forPath(spark, tgt_table_path)

        merge_condition = "and".join(
            [f" target.{col} = updates.{col} " for col in merge_keys]
        )

        tgt_table.alias('target').merge(
            source=source_df.alias("updates"), condition=merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    else:
        source_df.write.format("delta").mode("append").save(tgt_table_path)

except Exception as e:
    print('Error', e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(error_log)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
