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

%run m365_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import re
import sys
import requests
import pandas as pd
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql.functions import lit, col, when, size

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

def get_site_id(base_url, headers, hostname, site_name):

    url = f'{base_url}/sites/{hostname}:/sites/{site_name}'
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

def get_custom_tags(base_url, headers, site_id, list_id):

    url = f'{base_url}/sites/{site_id}/lists/{list_id}/columns'
    custom_fields = []

    standard_column_names = {
        "Title", "Modified", "Created", "Author", "Editor", "ContentType", "ContentTypeId",
        "ID", "Attachments", "Created By", "Modified By", "Version", "_ExtendedDescription",
        "MediaServiceImageTags"
    }

    standard_field_types = {
        "ContentTypeId", "Computed", "Attachments", "Note", "Counter", "CrossProjectLink"
    }

    while url:

        fields_data = get_url_response(url, headers)

        for field in fields_data.get('value', []):
            if not field.get('hidden', False) and \
                not field.get('readOnly', False) and \
                not field.get('required', False) and \
                field.get('name') not in standard_column_names and \
                field.get('type') not in standard_field_types:
                custom_fields.append(field.get('name'))
        
        url = fields_data.get('@odata.nextLink', None)
        

    return custom_fields

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_tags_for_file(base_url, headers, site_id, list_id, drive_id, file_id):

    tags_dict = {}
    url = f'{base_url}/drives/{drive_id}/items/{file_id}/listItem/fields'
    fields_data = get_url_response(url, headers)

    custom_tags = get_custom_tags(base_url, headers, site_id, list_id)

    for tag in custom_tags:
        tag_value = fields_data.get(tag)
        tags_list = []

        if isinstance(tag_value, list):
            tags_list = [tl.get('Label') for tl in tag_value if isinstance(tl, dict) and 'Label' in tl]
        elif isinstance(tag_value, dict):
            tags_list = [tag_value.get('Label')]
        elif isinstance(tag_value, str):
            tags_list = [tag_value]

        tags_dict[tag] = tags_list

    return tags_dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_items_in_folder(base_url, headers, site_id, drive_id, list_id, folder_id):

    url = f'{base_url}/drives/{drive_id}/items/{folder_id}/children'
    child_details = []

    while url:

        contents = get_url_response(url, headers)
        
        for content in contents.get('value', []):
            fold_dict = {
                'display_name': content.get('name'),
                'created_datetime': content.get('createdDateTime'),
                'last_modified_datetime': content.get('lastModifiedDateTime'),
                'id': content.get('id'),
                'sharepoint_url': content.get('webUrl')
            }

            tags_dict = get_tags_for_file(base_url, headers, site_id, list_id, drive_id, content.get('id'))
            tags_dict = convert_tag(tags_dict)
            fold_dict['custom_tags'] = tags_dict

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

def get_all_files_from_folder(base_url, headers, site_id, drive_id, list_id, folder_id):

    child_details = get_items_in_folder(base_url, headers, site_id, drive_id, list_id, folder_id)
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
                child_details_temp = get_items_in_folder(base_url, headers, site_id, drive_id, list_id, folder_id)
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

def write_data(df, delta_table_path):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    hostname = 'hullinc.sharepoint.com'
    base_url = 'https://graph.microsoft.com/v1.0'

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
                folder_name = hierarchy_list[0]
                folder_id = folders_id_mapper[folder_name]
                
                for i in range(len(hierarchy_list)):

                    folder_name = hierarchy_list[i]
                    if i==0:
                        folder_id = folders_id_mapper[folder_name]
                    else:
                        folder_id = subfolders_dict[folder_name]
                    
                    if i != len(hierarchy_list)-1:
                        subfolders_dict = get_subfolders(base_url, headers, drive_id, folder_id)
                    else:
                        files_df = get_all_files_from_folder(base_url, headers, site_id, drive_id, list_id, folder_id)  
                        
                        if schema is None:
                            schema = files_df.schema
                            all_files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                                .withColumn('site_id', lit(site_id)) \
                                                .withColumn('data_tag', lit(data_tag))
                        else:
                            files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                            .withColumn('site_id', lit(site_id)) \
                                            .withColumn('data_tag', lit(data_tag))
                            all_files_df = all_files_df.union(files_df)

            else:
                for folder_name, folder_id in folders_id_mapper.items():
                    files_df = get_all_files_from_folder(base_url, headers, site_id, drive_id, list_id, folder_id)  

                    if schema is None:
                        schema = files_df.schema
                        all_files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                            .withColumn('site_id', lit(site_id)) \
                                            .withColumn('data_tag', lit(data_tag))
                    else:
                        files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                        .withColumn('site_id', lit(site_id)) \
                                        .withColumn('data_tag', lit(data_tag))
                        all_files_df = all_files_df.union(files_df)
            
            print("File count Processed", all_files_df.count())
        
        else:
            raise Exception('List-id or Drive-id Not Found')

    all_files_df = all_files_df.drop('type', 'child_count')
    all_files_df = all_files_df.withColumn('to_download', when(size(all_files_df['custom_tags']) > 0, 'yes').otherwise('no'))

    files_download_df = all_files_df.filter(all_files_df['to_download'] == 'yes')
    files_download_df = files_download_df.withColumn('download_status', lit(0))\
                                         .withColumn('bronze_save_path', lit(''))

    # write_data(all_files_df, all_file_status_delta_table_path)
    # write_data(files_download_df, download_file_status_delta_table_path)

except Exception as e:
    print('Error - ', e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(all_files_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(files_download_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
