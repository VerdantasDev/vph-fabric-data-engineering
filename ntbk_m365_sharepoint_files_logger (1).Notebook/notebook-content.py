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

%run m365_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import re
import sys
import json
import requests
import pandas as pd
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, col, when, size, length
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark session
spark = SparkSession.builder.appName("LogTableExample").getOrCreate()

# Define schema for the log table
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("file_id", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("details", StringType(), True)
])

log_table_path = "Tables/log_table_m365_sharepoint_pl"

if DeltaTable.isDeltaTable(spark, log_table_path):
    # If the table exists, append to it
    log_df = spark.createDataFrame([], schema)
    log_df.write.format("delta").mode("append").save(log_table_path)
else:
    # If the table doesn't exist, create it and overwrite any existing data
    log_df = spark.createDataFrame([], schema)
    log_df.write.format("delta").mode("overwrite").save(log_table_path)

def log_operation(spark, log_table_path, file_id, operation, details):
    log_entry = [(datetime.now(), file_id, operation, details)]
    log_df = spark.createDataFrame(log_entry, schema=["timestamp", "file_id", "operation", "details"])
    log_df.write.format("delta").mode("append").save(log_table_path)

# Example usage:
# log_operation(spark, log_table_path, "file1", "upload", "Uploaded file1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def convert_tag(tags):
    """
    Converts dictionary keys from a format with Unicode escape sequences to a human-readable format.

    Args:
        tags (dict): A dictionary where keys are strings containing Unicode escape sequences 
                     and values are associated data.

    Returns:
        dict: A new dictionary with keys decoded from Unicode escape sequences to plain text.
    """
    result = {}
    for k, v in tags.items():
        unicode_str = re.sub(r'_x([0-9A-F]{4})_', r'\\u\1', k)
        decoded_str = unicode_str.encode('ascii').decode('unicode_escape')
        result[decoded_str] = v
    return result


def get_url_response(url, headers):
    """
    Sends a GET request to the specified URL and returns the response as JSON.

    Args:
        url (str): The URL to send the GET request to.
        headers (dict): A dictionary of HTTP headers to include in the request.

    Returns:
        dict: The JSON response from the URL.
    
    Raises:
        HTTPError: If the HTTP request returns an unsuccessful status code.
    """
    response = requests.get(url, headers=headers, timeout=None)
    response.raise_for_status()
    return response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_site_id(base_url, headers, hostname, site_name):
    """
    Fetches the site ID from the API based on the given hostname and site name.

    Args:
        base_url (str): The base URL of the API.
        headers (dict): The headers to include in the API request.
        hostname (str): The hostname of the site.
        site_name (str): The name of the site.

    Returns:
        dict: The API response containing site details.
    """
    url = f'{base_url}/sites/{hostname}:/sites/{site_name}'
    response = get_url_response(url, headers)
    return response


def get_drive_id(base_url, headers, site_id, req_drive_name):
    """
    Retrieves the drive ID for a specified drive name within a site.

    Args:
        base_url (str): The base URL of the API.
        headers (dict): The headers to include in the API request.
        site_id (str): The ID of the site.
        req_drive_name (str): The name of the drive.

    Returns:
        str or None: The ID of the drive if found, otherwise None.
    """
    url = f'{base_url}/sites/{site_id}/drives'
    drives_data = get_url_response(url, headers)

    for drive in drives_data.get('value', []):
        if drive.get('name') == req_drive_name:
            return drive.get('id')

    return None


def get_list_id(base_url, headers, site_id, req_drive_name):
    """
    Retrieves the list ID for a specified list name within a site.

    Args:
        base_url (str): The base URL of the API.
        headers (dict): The headers to include in the API request.
        site_id (str): The ID of the site.
        req_drive_name (str): The name of the list.

    Returns:
        str or None: The ID of the list if found, otherwise None.
    """
    url = f'{base_url}/sites/{site_id}/lists'    
    list_data = get_url_response(url, headers)
    
    for lst in list_data.get('value', []):
        if lst.get('name') == req_drive_name or lst.get('displayName') == req_drive_name:
            return lst.get('id')

    return None


def get_folders_from_drive(base_url, headers, drive_id):
    """
    Retrieves a dictionary of folders (name to ID) from a specified drive.

    Args:
        base_url (str): The base URL of the API.
        headers (dict): The headers to include in the API request.
        drive_id (str): The ID of the drive.

    Returns:
        dict: A dictionary where keys are folder names and values are folder IDs.
    """
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
    """
    Retrieves a dictionary of subfolders (name to ID) from a specified folder within a drive.

    Args:
        base_url (str): The base URL of the API.
        headers (dict): The headers to include in the API request.
        drive_id (str): The ID of the drive.
        folder_id (str): The ID of the folder.

    Returns:
        dict: A dictionary where keys are subfolder names and values are subfolder IDs.
    """
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
    """
    Retrieves custom column names from a specified list in a SharePoint site.

    Args:
        base_url (str): The base URL of the SharePoint site.
        headers (dict): A dictionary of HTTP headers to include in the request.
        site_id (str): The ID of the site containing the list.
        list_id (str): The ID of the list from which to retrieve column data.

    Returns:
        list: A list of custom column names that are not standard columns or field types.
    """
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

def get_tags_for_file(base_url, headers, site_id, drive_id, file_id, custom_tags):
    """
    Retrieves tags for a specific file from a SharePoint site and returns them in a dictionary.

    Args:
        base_url (str): The base URL of the SharePoint site.
        headers (dict): A dictionary of HTTP headers to include in the request.
        site_id (str): The ID of the site containing the list.
        drive_id (str): The ID of the drive where the file is stored.
        file_id (str): The ID of the file for which to retrieve tags.

    Returns:
        dict: A dictionary where keys are custom tag names and values are lists of tag labels.
    """
    tags_str = ','.join(custom_tags)
    tags_dict = {}
    url = f'{base_url}/drives/{drive_id}/items/{file_id}/listItem/fields?$select={tags_str}'
    
    try:
        fields_data = get_url_response(url, headers)
    except requests.exceptions.RequestException as e:
        if e.response.status_code == 504:
            print(f"504 Gateway Timeout for file {file_id}. Trying to retrieve fields individually.")
            fields_data = {}
            for ct in custom_tags:
                try:
                    url = f'{base_url}/drives/{drive_id}/items/{file_id}/listItem/fields?$select={ct}'
                    single_field_data = get_url_response(url, headers)
                    fields_data[ct] = single_field_data
                except requests.exceptions.RequestException as inner_e:
                    print(f"Failed to retrieve {ct} field data for file {file_id}: {inner_e}")
        else:
            print(f"Failed to retrieve fields data for file {file_id}: {e}")
            # Optionally re-raise the exception or handle it further
            fields_data = {}

    # custom_tags = get_custom_tags(base_url, headers, site_id, list_id)

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

def get_list_item_id(base_url, headers, site_id, drive_id, file_id):
    """
    Retrieves list ID for a specific file from a SharePoint site.

    Args:
        base_url (str): The base URL of the SharePoint site.
        headers (dict): A dictionary of HTTP headers to include in the request.
        site_id (str): The ID of the site containing the list.
        drive_id (str): The ID of the drive where the file is stored.
        file_id (str): The ID of the file for which to retrieve the list item.

    Returns:
        str: The list item ID for the SharePoint file, or None if retrieval fails.
    """
    url = f'{base_url}/drives/{drive_id}/items/{file_id}/listItem/id'
    
    try:
        # Send the request and get the response
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check if the request was successful

        # Parse the response data
        list_item_data = response.json()
        
        # Extract the list item ID
        list_item_id = list_item_data.get('value')
        if not list_item_id:
            print(f"List item ID not found for file {file_id}.")
            return None
        
        return list_item_id

    except requests.exceptions.RequestException as e:
        print(f"Request error while retrieving list item ID for file {file_id}: {e}")
        return None
    except ValueError as e:
        # Catch JSON decoding issues
        print(f"Error decoding JSON response for file {file_id}: {e}")
        return None
    except KeyError as e:
        # Catch missing 'id' in response data
        print(f"Key error: 'id' not found in response for file {file_id}: {e}")
        return None
    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_items_in_folder(base_url, headers, site_id, drive_id, folder_id, custom_tags):
    """
    Retrieves detailed information about items within a specified folder in a SharePoint drive.

    Args:
        base_url (str): The base URL of the SharePoint site.
        headers (dict): A dictionary of HTTP headers to include in the request.
        site_id (str): The ID of the site containing the list.
        drive_id (str): The ID of the drive where the folder is located.
        list_id (str): The ID of the list for retrieving custom tags.
        folder_id (str): The ID of the folder from which to retrieve items.

    Returns:
        list: A list of dictionaries, each containing details about an item (file or folder) in the specified folder.
              Each dictionary includes the display name, creation date, last modified date, ID, SharePoint URL,
              type (file or folder), child count, and custom tags of the item.
    """
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

            list_item_id = get_list_item_id(base_url, headers, site_id, drive_id, content.get('id'))
            fold_dict['list_item_id'] = list_item_id

            tags_dict = get_tags_for_file(base_url, headers, site_id, drive_id, content.get('id'), custom_tags)
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

def get_all_files_from_folder(base_url, headers, site_id, drive_id, folder_id, custom_tags):
    """
    Retrieves all files from a specified folder in a SharePoint drive, including files in nested folders.

    Args:
        base_url (str): The base URL of the SharePoint site.
        headers (dict): A dictionary of HTTP headers to include in the request.
        site_id (str): The ID of the site containing the list.
        drive_id (str): The ID of the drive where the folder is located.
        list_id (str): The ID of the list for retrieving custom tags.
        folder_id (str): The ID of the folder from which to retrieve files.

    Returns:
        DataFrame: A Spark DataFrame containing details about all files within the specified folder and its nested folders.
                   The DataFrame includes columns for display name, creation date, last modified date, ID, SharePoint URL,
                   type (file or folder), child count, and custom tags.
                   If no files are found, an empty DataFrame with the same schema is returned.
    """
    child_details = get_items_in_folder(base_url, headers, site_id, drive_id, folder_id, custom_tags)
    for item in child_details:
        item['custom_tags'] = json.dumps(item['custom_tags'])
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
                child_details_temp = get_items_in_folder(base_url, headers, site_id, drive_id, folder_id, custom_tags)
                for item in child_details_temp:
                    item['custom_tags'] = json.dumps(item['custom_tags'])
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

def get_files_from_drive(base_url, headers, site_id, drive_id, list_id, custom_tags):
    """
    Retrieves a dictionary of folders (name to ID) from a specified drive.

    Args:
        base_url (str): The base URL of the API.
        headers (dict): The headers to include in the API request.
        drive_id (str): The ID of the drive.

    Returns:
        dict: A dictionary where keys are folder names and values are folder IDs.
    """
    url = f'{base_url}/drives/{drive_id}/root/children'

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

            list_item_id = get_list_item_id(base_url, headers, site_id, drive_id, content.get('id'))
            fold_dict['list_item_id'] = list_item_id

            tags_dict = get_tags_for_file(base_url, headers, site_id, drive_id, content.get('id'), custom_tags)
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

def push_to_DB(temp, delta_table_path):
    merge_keys = ['id']
    updates_log = []  # List to hold log messages

    total_rows = temp.count()
    updates_log.append(f"Total rows in new dataframe: {total_rows}")

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        tgt_table = DeltaTable.forPath(spark, delta_table_path)
        merge_condition = " AND ".join([f"target.{col} = updates.{col}" for col in merge_keys])

        # Count existing rows
        existing_rows = tgt_table.toDF().count()
        updates_log.append(f"Existing rows in target table: {existing_rows}")

        # Calculate rows that would be updated
        updates_to_be_made = tgt_table.toDF().alias("target").join(
            temp.alias("updates"), merge_keys, "inner"
        ).filter("target.last_modified_datetime != updates.last_modified_datetime").count()
        
        updates_log.append(f"Rows to be updated: {updates_to_be_made}")

        # Perform merge operation
        tgt_table.alias('target').merge(
            source=temp.alias("updates"),
            condition=merge_condition
        ).whenMatchedUpdate(
            condition="target.last_modified_datetime != updates.last_modified_datetime",
            set={col: f"updates.{col}" for col in temp.columns if col != 'id'}
        ).whenNotMatchedInsertAll().execute()

        # Count new rows after the merge
        new_row_count = tgt_table.toDF().count()

        # Calculate inserted rows
        inserted_rows = new_row_count - existing_rows + updates_to_be_made
        updates_log.append(f"Rows inserted: {inserted_rows}")

    else:
        temp.write.format("delta").mode("append").save(delta_table_path)
        updates_log.append(f"All rows inserted as new. Total rows: {total_rows}")

    # Return the log as a single string
    return "\n".join(updates_log)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# try:
start_time = datetime.now()
log_operation(spark, log_table_path, "N/A", "start", f"Run started at {start_time}")

hostname = 'hullinc.sharepoint.com'
base_url = 'https://graph.microsoft.com/v1.0'

all_file_status_delta_table_path = "Tables/br_m365_sharepoint_all_file_status_resume_v1"
download_file_status_delta_table_path = "Tables/br_m365_sharepoint_download_file_status_resume_v1"

key_vault_uri = 'https://vpc-dev-keyvault.vault.azure.net/'

client_id = get_secrets_from_KV(key_vault_uri,'vpc-sharepoint-data-access-client-id')
client_secret = get_secrets_from_KV(key_vault_uri,'vpc-sharepoint-data-access-client-secret')
tenant_id = get_secrets_from_KV(key_vault_uri,'vpc-sharepoint-data-access-tenant-id')

access_token = get_access_token(tenant_id, client_id, client_secret)
if access_token is not None:
    headers = get_headers(access_token)
else:
    raise Exception("Invalid access token")

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

    custom_tags = get_custom_tags(base_url, headers, site_id, list_id)

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
                    files_df = get_all_files_from_folder(base_url, headers, site_id, drive_id, folder_id, custom_tags)  
                    
                    if schema is None:
                        schema = files_df.schema
                        all_files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                            .withColumn('site_id', lit(site_id)) \
                                            .withColumn('list_id', lit(list_id)) \
                                            .withColumn('data_tag', lit(data_tag))
                    else:
                        files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                        .withColumn('site_id', lit(site_id)) \
                                        .withColumn('list_id', lit(list_id)) \
                                        .withColumn('data_tag', lit(data_tag))
                        all_files_df = all_files_df.union(files_df)

        else:
            if len(folders_id_mapper) > 0:
                for folder_name, folder_id in folders_id_mapper.items():
                    files_df = get_all_files_from_folder(base_url, headers, site_id, drive_id, list_id, folder_id, custom_tags)  

                    if schema is None:
                        schema = files_df.schema
                        all_files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                            .withColumn('site_id', lit(site_id)) \
                                            .withColumn('list_id', lit(list_id)) \
                                            .withColumn('data_tag', lit(data_tag))
                    else:
                        files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                        .withColumn('site_id', lit(site_id)) \
                                        .withColumn('list_id', lit(list_id)) \
                                        .withColumn('data_tag', lit(data_tag))
                        all_files_df = all_files_df.union(files_df)
            else:
                files_df = get_files_from_drive(base_url, headers, site_id, drive_id, list_id, custom_tags)  

                if schema is None:
                        schema = files_df.schema
                        all_files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                            .withColumn('site_id', lit(site_id)) \
                                            .withColumn('list_id', lit(list_id)) \
                                            .withColumn('data_tag', lit(data_tag))
                else:
                    files_df = files_df.withColumn('drive_id', lit(drive_id)) \
                                    .withColumn('site_id', lit(site_id)) \
                                    .withColumn('list_id', lit(list_id)) \
                                    .withColumn('data_tag', lit(data_tag))
                    all_files_df = all_files_df.union(files_df)

        print("File count Processed", all_files_df.count())
    
    else:
        raise Exception('List-id or Drive-id Not Found')

all_files_df = all_files_df.drop('type', 'child_count')
all_files_df = all_files_df.withColumn('to_download', when(length(col('custom_tags')) > 2, 'yes').otherwise('no'))

files_download_df = all_files_df.filter(all_files_df['to_download'] == 'yes')
files_download_df = files_download_df.withColumn('download_status', lit(0))\
                                        .withColumn('bronze_save_path', lit(''))
print("Files retrieved. Now pushing to DB")

# write_data(all_files_df, all_file_status_delta_table_path)
# write_data(files_download_df, download_file_status_delta_table_path)

# Push to the first table and capture the update summary
all_updates_summary = push_to_DB(all_files_df, all_file_status_delta_table_path)

# Push to the second table and capture the update summary
download_updates_summary = push_to_DB(files_download_df, download_file_status_delta_table_path)

print("Data successfully updated in DB")

# Consolidate both update summaries into one string
all_update_logs = f"Updates for all_files_df:\n{all_updates_summary}\n\n"
all_update_logs += f"Updates for files_download_df:\n{download_updates_summary}"

# # Log the consolidated updates
log_operation(spark, log_table_path, "N/A", "update_summary", all_update_logs)

# # Capture the end time and log it
end_time = datetime.now()
log_operation(spark, log_table_path, "N/A", "end", f"Run ended at {end_time}")

# except Exception as e:
#     print('Error - ', e)

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
