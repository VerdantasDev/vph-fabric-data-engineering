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

def get_site_id(access_token, headers, hostname, site_path):

    url = f'https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}'
    
    try:
        # Make the GET request to the Microsoft Graph API
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse the response JSON
        site_data = response.json()
        
        # Return the site ID if available
        return site_data.get('id', None)
    
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except KeyError as e:
        print(f"Missing expected data in response: {e}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def list_drives_in_spo(headers, site_id):
    # Construct the URL to list drives in the SharePoint site
    url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives'
    
    try:
        # Make the GET request to the Microsoft Graph API
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse and return the JSON response
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_drive_id(req_drive_name, drives_data):

    # Loop through the drives in the drives_data
    for drive in drives_data.get('value', []):
        if drive.get('name') == req_drive_name:
            return drive.get('id')
    
    # Return None if the drive name is not found
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_list_id(headers, site_id, req_drive_name):

    # Construct the URL to get the lists
    url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists'
    
    try:
        # Make the GET request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse the JSON response
        list_data = response.json()
        
        # Loop through the lists to find the requested list name
        for lst in list_data.get('value', []):
            if lst.get('name') == req_drive_name or lst.get('displayName') == req_drive_name:
                return lst.get('id')
    
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    
    # Return None if the list name is not found or if an error occurs
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_folders_from_drive(headers, drive_id):

    # Base URL for the drive root children endpoint
    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children'
    folders_data_dict = dict()
    
    while url:
        try:
            # Make the GET request
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            # Parse the JSON response
            folders_data = response.json()
            
            # Process the items in the response
            for item in folders_data.get('value', []):
                if 'folder' in item:
                    folders_data_dict[item['name']] = item['id']
            
            # Check if there is a next page of results
            url = folders_data.get('@odata.nextLink', None)
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break
    
    return folders_data_dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def get_custom_tags(fields_data):

#     standard_fields = [
#         '@odata.context', '@odata.etag', 'id', 'ContentType', 'Created', 'AuthorLookupId', 'Modified',
#         'EditorLookupId', '_CheckinComment', 'LinkFilenameNoMenu', 'LinkFilename', 'DocIcon', 'FileSizeDisplay',
#         'ItemChildCount', 'FolderChildCount', '_ComplianceFlags', '_ComplianceTag', '_ComplianceTagWrittenTime',
#         '_ComplianceTagUserId', '_CommentCount', '_LikeCount', '_DisplayName', 'Edit', '_UIVersionString',
#         'ParentVersionStringLookupId', 'ParentLeafNameLookupId','FileLeafRef',
#         'Title', 'MediaServiceImageTags', 'AppAuthorLookupId', 'AppEditorLookupId'
#     ]
    
#     # Filter out standard fields
#     custom_fields = {key: value for key, value in fields_data.items() if key not in standard_fields}

#     return custom_fields

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_custom_tags(headers, site_id, list_id):

    # Base URL for retrieving columns from the list
    url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns'
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
        try:
            # Make the GET request
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            # Parse the JSON response
            fields_data = response.json()
            
            # Process the fields in the response
            for field in fields_data.get('value', []):
                if not field.get('hidden', False) and \
                   not field.get('readOnly', False) and \
                   not field.get('required', False) and \
                   field.get('name') not in standard_column_names and \
                   field.get('type') not in standard_field_types:
                    custom_fields.append(field.get('name'))
            
            # Check if there is a next page of results
            url = fields_data.get('@odata.nextLink', None)
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break
    
    return custom_fields

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_tags_for_file(headers, drive_id, file_id, site_id, list_id):

    tags_dict = {}
    fields_url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/listItem/fields'

    try:
        # Make the GET request to retrieve the fields
        fields_response = requests.get(fields_url, headers=headers)
        fields_response.raise_for_status()  # Raise an exception for HTTP errors
        fields_data = fields_response.json()

    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve fields data for file {file_id}: {e}")
        return tags_dict

    # Retrieve custom tags
    custom_tags = get_custom_tags(headers, site_id, list_id)

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

def get_items_in_folder(drive_id, folder_id, headers, site_id, list_id):

    # Base URL for retrieving items in the folder
    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children'
    child_details = []

    while url:
        try:
            # Make the GET request
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            # Parse the JSON response
            contents = response.json()
            
            # Process the items in the response
            for c in contents.get('value', []):
                fold_dict = {
                    'display_name': c.get('name'),
                    'created_datetime': c.get('createdDateTime'),
                    'last_modified_datetime': c.get('lastModifiedDateTime'),
                    'id': c.get('id'),
                    'sharepoint_url': c.get('webUrl')
                }

                tags_dict = get_tags_for_file(
                    headers, drive_id, c.get('id'), site_id, list_id)
                fold_dict['custom_tags'] = tags_dict

                if 'folder' in c:
                    fold_dict['type'] = 'folder'
                    fold_dict['child_count'] = c['folder'].get('childCount', 0)
                else:
                    fold_dict['type'] = 'file'
                    fold_dict['child_count'] = 0

                child_details.append(fold_dict)
            
            # Check if there is a next page of results
            url = contents.get('@odata.nextLink', None)
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break
    
    # Convert the list of dictionaries to a DataFrame
    child_df = pd.DataFrame(child_details)

    return child_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_all_files_from_folder(headers, drive_id, folder_id, site_id, list_id):

    all_files_df = pd.DataFrame()
    folders_df = pd.DataFrame()

    # Initial retrieval of items in the folder
    child_df = get_items_in_folder(drive_id, folder_id, headers, site_id, list_id)

    if not child_df.empty:
        # Separate folders and files
        folders_df = pd.concat([folders_df, child_df[child_df['type'] == 'folder']], ignore_index=True)
        folders_df['parsed'] = False
        all_files_df = pd.concat([all_files_df, child_df[child_df['type'] == 'file']], ignore_index=True)

        while not folders_df[folders_df['parsed'] == False].empty:
            # Process each folder
            for i in folders_df.index:
                if folders_df.loc[i, 'parsed']:
                    continue

                if folders_df.loc[i, 'child_count'] == 0:
                    folders_df.loc[i, 'parsed'] = True
                    continue

                folder_id = folders_df.loc[i, 'id']
                child_df_temp = get_items_in_folder(drive_id, folder_id, headers, site_id, list_id)

                if not child_df_temp.empty:
                    # Add new folders and files
                    folders_df_temp = child_df_temp[child_df_temp['type'] == 'folder'].copy()
                    folders_df_temp['parsed'] = False
                    folders_df = pd.concat([folders_df, folders_df_temp], ignore_index=True)
                    all_files_df = pd.concat([all_files_df, child_df_temp[child_df_temp['type'] == 'file']], ignore_index=True)

                # Mark the current folder as parsed
                folders_df.loc[i, 'parsed'] = True

    return all_files_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_subfolders(drive_id, folder_id, headers):

    folders_data_dict = {}
    url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children'

    while url:
        try:
            # Make the GET request
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            # Parse the JSON response
            contents = response.json()
            
            # Process the items in the response
            for item in contents.get('value', []):
                if 'folder' in item:
                    folders_data_dict[item.get('name')] = item.get('id')
            
            # Check if there is a next page of results
            url = contents.get('@odata.nextLink', None)
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break
    
    return folders_data_dict


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

url = f'https://graph.microsoft.com/v1.0/drives/b!F-BiFCVXhkO8rQsmnzyGjS16NV7kIFJAgSC5oa5jElZFMeuZrNArQryRjqSbvFED/items/01LC474ITKFCBAZSN75FCYLCTWF6JCX2JS/listItem/fields?$select=Engineering'
response = requests.get(url, headers=headers)
data = response.json()
data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

custom_tags = get_custom_tags(headers, site_id, list_id)
custom_tags

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_files_df = pd.DataFrame()
for path in sites_to_scrape:
    print(path)
    site_name, folder_path = path.split('/', maxsplit = 1)

    hostname = 'hullinc.sharepoint.com'
    site_path = f'/sites/{site_name}'

    site_id = get_site_id(access_token, headers, hostname, site_path)
    drives_data = list_drives_in_spo(headers, site_id)

    req_drive_name = folder_path.split('/')[0]

    drive_id = get_drive_id(req_drive_name, drives_data)

    list_id = get_list_id(headers, site_id, req_drive_name)

    all_folders = get_folders_from_drive(headers, drive_id)

    data_tag = path.split('/')[-1]

    if len(folder_path.split('/'))>1:
        
        hierarchy_list = folder_path.split('/')[1:]
        folder_name = hierarchy_list[0]
        folder_id = all_folders[folder_name]
        
        for i in range(len(hierarchy_list)):

            folder_name = hierarchy_list[i]
            if i==0:
                folder_id = all_folders[folder_name]
            else:
                folder_id = subfolders_dict[folder_name]
            
            if i != len(hierarchy_list)-1:
                subfolders_dict = get_subfolders(drive_id, folder_id, headers)
            else:
                files_df = get_all_files_from_folder(headers, drive_id, folder_id, site_id, list_id)   
                files_df['drive_id'] = drive_id
                files_df['site_id'] = site_id      
                files_df['data_tag'] = data_tag       
                all_files_df = pd.concat([all_files_df,files_df], ignore_index = True)
    else:
        for folder_name, folder_id in all_folders.items():
            files_df = get_all_files_from_folder(headers, drive_id, folder_id, site_id, list_id)  
            files_df['drive_id'] = drive_id
            files_df['site_id'] = site_id   
            files_df['data_tag'] = data_tag            
            all_files_df = pd.concat([all_files_df,files_df], ignore_index = True)

    print(len(all_files_df))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_files_df['custom_tags'] = all_files_df['custom_tags'].apply(lambda x: {convert_tag(key): value for key, value in x.items()})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_files_df = all_files_df.drop(labels=['type','child_count'], axis = 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update 'to_download' column based on the condition
all_files_df['to_download'] = all_files_df['custom_tags'].apply(lambda x: 'yes' if x != {} else 'no')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files_download_df = all_files_df[all_files_df['to_download'] == 'yes']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(files_download_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files_download_df['download_status'] = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# adding Resume/RFP tags

# tagging_keywords = {
#     'Resume':['Resume'],
#     'RFP':['RFP','Request for proposal']
# }

# def assign_tag(file_name):
#     for tag, keywords in tagging_keywords.items():
#         for keyword in keywords:
#             if keyword.lower() in file_name.lower():
#                 return tag
#     return 'Other'

# file_url_df['data_tag'] = file_url_df['file_name'].apply(assign_tag)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files_download_df['bronze_save_path'] = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("Sharepoint") \
    .getOrCreate()
spark_df = spark.createDataFrame(all_files_df)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_m365_sharepoint_all_file_status"  # Example Delta Lake table path in Azure Data Lake Storage

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

# CELL ********************

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("Sharepoint") \
    .getOrCreate()
spark_df = spark.createDataFrame(files_download_df)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_m365_sharepoint_download_file_status"  # Example Delta Lake table path in Azure Data Lake Storage

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

# CELL ********************

# from pyspark.sql import SparkSession
 
# # Create a Spark session
# spark = SparkSession.builder \
#     .appName("Sharepoint") \
#     .getOrCreate()
# spark_df = spark.createDataFrame(all_files_df)
# # spark_df.write.format("delta").mode("append").save('abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Tables/br_m365_sharepoint_file_status_new')
# spark_df.write.mode("overwrite").format("delta").saveAsTable("br_m365_sharepoint_file_status_new")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# today = datetime.today()
# today_str = today.strftime("%Y-%mm-%d")
# year_str = today.year
# month_str = today.strftime("%m")
# day_str = today.strftime("%d") 
# os.makedirs(f'/lakehouse/default/Files/bronze/microsoft365/sharepoint/{year_str}/{month_str}/{day_str}',exist_ok=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# for file in file_urls:

#     timestamp = datetime.now()
#     timestamp = int(timestamp.timestamp())

#     file_id = file['file_id']
#     file_name = file['file_name']
#     file_suffix = '.'+file_name.split('.')[-1]
#     save_file_name = file_name.replace(file_suffix,'').replace('.','') + '||' + str(timestamp) + file_suffix

#     modified_date = file['file_modified_datetime']

#     file_download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}/content"

#     response = requests.get(file_download_url, headers=headers)
#     if response.status_code == 200:
#         data = response.content
#         full_path = f'/lakehouse/default/Files/bronze/microsoft365/sharepoint/{year_str}/{month_str}/{day_str}/{save_file_name}'

#         with open(full_path, 'wb') as file:
#             file.write(response.content)

#         file_url_df.loc[file_url_df['file_id'] == file_id,'download_status'] = 1


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark_df = spark.createDataFrame(file_url_df)
# delta_table = DeltaTable.forPath(spark, 'abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Tables/br_m365_sharepoint_file_status')
# merge_condition =  "delta_table.file_id = spark_df.file_id"
# delta_table.alias("delta_table").merge(spark_df.alias("spark_df"),
#         merge_condition
#     ).whenMatchedUpdateAll() \
#     .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import requests
# import pandas as pd

# # Initialize the URL for the first request
# url = 'https://graph.microsoft.com/v1.0/sites'

# # Initialize an empty list to hold all site data
# all_sites = []

# # Loop through the pages of results
# while url:
#     # Send GET request to the current URL
#     response = requests.get(url, headers=headers)
    
#     # Parse the JSON response
#     site_data = response.json()
    
#     # Append the current page of site data to the list
#     all_sites.extend(site_data['value'])
    
#     # Check if there is a next page
#     url = site_data.get('@odata.nextLink', None)

# # Create a DataFrame from the accumulated site data
# site_df = pd.DataFrame(all_sites)

# # Display the DataFrame
# display(site_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
