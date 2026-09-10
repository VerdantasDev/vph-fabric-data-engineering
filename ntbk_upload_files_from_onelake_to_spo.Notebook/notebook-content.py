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
from urllib.parse import quote

from pyspark.sql.functions import split, col, when, size, lower, lit

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

    if response.status_code == 200:
        access_token = response.json().get('access_token')
    else:
        access_token = None
    
    return access_token


def get_headers(access_token):
    
    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    return headers


def get_url_response(url, headers):

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

def get_spo_target_details(hostname, base_url, spo_location):
    
    access_token = get_access_token(tenant_id, client_id, client_secret)
    
    if access_token is not None:
        headers = get_headers(access_token)
    else:
        raise Exception("Invalid access token")
        
    site_name, folder_path = spo_location.split('/', maxsplit = 1)
    folders = folder_path.split('/')

    site_data = get_site_id(base_url, headers, hostname, site_name)
    site_id = site_data['id']

    req_drive_name = folders[0]
    drive_id = get_drive_id(base_url, headers, site_id, req_drive_name)

    if drive_id is not None:

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
                    target_folder_id = folder_id

        else:
            raise Exception("Folder not defined")
    
    else:
        raise Exception('Drive-id Not Found')
        
    return site_id, drive_id, folder_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_upload_session(access_token, upload_session_url):
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }
    response = requests.post(upload_session_url, headers=headers)
    response.raise_for_status()
    upload_url = response.json().get('uploadUrl')
    
    return upload_url


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upload_file_in_chunks(file_name, upload_url, file_path):
    chunk_size = 327680  # 320 KB per chunk
    file_size = os.path.getsize(file_path)
    chunk_number = 0

    try:
        with open(file_path, 'rb') as f:
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
                if response.status_code in [200, 201, 202]:
                    print(f'chunk uploaded {chunk_number + 1}')
                else:
                    print(f"Failed to upload chunk {chunk_number + 1}: {response.status_code}")
                    return False

                if end == file_size - 1:
                    print(f"Upload completed {file_name}")
                    return True

                chunk_number += 1
                
    except Exception as e:
        raise Exception(e)
        return False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load('Tables/br_acquia_files_ingestion_status_v2')
df = df.filter(df.collection_name == 'Verdantas Resumes')
df = df.filter(df.download_status == 1)
df = df.filter(df.upload_status == 0)
cols = df.columns

supported_extensions = ["docx", 'pdf']

df = df.withColumn("extension", split(col("asset_name"), "\."))\
        .withColumn('arrsize', size(col('extension')))\
        .withColumn('temp', when(col('arrsize') >= 2, col('extension')[col('arrsize')-1]).otherwise(None))\
        .filter(col('temp').isNotNull())\
        .filter(lower(col('temp')).isin(supported_extensions))

df = df.select(*cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    error_logger = []

    tenant_id = '694b700c-7f7a-4dd6-8d36-04e90dfc0d02'
    client_id = '40a18c52-47b1-4244-b372-dc113dcef964'
    client_secret = 'mDM8Q~59WTRhEjCEKkfgH5Xipw4bTznBpV3qRcGj'

    hostname = 'hullinc.sharepoint.com'
    base_url = 'https://graph.microsoft.com/v1.0'
    spo_location = 'ResourcesWorkspace/Resumes/Acquia'
    site_id, tgt_drive_id, tgt_folder_id = get_spo_target_details(hostname, base_url, spo_location)
    print(site_id, tgt_drive_id, tgt_folder_id)

    temp = df.select('asset_name', 'bronze_save_path')
    fileslist = temp.collect()

    for files in fileslist:
        file_name = files['asset_name']
        file_name_encoded = quote(file_name)
        local_file_path = files['bronze_save_path']

        upload_session_url = f"https://graph.microsoft.com/v1.0/drives/{tgt_drive_id}/items/{tgt_folder_id}/children/{file_name_encoded}/createUploadSession"
        access_token = get_access_token(tenant_id, client_id, client_secret)

        try:
            upload_url = create_upload_session(access_token, upload_session_url)

            if os.path.exists(local_file_path):
                print(f"processing {file_name}")
                status = upload_file_in_chunks(file_name, upload_url, local_file_path)
                if status:
                    upload_logger = 1
                else:
                    upload_logger = 0
                    error_logger.append(file_name)
                df = df.withColumn('upload_status', when(df.asset_name == file_name, 1).otherwise(col('upload_status')))
            else:
                error_logger.append(file_name)
        except Exception as e:
            error_logger.append(file_name)
            continue

except Exception as e:
    print(e)
    raise Exception(e)

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
