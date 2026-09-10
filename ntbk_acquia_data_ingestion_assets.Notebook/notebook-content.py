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

%run utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import json
import traceback
import requests
from datetime import datetime

from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

InputParam = """
    {"Verdantas Resumes": "https://api.widencollective.com/v2/assets/search?query=acn%3A%28%7BVerdantas+R%C3%A9sum%C3%A9s%7D%29"}
"""

target_table_path = "Tables/br_acquia_files_ingestion_status_v4"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_authorization_token(url, client_id, redirect_uri):
    """
        Generates authorization token
        Args:
            url (str): Base Acquia URL
            client_id (str): clientid
            redirect_uri (str): Redirect URL
        Returns:
            str: Redirect URL to hit to get the access token

    """

    redirect_url = f"{url}?client_id={client_id}&redirect_uri={redirect_uri}"
    return redirect_url


def get_access_token(url, client_id, client_secret, authorization_code):
    """
        Generates Access token
        Args:
            url (str): URL to hit to get the Access token
            client_id (str): clientid
            client_secret (str): clientsecret
            authorization_code (str): authorization code
        Returns:
            str: Access token
    """

    payload = {
        "grant_type": "authorization_code",
        "authorization_code": authorization_code,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    return response.json()["access_token"]


def get_response_data(url, headers):
    """
        Retrieves data from URL if the response is 4** raises exception
        Args:
            url (str): URL to Ingest data from
            headers (dict): The headers required for the API request.
        Returns:
            dict: Data returned from the URL
    """

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_asset_details(asset, collection_name, headers, local_file_path):
    """
    Retrieves asset details, metadata and downloads the assets in Onelake.

    Args:
        asset (dict): The asset information containing 'id', 'filename', and other details.
        collection_name (str): The name of the collection to which the asset belongs.
        headers (dict): The headers required for the API request.
        local_file_path (str): The local directory path where the asset will be saved.

    Returns:
            dict: Metadata of the asset.
            str: Asset ID.
            int: Download status (1 if successful, 0 otherwise).
    """

    status = 0
    timestamp = int(datetime.now().timestamp())
    asset_id = asset["id"]
    asset_name = asset["filename"]
    
    file_suffix = os.path.splitext(asset_name)[-1]
    save_file_name = f"{asset_name.replace(file_suffix, '').replace('.', '')}||{timestamp}{file_suffix}"
    local_file_path = os.path.join(local_file_path, save_file_name)

    try:
    #     download_link = asset["_links"]["download"]
    #     response = requests.get(download_link)
    #     response.raise_for_status()

    #     with open(local_file_path, "wb") as file:
    #         file.write(response.content)
        status = 1
    except Exception as e:
        print("error", e)
        print(f"Processing for {asset_id} failed")

    url = f"https://api.widencollective.com/v2/assets/{asset_id}/metadata"
    try:

        result = get_response_data(url, headers)
        metadata_result = result.get("fields", {})

        metadata = {
            "asset_id": asset_id,
            "asset_name": asset_name,
            "asset_url":download_link,
            "collection_name": collection_name,
            "assettype": ','.join(metadata_result.get("assettype", "")),
            "client": ','.join(metadata_result.get("client", "")),
            "companyLegacy": ','.join(metadata_result.get("companyLegacy", "")),
            "expertise": ','.join(metadata_result.get("expertise", "")),
            "keywords": str(metadata_result.get("keywords", "")),
            "projectName": ','.join(metadata_result.get("projectName", "")),
            "solutions": ','.join(metadata_result.get("solutions", "")),
            "state": ','.join(metadata_result.get("state", "")),
            "IRecognitionKeywords": str(metadata_result.get("IRecognitionKeywords", "")),
            "asset_created_date": asset["created_date"],
            "asset_modified_date": asset["last_update_date"],
            "bronze_save_path": local_file_path,
            "download_status": status,
        }

        return metadata, asset_id, status
    except Exception as e:
        print(f"Error fetching metadata for {asset_id}: {e}")
        return {}, asset_id, status


def get_asset_metadata(url, collection_name, local_file_path, processed_assets_list, headers):
    """
        Retrieves metadata for assets from a given URL and processes them if they haven't 
        been processed or have been updated.

        Args:
            url (str): The base URL for fetching asset data.
            collection_name (str): The name of the collection to which the assets belong.
            local_file_path (str): The local directory path where assets will be saved.
            processed_assets_list (dict): A dictionary of processed assets with their last processed dates.
            headers (dict): The headers required for the API request.

        Returns:
            list: A list of metadata dictionaries for the processed assets.
            list: A list of asset IDs that failed to process.
    """

    logger = []

    offset = 0
    batch_size = 100
    url = f"{url}&limit=100"

    params_url = f"{url}&offset={offset}"
    result = get_response_data(params_url, headers)
    total_count = result.get("total_count", None)
    assets = result.get("items", [])

    while offset + batch_size < total_count:
        offset += batch_size
        params_url = f"{url}&offset={offset}"
        data = get_response_data(params_url, headers)
        assets.extend(data.get("items", []))

    asset_metadata = []
    for asset in assets:
        fileid = asset["id"]
        last_update_date = datetime.strptime(asset['last_update_date'], '%Y-%m-%dT%H:%M:%SZ').date()
        processed_date = processed_assets_list.get(fileid)
        # print(asset["id"])
        # if processed_date is None or processed_date < last_update_date:
        #     metadata, asset_id, status = get_asset_details(
        #         asset, collection_name, headers, local_file_path
        #     )
        #     if status == 1:
        #         asset_metadata.append(metadata)
        #     else:
        #         logger.append(asset_id)
        # else:
        #     print(f"File - {asset['filename']}, already processed")

    return asset_metadata, logger


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_target_schema():
    """
        Defines the schema for the target table.
        Returns:
            spark schema: Target table schema
    """

    schema = StructType([
        StructField('asset_id', StringType(), True),
        StructField('asset_name', StringType(), True),
        StructField('asset_url', StringType(), True),
        StructField('collection_name', StringType(), True),
        StructField('assettype', StringType(), True),
        StructField('client', StringType(), True),
        StructField('companyLegacy', StringType(), True),
        StructField('expertise', StringType(), True),
        StructField('keywords', StringType(), True),
        StructField('projectName', StringType(), True),
        StructField('solutions', StringType(), True),
        StructField('state', StringType(), True),
        StructField('IRecognitionKeywords', StringType(), True),
        StructField('asset_created_date', StringType(), True),
        StructField('asset_modified_date', StringType(), True),
        StructField('bronze_save_path', StringType(), True),
        StructField('download_status', IntegerType(), True)
    ])
    return schema


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
    Processes collections to retireve assets and their metadata information
"""
try:

    asset_queries_map = json.loads(InputParam)
    collection_name = list(asset_queries_map.keys())[0]
    asset = asset_queries_map[collection_name]

    today = datetime.today().date()

    year_str = today.year
    month_str = today.strftime("%m")
    day_str = today.strftime("%d")
    root_path = f"/lakehouse/default/Files/bronze/Acquia/{year_str}/{month_str}/{day_str}"

    os.makedirs(root_path, exist_ok=True)

    merge_keys = ['asset_id', 'collection_name']
    partition_keys = ['collection_name']

    if DeltaTable.isDeltaTable(spark, target_table_path):
        df = spark.read.load(target_table_path)
        df = df.filter((df.download_status == 1) & (df.collection_name == collection_name))
        df = df.select(df.asset_id, df.asset_modified_date)
        
        processed_assets_list = {}
        for processed_asset in df.collect():
            modified_dt = datetime.strptime(processed_asset['asset_modified_date'], '%Y-%m-%dT%H:%M:%SZ').date()
            processed_assets_list[processed_asset['asset_id']] = modified_dt
    else:
        processed_assets_list = {}

    client_id = "961414658ff4034132e64fd9ef2cc75af32c0ee1.app.widen.com"
    client_secret = "10106ba3f2b66bef7f6de7dfbdf9b190cf7583d9"
    redirect_uri = "https://verdantas.acquiadam.com/auth/acquia/callback"

    authorization_base_url = "https://verdantas.widencollective.com/allowaccess"
    access_token_base_url = "https://api.widencollective.com/v2/oauth/access-token"

    access_token = "wat_verdantas_0c79e80b6d5e97fe79aba3c48f894eb0"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    asset_metadata, logger = get_asset_metadata(
        asset, collection_name, root_path, processed_assets_list, headers
    )

    schema = get_target_schema()
    df = spark.createDataFrame(asset_metadata, schema=schema)
    df = df.withColumn('created_on', lit(today))

    # write_to_target(df, target_table_path, merge_keys, partition_keys)

except Exception as e:
    print('Error', e)
    raise Exception(e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(logger)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
