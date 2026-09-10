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
import requests
import unicodedata
from datetime import datetime

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

    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()
        access_token = response.json()["access_token"]
        return access_token
    except requests.exceptions.RequestException as e:
        raise


def get_response_data(url, headers):
    """
        Retrieves data from URL if the response is 4** raises exception
        Args:
            url (str): URL to Ingest data from
            headers (dict): The headers required for the API request.
        Returns:
            dict: Data returned from the URL
    """
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_assets_queries(url, headers):
    """
        Retrieves data from URL if the response is 4** raises exception
        Args:
            url (str): URL to Ingest data from
            headers (dict): The headers required for the API request.
        Returns:
            list: of retrieved assets with collections name as a dict
    """

    offset = 0
    batch_size = 100
    assets = []

    while True:
        params_url = f"{url}&offset={offset}"
        try:
            result = get_response_data(params_url, headers)
            total_count = result.get("total_count")
            new_assets = result.get("items", [])
        
            if not new_assets:
                break

            assets.extend(new_assets)

            if len(new_assets) < batch_size or offset + batch_size >= total_count:
                break
        
            offset += batch_size
        except Exception as e:
            raise

    asset_map = []
    for asset in assets:
        title = normalized_text = unicodedata.normalize('NFKD', asset["title"]).encode('ascii', 'ignore').decode('utf-8')
        asset_map.append({title: asset["_links"]["assets"]})

    return asset_map


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
    Get All assets queries under collections
    Added a filter condition on assets to only include `All Assets (API Integration)` collection
    as it has assets from all collections
"""

try:

    client_id = "961414658ff4034132e64fd9ef2cc75af32c0ee1.app.widen.com"
    client_secret = "10106ba3f2b66bef7f6de7dfbdf9b190cf7583d9"
    redirect_uri = "https://verdantas.acquiadam.com/auth/acquia/callback"

    authorization_base_url = "https://verdantas.widencollective.com/allowaccess"
    access_token_base_url = "https://api.widencollective.com/v2/oauth/access-token"


    # authorization_code = get_authorization_token(
    #     authorization_base_url, client_id, redirect_uri
    # )

    # access_token = get_access_token(
    #     access_token_base_url, client_id, client_secret, authorization_code
    # )

    access_token = "wat_verdantas_0c79e80b6d5e97fe79aba3c48f894eb0"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    collections_url = "https://api.widencollective.com/v2/collections?type=global&limit=100"
    
    asset_queries_map = get_assets_queries(collections_url, headers)

    asset_queries = {}
    for asset in asset_queries_map:
        key = list(asset.keys())[0]
        if key == 'All Assets (API Integration)':
            asset_queries[key] = asset[key]

    assets = []
    assets.append(asset_queries)

except Exception as e:
    raise Exception(e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(json.dumps(asset_queries_map))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
