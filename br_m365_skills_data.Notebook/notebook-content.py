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
import requests
import pandas as pd
import logging
from delta.tables import DeltaTable
from tqdm import tqdm
import re
import sys

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

def get_users_in_tenant(headers):
    
    url = 'https://graph.microsoft.com/v1.0/users'
    all_users = []

    while url:
        
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to retrieve users: {response.status_code}")
            return pd.DataFrame()

        users_data = response.json()
        all_users.extend(users_data.get('value', []))
        
        # Check if there's a next page
        url = users_data.get('@odata.nextLink', None)

    if all_users:
        users_df = pd.DataFrame(all_users)
        return users_df
    else:
        return pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_skills_of_user(headers, user_id):

    url = f'https://graph.microsoft.com/beta/users/{user_id}/profile/skills'
    response = requests.get(url, headers=headers)

    # Check if the response is successful
    if response.status_code == 200:
        data = response.json()
        
        # keys_to_select = ['displayName', 'proficiency', 'collaborationTags']
        keys_to_select = ['displayName','proficiency']
        
        # Filter each skill dictionary in the list
        filtered_skills = [
            {key: skill[key] for key in keys_to_select if key in skill} 
            for skill in data['value']
        ]
        return filtered_skills
    else:
        # Handle error response
        raise Exception(f"Error: {response.status_code}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_skills_for_users(headers, users_dict):
    users_skills_data = []
    failed_users = []
    for user in tqdm(users_dict, desc="Fetching user data and skills"):
        # Select specific keys from the user data
        keys_to_select = ['displayName', 'givenName', 'mail', 'surname', 'id']
        user_dict = {key: user[key] for key in keys_to_select if key in user}
        
        # Get the user's ID to fetch their skills
        user_id = user['id']
        try:
            skills_dict = get_skills_of_user(headers, user_id)
        except:
            failed_users.append(user)
        
        # Add skills data to the user dictionary
        user_dict['skills'] = skills_dict
        users_skills_data.append(user_dict)

    # Convert the list of dictionaries to a DataFrame
    user_skill_df = pd.DataFrame(users_skills_data)

    return user_skill_df, failed_users

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

if selected_users == 'ALL':
    users_df = get_users_in_tenant(headers)
    users_dict = users_df.to_dict(orient = 'records')
    user_skill_df, failed_users = get_skills_for_users(headers, users_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df = pd.merge(user_skill_df[['skills','id']], users_df, on='id', how='inner')
joined_df = joined_df[['displayName','givenName','surname', 'skills','jobTitle','mail','id', 'businessPhones', 
       'mobilePhone', 'officeLocation',
       'preferredLanguage', 'userPrincipalName']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(joined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("Delve") \
    .getOrCreate()
spark_df = spark.createDataFrame(joined_df)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_m365_profile_skills_data"  # Example Delta Lake table path in Azure Data Lake Storage

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

display(spark_df.select("skills").distinct())

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
