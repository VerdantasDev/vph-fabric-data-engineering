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
import pandas as pd
import logging
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Getting app secret keys from key vault
client_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-teams-data-access-client-id')
tenant_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-teams-data-access-tenant-id')
client_secret_value = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-teams-data-access-client-secret')

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
    access_token = response.json().get('access_token')

    return access_token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_headers(access_token):

    headers = {
        'Authorization': 'Bearer ' + access_token
    }

    return headers

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

def get_username(headers, user_id):
    
    url = f'https://graph.microsoft.com/v1.0/users/{user_id}'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        
        user_data = response.json()
        
        return user_data.get('displayName', 'Unknown')
    
    return 'Unknown'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_chats_of_user(headers, user_id):
    
    url = f'https://graph.microsoft.com/v1.0/users/{user_id}/chats'
    all_chats = []

    while url:
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to retrieve chats: {response.status_code}")
            return pd.DataFrame()

        chats_data = response.json()
        all_chats.extend(chats_data.get('value', []))
        
        # Check if there's a next page
        url = chats_data.get('@odata.nextLink', None)

    if all_chats:
        chats_list = []
        
        for chat in all_chats:
            
            chat_dict = {}
            
            if chat.get('chatType', '') == 'oneOnOne':
                chatIDs = chat.get('id', '').split(':')[-1].split('@')[0].split('_')
                chatWithID = [c for c in chatIDs if c != user_id]
                if len(chatWithID)>0:
                    chat_dict['chat_name'] = get_username(headers, chatWithID[0])
                else:
                    chat_dict['chat_name'] = get_username(headers, user_id)
            else:
                chat_dict['chat_name'] = chat.get('topic', '')
            
            chat_dict['chat_type'] = chat.get('chatType', '')
            chat_dict['chat_id'] = chat.get('id', '')
            chat_dict['chat_created_datetime'] = chat.get('createdDateTime', '')
            chat_dict['chat_updated_datetime'] = chat.get('lastUpdatedDateTime', '')

            chats_list.append(chat_dict)
        
        chats_df = pd.DataFrame(chats_list)
        return chats_df
    else:
        return pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_messages_from_chat(headers, chat_id):
    
    url = f'https://graph.microsoft.com/v1.0/chats/{chat_id}/messages'
    all_messages = []

    while url:
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to retrieve messages: {response.status_code}")
            return pd.DataFrame()

        messages_data = response.json()
        all_messages.extend(messages_data.get('value', []))
        
        # Check if there's a next page
        url = messages_data.get('@odata.nextLink', None)

    if all_messages:
        ch_messages = []

        for message in all_messages:
            
            if message['from']:
            
                message_dict = {
                    'message_from': message['from']['user'].get('displayName', 'Unknown'),
                    'message_header': message.get('subject', ''),
                    'message_text': message['body'].get('content', ''),
                    'attachments': len(message.get('attachments', [])),
                    'created_datetime': message.get('createdDateTime', ''),
                    'message_id': message.get('id', ''),
                    'message_url': message.get('webUrl', '')
                }
                
                # Add messages with attachments
                if len(message.get('attachments', [])) > 0:
                    for a in message['attachments']:
                        attachment_message = message_dict.copy()
                        attachment_message['file_name'] = a.get('name', '')
                        attachment_message['file_url'] = a.get('contentUrl', '')
                        ch_messages.append(attachment_message)
                else:
                    # Add the base message dictionary
                    ch_messages.append(message_dict)
                    
            else:
                continue
                
        ch_messages_df = pd.DataFrame(ch_messages)
        return ch_messages_df
    else:
        return pd.DataFrame()

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

########################## get all users ##################################
    
all_users_df = get_users_in_tenant(headers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("teams_users") \
    .getOrCreate()
spark_df = spark.createDataFrame(all_users_df)
# spark_df.write.format("delta").mode("append").save('abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Tables/br_m365_sharepoint_file_status')
spark_df.write.mode("overwrite").format("delta").saveAsTable("/Tables/br_m365_teams_all_users")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

########################## get user by email ##############################

url = f'https://graph.microsoft.com/v1.0/users/Maitri.Dixit@VDTDevOps.com'

response = requests.get(url, headers=headers)

data = response.json()

user_id = data['id']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

########################## get chats of an user ###########################

all_chats_df = get_chats_of_user(headers, user_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("teams_chats") \
    .getOrCreate()
spark_df = spark.createDataFrame(all_chats_df)
# spark_df.write.format("delta").mode("append").save('abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Tables/br_m365_sharepoint_file_status')
spark_df.write.mode("overwrite").format("delta").saveAsTable("br_m365_teams_chats")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

########################## get messages of a chat #########################

chat_id = all_chats_df.loc[0,'chat_id']

all_messages_df = get_messages_from_chat(headers, chat_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_messages_df = all_messages_df.replace('\<.*?\>','', regex = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_messages_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("teams_messages") \
    .getOrCreate()
spark_df = spark.createDataFrame(all_messages_df)
# spark_df.write.format("delta").mode("append").save('abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Tables/br_m365_sharepoint_file_status')
spark_df.write.mode("overwrite").format("delta").saveAsTable("br_m365_teams_messages")

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
