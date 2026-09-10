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

import os
import requests
import pandas as pd
import logging
from delta.tables import DeltaTable
import re
import sys
sys.path.insert(0, '/lakehouse/default/Files/Configs')
from datetime import datetime
import base64

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

tenant_id = 'f9ca04e3-3d15-4386-8953-26320d3eefab'
client_id = '27cedaca-74b8-4e3c-9ee4-f7522222f849'
client_secret_value = 'nEe8Q~4un1sTQOrHtZauAI9i~DKEWFyJ83ELVbKO'

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_user_id(headers, email_id):

    url = f"https://graph.microsoft.com/v1.0/users?$filter=mail eq '{email_id}'"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        users = response.json()
        if users['value']:
            user_id = users['value'][0]['id']
            return user_id
        else:
            return None  # Email not found
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_mail_folder_id(headers, user_id, folder_name):

    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        folders = response.json()
        for folder in folders.get('value', []):
            if folder['displayName'] == folder_name:
                return folder['id']
        return None  # Folder not found
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_directory_path():
    today = datetime.today()
    year_str = today.year
    month_str = today.strftime("%m")
    day_str = today.strftime("%d")
    directory_path = f'/lakehouse/default/Files/bronze/microsoft365/outlook/{year_str}/{month_str}/{day_str}'
    os.makedirs(directory_path, exist_ok=True)
    return directory_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_messages(url, headers):
    response = requests.get(url, headers=headers)
    return response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_attachments(url, headers):
    response = requests.get(url, headers=headers)
    return response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_attachment(content_bytes, save_path):
    with open(save_path, 'wb') as file:
        file.write(base64.b64decode(content_bytes))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_mail(mail, headers, directory_path):
    email_id = mail['email']
    folder_name = mail.get('folder_name')
    extraction_category = mail.get('category')
    
    user_id = get_user_id(headers, email_id)
    
    if not user_id:
        logging.error(f"User email {email_id} not found in tenant")
        return None

    folder_id = get_mail_folder_id(headers, user_id, folder_name) if folder_name else None
    base_url = f"https://graph.microsoft.com/v1.0/users/{user_id}"
    if folder_name:
        base_url += f"/mailFolders/{folder_id}"
    message_url = base_url + "/messages"
    if extraction_category:
        message_url += f"?$filter=categories/any(c:c eq '{extraction_category}')"
    
    data = fetch_messages(message_url, headers)
    all_mails = []
    
    if data['value']:
        for single_mail in data['value']:
            attachment_path = []
            if single_mail.get('hasAttachments'):
                mail_id = single_mail['id']
                att_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{mail_id}/attachments"
                attachments = fetch_attachments(att_url, headers)
                
                for att in attachments['value']:
                    save_file_name = att['name']
                    full_path = os.path.join(directory_path, save_file_name)
                    save_attachment(att['contentBytes'], full_path)
                    attachment_path.append(full_path)
            else:
                attachment_path = None
            
            mail_details = single_mail.copy()
            mail_details['attachment_path'] = attachment_path
            mail_details['folder_name'] = folder_name
            mail_details['user_email'] = email_id
            all_mails.append(mail_details)
    else:
        logging.error(f"No mails found for user {email_id}")
    
    return all_mails

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folder_id = get_mail_folder_id(headers, user_id, folder_name)
base_url = f"https://graph.microsoft.com/v1.0/users/{user_id}"
if folder_name:
    base_url += f"/mailFolders/{folder_id}"
message_url = base_url + "/messages"
if extraction_category:
    message_url += f"?$filter=categories/any(c:c eq '{extraction_category}')"

data = fetch_messages(message_url, headers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logging.info("Retrieving Access Token")
try:
    access_token = get_access_token(tenant_id, client_id, client_secret_value)
    logging.info("Access Token Retrieved Successfully")
except:
    logging.error("Failed to retrieve access token")

headers = get_headers(access_token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

directory_path = get_directory_path()

emails_to_extract = [
    {'email': "maitri.dixit@vdtdevops.com",
        'folder_name': "Extract",
        'category': 'Purple category'}
]

all_mails = []
for mail in emails_to_extract:
    mail_data = process_mail(mail, headers, directory_path)
    if mail_data:
        all_mails.extend(mail_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_mails_df = pd.DataFrame(all_mails)
all_mails_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_mails_df_final = all_mails_df[['user_email','folder_name','id',
       'categories', 'receivedDateTime', 'sentDateTime',
       'hasAttachments', 'subject', 'bodyPreview',
       'importance', 'parentFolderId', 'sender',
       'from', 'toRecipients',
       'flag', 'attachment_path']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract address from 'from' column
all_mails_df_final['from'] = all_mails_df_final['from'].apply(lambda x: x['emailAddress']['address'] if pd.notnull(x) and 'emailAddress' in x else None)

# Extract addresses from 'toRecipients' column
all_mails_df_final['toRecipients'] = all_mails_df_final['toRecipients'].apply(lambda x: [recipient['emailAddress']['address'] for recipient in x] if pd.notnull(x) else [])
all_mails_df_final['sender'] = all_mails_df_final['sender'].apply(lambda x: x['emailAddress']['address'] if pd.notnull(x) and 'emailAddress' in x else None)

# Extract flag value from 'flag' column
all_mails_df_final['flag'] = all_mails_df_final['flag'].apply(lambda x: x['flagStatus'] if pd.notnull(x) and 'flagStatus' in x else None)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_mails_df_final

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

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("Outlook") \
    .getOrCreate()
spark_df = spark.createDataFrame(all_mails_df_final)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_m365_outlook_sample"  # Example Delta Lake table path in Azure Data Lake Storage

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
