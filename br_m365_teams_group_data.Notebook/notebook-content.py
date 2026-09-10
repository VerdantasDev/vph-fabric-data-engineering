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
import re
import sys
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
client_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-teams-data-access-client-id')
tenant_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-teams-data-access-tenant-id')
client_secret_value = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','vpc-teams-data-access-client-secret')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
import os
from datetime import datetime

# Step 1: Define the log directory and filename
date_str = datetime.now().strftime("%Y-%m-%d")
log_dir = f'/lakehouse/default/Files/logs/{datetime.now().strftime("%Y/%m/%d")}/m365/teams'
os.makedirs(log_dir, exist_ok=True)

# Step 2: Set up the log file path
log_file_name = f'br_m365_teams_group_data_{date_str}.log'
log_file_path = os.path.join(log_dir, log_file_name)

# Step 3: Configure logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Custom error tracking handler
class ErrorTrackingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.error_occurred = False

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            self.error_occurred = True

error_handler = ErrorTrackingHandler()
logging.getLogger().addHandler(error_handler)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_teams_in_tenant(headers):
    url = 'https://graph.microsoft.com/v1.0/groups'
    all_teams_data = []

    while url:
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
        teams_data = response.json()

        for team in teams_data.get('value', []):
            single_team_dict = {
                'team_name': team.get('displayName'),
                'team_id': team.get('id')
            }
            all_teams_data.append(single_team_dict)

        # Get the next page URL, if present
        url = teams_data.get('@odata.nextLink')

    all_teams_df = pd.DataFrame(all_teams_data)
    return all_teams_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_team_members(headers, team_id):
    try:
        url = f'https://graph.microsoft.com/v1.0/teams/{team_id}/members'

        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad responses (4xx or 5xx)

        team_members_data = response.json()

        if 'value' in team_members_data:
            team_members_df = pd.DataFrame(team_members_data['value'])
            team_members_df = team_members_df[[
                'displayName', 'userId', 'email', 'roles', 'id']]
            return team_members_df
        else:
            print("No 'value' key found in response data.")
            return pd.DataFrame()  # Return an empty DataFrame if no members found

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return pd.DataFrame()  # Return empty DataFrame on request failure
    except KeyError as e:
        logging.error(f"KeyError: {e}. Check the structure of the response.")
        return pd.DataFrame()  # Return empty DataFrame on unexpected response structure

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_channels_in_team(headers, team_id):

    url = f'https://graph.microsoft.com/v1.0/teams/{team_id}/channels'
    all_channels_data = []

    try:
        # Make the GET request to retrieve channels
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        channels_data = response.json()

        # Extract channel details
        for channel in channels_data.get('value', []):
            single_channel_dict = {
                'channel_name': channel.get('displayName', ''),
                'channel_id': channel.get('id', '')
            }
            all_channels_data.append(single_channel_dict)

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to retrieve channels for team {team_id}: {e}")

    # Create DataFrame from the collected channel data
    channels_df = pd.DataFrame(all_channels_data)

    return channels_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_team_channel_details(headers, teams_extraction_dict):
    
    # Initialize an empty DataFrame to hold combined results
    tc_combined_df = pd.DataFrame()

    # Retrieve all teams in the tenant
    all_teams_df = get_teams_in_tenant(headers)

    for team_details in teams_extraction_dict:
        team_name = team_details.get('team_name')
        channel_name = team_details.get('channel_name')

        # Filter teams DataFrame to find the relevant team
        filtered_team = all_teams_df[all_teams_df['team_name'] == team_name]

        if not filtered_team.empty:
            team_id = filtered_team.iloc[0]['team_id']
            channels_df = get_channels_in_team(headers, team_id)
            
            # Filter channels DataFrame to find the relevant channel
            req_channels_df = channels_df[channels_df['channel_name'] == channel_name]
            req_channels_df['team_name'] = team_name
            req_channels_df['team_id'] = team_id
            
            # Select and reorder columns
            req_channels_df = req_channels_df[['team_name', 'channel_name', 'team_id', 'channel_id']]
            
            # Concatenate the result to the combined DataFrame
            tc_combined_df = pd.concat([tc_combined_df, req_channels_df], ignore_index=True)

    return tc_combined_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_teams_drive_data(headers, team_id, channel_name):

    url = f'https://graph.microsoft.com/v1.0/groups/{team_id}/drive/root/children'

    try:
        # Make the API request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
        teams_drive_data = response.json()

        # Ensure the JSON response has the 'value' key
        child_items = teams_drive_data.get('value', [])

        # Filter and collect relevant drive items
        team_drive_items = []
        for item in child_items:
            # Check if the item's name matches the channel name
            if item.get('name') == channel_name:
                # Safely get drive and site IDs from parentReference
                team_drive_items.append({
                    'folder_id': item.get('id'),
                    'drive_id': item.get('parentReference', {}).get('driveId'),
                    'site_id': item.get('parentReference', {}).get('siteId')
                })

        return team_drive_items

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching drive data for team {team_id}: {e}")
        return []


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
            logging.error(f"Request failed: {e}")
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
        logging.error(f"Failed to retrieve fields data for file {file_id}: {e}")
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
            logging.error(f"Request failed: {e}")
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

def get_channel_messages(headers, team_id, channel_id):

    url = f'https://graph.microsoft.com/beta/teams/{team_id}/channels/{channel_id}/messages'

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes

        channel_messages_data = response.json()
        ch_messages = []

        # Process the messages if the 'value' key exists in the response
        if 'value' in channel_messages_data:
            for post in channel_messages_data['value']:
                base_message = {
                    'message_header': post.get('subject', ''),
                    'message_text': post.get('body', {}).get('content', ''),
                    'attachments': len(post.get('attachments', [])),
                    'created_datetime': post.get('createdDateTime', ''),
                    'message_id': post.get('id', ''),
                    'message_url': post.get('webUrl', '')
                }

                # Append message with attachments
                if 'attachments' in post and post['attachments']:
                    for attachment in post['attachments']:
                        attachment_message = base_message.copy()
                        attachment_message['file_name'] = attachment.get('name', '')
                        attachment_message['file_url'] = attachment.get('contentUrl', '')
                        ch_messages.append(attachment_message)
                else:
                    ch_messages.append(base_message)

        return pd.DataFrame(ch_messages)

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching messages for channel {channel_id} in team {team_id}: {e}")
        return pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_list_id(headers, site_id):

    url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists'

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes

        list_data = response.json()
        for lst in list_data.get('value', []):
            if lst.get('displayName') == 'Documents' or lst.get('name') == 'Documents':
                return lst.get('id')

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching lists for site {site_id}: {e}")

    return None

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

teams_extraction_dict
logging.info(f'Teams and channels to be extracted are {teams_extraction_dict}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

########################## get teams and channel details ##################

tc_df = get_team_channel_details(headers, teams_extraction_dict)
logging.info(f'Team and Channel Ids obtained successfully')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

########################## get files in teams drive #######################

tc_dicts = tc_df.to_dict('records')
all_files_df = pd.DataFrame()

for tc in tc_dicts:
    try:
        logging.info(f"Starting files retrieval for team '{tc['team_name']}' and channel '{tc['channel_name']}'.")

        # Retrieve team drive items
        team_drive_items = get_teams_drive_data(headers, tc['team_id'], tc['channel_name'])
        
        if not team_drive_items:
            logging.warning(f"No drive items found for team {tc['team_name']} and channel {tc['channel_name']}")
            continue
        
        # Update tc dict with team drive details
        tc.update(team_drive_items[0])
        
        site_id = tc['site_id']
        list_id = get_list_id(headers, site_id)

        if not list_id:
            logging.warning(f"No list ID found for site {site_id} and channel {tc['channel_name']}")
            continue
        
        # Get all files from folder
        files_df = get_all_files_from_folder(headers, tc['drive_id'], tc['folder_id'], site_id, list_id)
        
        # Add metadata to files DataFrame
        files_df['team_name'] = tc['team_name']
        files_df['team_id'] = tc['team_id']
        files_df['channel_name'] = tc['channel_name']
        files_df['channel_id'] = tc['channel_id']
        
        # Append to the main DataFrame
        all_files_df = pd.concat([all_files_df, files_df], ignore_index=True)

        logging.info(f"Successfully retrieved and processed files for team '{tc['team_name']}' and channel '{tc['channel_name']}'.")
    
    except Exception as e:
        logging.error(f"An error occurred while processing team {tc['team_name']} and channel {tc['channel_name']}: {e}")

# Apply transformation to 'custom_tags' column
if 'custom_tags' in all_files_df.columns:
    logging.info("Applying transformation to 'custom_tags' column.")
    all_files_df['custom_tags'] = all_files_df['custom_tags'].apply(lambda x: {convert_tag(key): value for key, value in x.items()})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_files_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("teams_files") \
    .getOrCreate()
spark_df = spark.createDataFrame(all_files_df)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_m365_teams_shared_files"  # Example Delta Lake table path in Azure Data Lake Storage

try:
    # Write data to Delta Lake table
    spark_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(delta_table_path)
    logging.info("Shared files data written to lakehouse successfully.")

except Exception as e:

    logging.error(f"Failed to write files data to lakehouse: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_df = spark.read.format("delta").load("Tables/br_m365_teams_shared_files")

display(source_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

########################## get channel messages ###########################

all_channel_messages = pd.DataFrame()

all_channel_messages = pd.DataFrame()

for i in tc_df.index:
    team_id = tc_df.loc[i, 'team_id']
    channel_id = tc_df.loc[i, 'channel_id']
    
    try:
        logging.info(f"Starting message retrieval for team '{tc_df.loc[i, 'team_name']}' and channel '{tc_df.loc[i, 'channel_name']}'.")

        # Get channel messages
        ch_messages_df = get_channel_messages(headers, team_id, channel_id)
        
        # Check if ch_messages_df is not empty
        if not ch_messages_df.empty:
            # Add team and channel metadata
            ch_messages_df['team_name'] = tc_df.loc[i, 'team_name']
            ch_messages_df['channel_name'] = tc_df.loc[i, 'channel_name']
            ch_messages_df['team_id'] = team_id
            ch_messages_df['channel_id'] = channel_id

            # Concatenate with the main DataFrame
            all_channel_messages = pd.concat(
                [all_channel_messages, ch_messages_df], ignore_index=True
            )

            logging.info(f"Successfully retrieved messages for team '{tc_df.loc[i, 'team_name']}' and channel '{tc_df.loc[i, 'channel_name']}'.")
    
    except Exception as e:
        logging.error(f"An error occurred while retrieving messages for team '{team_id}' and channel '{channel_id}': {e}")

all_channel_messages.reset_index(drop=True, inplace=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_channel_messages

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
spark_df = spark.createDataFrame(all_channel_messages)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_m365_teams_channel_messages"  # Example Delta Lake table path in Azure Data Lake Storage

try:

    logging.info(f"Writing Spark DataFrame to Delta Lake table at '{delta_table_path}'...")
    spark_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(delta_table_path)
    logging.info(f"Data written successfully to Delta Lake table at '{delta_table_path}'.")

except Exception as e:
    logging.error(f"An error occurred during the Spark DataFrame processing or writing: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_df = spark.read.format("delta").load("Tables/br_m365_teams_channel_messages")

display(source_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if error_handler.error_occurred:
    logging.error("The run failed.")
else:
    logging.info("The run was successful.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
