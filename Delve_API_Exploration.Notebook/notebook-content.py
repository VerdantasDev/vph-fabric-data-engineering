# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

access_token = get_access_token(tenant_id, client_id, client_secret_value)

headers = get_headers(access_token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

user_df = get_users_in_tenant(headers)
display(user_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f'https://graph.microsoft.com/v1.0/users/d71205de-88b7-48d7-86b1-183dd733cf3e/?$select=skills'

response = requests.get(url, headers=headers)

data = response.json()

data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f'https://graph.microsoft.com/beta/users/d71205de-88b7-48d7-86b1-183dd733cf3e/profile/skills'
response = requests.get(url, headers=headers)
data1 = response.json()
data1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f'https://graph.microsoft.com/beta/users/19eec6b8-0d88-4af4-b642-203bcfb22562/profile/skills'

response = requests.get(url, headers=headers)

data1 = response.json()

data1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f'https://graph.microsoft.com/beta/users/8d87e803-7685-4333-ac84-177508b28f38/profile/skills'

response = requests.get(url, headers=headers)

data1 = response.json()

data1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

# Replace with your actual user ID and access token
user_id = '8d87e803-7685-4333-ac84-177508b28f38'
url = f'https://graph.microsoft.com/beta/users/{user_id}/profile'


# Set the 'skills' property to an empty list to delete it
json_data = {
    'skills': []
}

# Make the PATCH request to update the user's profile
response = requests.patch(url, headers=headers, json=json_data)

# Check the status code
print(response.status_code)

# If needed, print the response JSON to check for any errors
print(response.json())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

url = 'https://graph.microsoft.com/beta/users/19eec6b8-0d88-4af4-b642-203bcfb22562/profile/skills'

json_data = {'categories': ['Professional'],
   'displayName': 'Python',
   'proficiency': 'expert',
   'collaborationTags': ['canMentor'],
   'allowedAudiences': 'organization'
   }

response = requests.post(url, headers=headers, json=json_data)

response.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Delegated access token**

# CELL ********************

# delegated_access_token = 'eyJ0eXAiOiJKV1QiLCJub25jZSI6IlFtWnUzeUFqanB2dWlvVk5HSnNGazdkSV9QeHloSW5VbndaQVNuNGtlZTgiLCJhbGciOiJSUzI1NiIsIng1dCI6IktRMnRBY3JFN2xCYVZWR0JtYzVGb2JnZEpvNCIsImtpZCI6IktRMnRBY3JFN2xCYVZWR0JtYzVGb2JnZEpvNCJ9.eyJhdWQiOiIwMDAwMDAwMy0wMDAwLTAwMDAtYzAwMC0wMDAwMDAwMDAwMDAiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC9mOWNhMDRlMy0zZDE1LTQzODYtODk1My0yNjMyMGQzZWVmYWIvIiwiaWF0IjoxNzI0Nzg0NTk0LCJuYmYiOjE3MjQ3ODQ1OTQsImV4cCI6MTcyNDg3MTI5NCwiYWNjdCI6MCwiYWNyIjoiMSIsImFpbyI6IkFWUUFxLzhYQUFBQWUxdTF6UE9ISkM1Mm5IM2M3MXB5R3dNYWwvN3pzK2FabnNtekhhb01sZTMwbU8vR3RJaGIxUVhtQjVwdXpVV2xzTUIwbDQ4akVDT0p0cHVTMHcyM3RJdlB6Q0JRdGpoNktvZ1JKN3R4WEhRPSIsImFtciI6WyJwd2QiLCJtZmEiXSwiYXBwX2Rpc3BsYXluYW1lIjoiR3JhcGggRXhwbG9yZXIiLCJhcHBpZCI6ImRlOGJjOGI1LWQ5ZjktNDhiMS1hOGFkLWI3NDhkYTcyNTA2NCIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiRGl4aXQiLCJnaXZlbl9uYW1lIjoiTWFpdHJpIiwiaWR0eXAiOiJ1c2VyIiwiaXBhZGRyIjoiNDUuMjUyLjc1LjIwOCIsIm5hbWUiOiJNYWl0cmkgRGl4aXQiLCJvaWQiOiI4ZDg3ZTgwMy03Njg1LTQzMzMtYWM4NC0xNzc1MDhiMjhmMzgiLCJwbGF0ZiI6IjMiLCJwdWlkIjoiMTAwMzIwMDM5QjdENTU2OSIsInJoIjoiMC5BYmNBNHdUSy1SVTloa09KVXlZeURUN3Zxd01BQUFBQUFBQUF3QUFBQUFBQUFBRDhBQ3cuIiwic2NwIjoib3BlbmlkIHByb2ZpbGUgVXNlci5SZWFkIGVtYWlsIFVzZXIuUmVhZFdyaXRlIiwic3ViIjoiVGVMZ1RXd1JKYW8wZE8xWlpMeWNpcEhhRi16UkxmWE5UVU9IdDd1eHkzZyIsInRlbmFudF9yZWdpb25fc2NvcGUiOiJOQSIsInRpZCI6ImY5Y2EwNGUzLTNkMTUtNDM4Ni04OTUzLTI2MzIwZDNlZWZhYiIsInVuaXF1ZV9uYW1lIjoiTWFpdHJpLkRpeGl0QFZEVERldk9wcy5jb20iLCJ1cG4iOiJNYWl0cmkuRGl4aXRAVkRURGV2T3BzLmNvbSIsInV0aSI6Il96anc0aXQxMTBlMFAtdkcwUEl1QUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfY2MiOlsiQ1AxIl0sInhtc19pZHJlbCI6IjEgMTQiLCJ4bXNfc3NtIjoiMSIsInhtc19zdCI6eyJzdWIiOiI3WDBwWld6NEFrcU5ETG1tdWtXdTFWbWtVYXVOVC1mcmJLTWlzYWRjSEl3In0sInhtc190Y2R0IjoxNzE5NDI3Njc1fQ.D-AuaAXYUYa_XkO4RoQcxE1CnxlZicdss-nOwuLIfDZrUBUx68iSDIwxmZAACXPEuAuFoEn_zrb9hFOFm1ax2TL11hXKxGaVISqSbubvjTuOEmkLUR_oL4SU9qMGOez5B5lL9HJH4ERjAT4oRMusebO9B5bwYqIJ20ubYHsGnHmeDWsrdqEqbOSqzCG4ylEhzISUuDELTisH73vupLTn5__Ch7QFsH7V4xFHsFK2YxyWEHZSlRW8CBy8DuOzkypC_AZkYsKY-yF_V9VpN3xO40-ViTW8qI2p1N2XCTTF5QuBQnZe0ymQLwK7aqwoTkfYhJE0QkKjqppDN3sdg2qkdA'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# headers2 = {
#     'Authorization': f'Bearer {delegated_access_token}',
#     'Content-Type': 'application/json'
# }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# url = 'https://graph.microsoft.com/beta/me/profile/skills'
# response = requests.get(url, headers=headers2)
# data2 = response.json()
# data2

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
