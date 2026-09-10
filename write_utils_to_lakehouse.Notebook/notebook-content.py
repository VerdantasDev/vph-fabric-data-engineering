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

utils_code = '''
import re
import requests

def convert_tag(tag):
    # Replace _xXXXX with \\uXXXX
    unicode_str = re.sub(r'_x([0-9A-F]{4})_', r'\\\\u\\1', tag)
    # Decode unicode escape sequences
    return unicode_str.encode('ascii').decode('unicode_escape')


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

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        # Raise an error or return None
        response.raise_for_status()  # This will raise an HTTPError for bad responses
        

def get_headers(access_token):

    headers = {
        'Authorization': 'Bearer ' + access_token
    }

    return headers

'''


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

utils_file_path = "Files/Configs/m365_utils.py"

# Set the last parameter as True to overwrite the file if it existed already
notebookutils.fs.put(utils_file_path, utils_code, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# content = notebookutils.fs.head(utils_file_path)
# print(content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
