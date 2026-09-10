# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import base64
import requests
import os
import pandas as pd
from tqdm import tqdm


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_secret_value(secret_value):
    # Strip any extraneous whitespace or newline characters
    secret_value = secret_value.strip()

    # Ensure the secret is properly formatted with newlines
    lines = secret_value.splitlines()
    clean_lines = [line.strip() for line in lines if line.strip() != '']

    # Rejoin lines with a newline character
    cleaned_secret_value = "\n".join(clean_lines)

    return cleaned_secret_value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

client_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','adp-client-id')
client_secret = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','adp-client-secret')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_api_cert = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','adp-cert-v2')
adp_api_key = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','adp-key-v2')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# using time module
import time
 
# ts stores the time in seconds
timestamp = time.time()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cert_path = f'api_cert_{timestamp}.pem'
key_path = f'api_key_{timestamp}.key'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_cert_value = clean_secret_value(adp_api_cert)
cleaned_key_value = clean_secret_value(adp_api_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not cleaned_cert_value.startswith("-----BEGIN CERTIFICATE-----"):
        cleaned_cert_value = f"-----BEGIN CERTIFICATE-----\n{cleaned_cert_value}\n-----END CERTIFICATE-----"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not cleaned_key_value.startswith("-----BEGIN PRIVATE KEY-----"):
        cleaned_key_value = f"-----BEGIN PRIVATE KEY-----\n{cleaned_key_value}\n-----END PRIVATE KEY-----"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pem_cert_value_bytes = cleaned_cert_value.encode('utf-8')
pem_key_value_bytes = cleaned_key_value.encode('utf-8')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the PEM value to a file
with open(cert_path, 'wb') as pem_file:
    pem_file.write(pem_cert_value_bytes)
    # Write the PEM value to a file
with open(key_path, 'wb') as pem_file:
    pem_file.write(pem_key_value_bytes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

token_url = "https://accounts.adp.com/auth/oauth/v2/token?grant_type=client_credentials"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get the OAuth 2.0 token using client certificate and key
def get_access_token(client_id, client_secret, cert_path,key_path):
    credentials = f"{client_id}:{client_secret}"
    encoded_auth = base64.b64encode(credentials.encode()).decode()
    auth_headers = {"Authorization": f"Basic {encoded_auth}"}
    response = requests.post(token_url, headers=auth_headers, cert=(cert_path,key_path))
    response.raise_for_status()
    return response.json()['access_token']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to make a GET request with automatic token regeneration
def make_get_request(url, headers, params=None):
    response = requests.get(url, headers=headers, params=params, cert=(cert_path, key_path))
    if response.status_code == 401:  # Unauthorized, likely due to expired token
        # Regenerate the token
        new_token = get_access_token(client_id, client_secret, cert_path,key_path)
        headers['Authorization'] = f'Bearer {new_token}'
        response = requests.get(url, headers=headers, params=params, cert=(cert_path, key_path))
    response.raise_for_status()
    return response


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get worker data from ADP API with pagination
def get_all_worker_data(access_token):
    api_url = 'https://api.adp.com/hr/v2/workers'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    all_data = []
    skip = 0
    page_size = 50  # Adjust the page size as per your API's pagination limit

    select_fields = (
        'workers/associateOID,workers/person/legalName/formattedName,'
        'workers/workerID/idValue,workers/workerStatus/statusCode/codeValue,'
        'workers/workAssignments/actualStartDate,workers/workAssignments/managementPositionIndicator,'
        'workers/workAssignments/positionTitle,workers/workAssignments/jobCode'
    )

    while True:
        # Construct parameters with $skip and $select
        params = {
            '$skip': skip,
            '$select': select_fields
        }

        # Make a GET request to the API
        response = make_get_request(api_url, headers=headers, params=params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data.get('workers', []))  # Append current page data to all_data list

            # Check if there are more pages to fetch
            if len(data.get('workers', [])) < page_size:
                break  # Break the loop if the fetched data is less than page_size
            else:
                skip += page_size  # Increment $skip for the next page
        elif response.status_code == 204:
            break
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            print(response.text)  # Print the error message if available
            break

    return all_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get certifications for a specific associate
def get_certifications(access_token, associate_oid):
    api_url = f'https://api.adp.com/talent/v2/associates/{associate_oid}/associate-certifications'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    response = make_get_request(api_url, headers=headers)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 204:
        return None  # No content
    else:
        print(f"Failed to retrieve certifications for {associate_oid}: {response.status_code}")
        return None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get pay distributions for a specific associate with select fields
def get_pay_distributions(access_token, associate_oid):
    api_url = f'https://api.adp.com/payroll/v2/workers/{associate_oid}/pay-distributions'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    params = {
        '$select': 'payDistributions/payrollFileNumber'
    }
    response = make_get_request(api_url, headers=headers, params=params)
 
    if response.status_code == 200:
        dic = dict()
        dic = {
          'payrollFileNumber' : response.json().get('payDistributions', {})[0].get('payrollFileNumber'),
          'associateOID': associate_oid
         }

        return dic
    elif response.status_code == 204:
        return None  # No content
    else:
        print(f"Failed to retrieve pay distributions for {associate_oid}: {response.status_code}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if __name__ == "__main__":
    try:
        # Step 1: Get the access token
        token = get_access_token(client_id, client_secret, cert_path,key_path)
        print(token)
        # Step 2: Retrieve all worker data
        workers_data = get_all_worker_data(token)
        print(f"Total workers retrieved: {len(workers_data)}")

        # # Step 3: Convert worker data to pandas DataFrame
        # workers_df = pd.json_normalize(workers_data)

        # # Step 4: Extract associateOID for each worker
        # associate_oids = [worker.get('associateOID') for worker in workers_data]
        # associate_oids = ['G3BQJWEFTHC2MH1W']
        # # Step 5: Retrieve certifications and pay distributions for each worker
        # certifications_list = []
        # pay_distributions_list = []
        # for oid in tqdm(associate_oids, desc="Fetching certifications and pay distributions"):
        #     certifications = get_certifications(token, oid)
        #     if certifications:
        #         certifications_list.append(certifications)

        #     pay_distributions = get_pay_distributions(token, oid)
        #     if pay_distributions:
        #         pay_distributions_list.append(pay_distributions)

        # # Step 6: Convert certifications and pay distributions data to pandas DataFrames
        # certifications_df = pd.DataFrame(certifications_list)
        # pay_distributions_df = pd.DataFrame(pay_distributions_list)
        # path = 'C:/Users/sturlapaty/VPCADPExtract/'
        # workers_df.to_csv(path+ 'workers_data.csv',index=False)
        # certifications_df.to_csv(path + 'certification_data.csv',index=False)
        # pay_distributions_df.to_csv(path + 'payrollFileNumber_data.csv',index=False)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"Other error occurred: {err}")
    finally:

        if os.path.exists(cert_path):
            os.remove(cert_path)
            #print(f"{cert_path} has been deleted.")
        else:
            print(f"{cert_path} does not exist.")
        
        if os.path.exists(key_path):
            os.remove(key_path)
            #print(f"{key_path} has been deleted.")
        else:
            print(f"{key_path} does not exist.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if os.path.exists(key_path):
    os.remove(key_path)
    print(f"{key_path} has been deleted.")
else:
    print(f"{key_path} does not exist.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
import base64
from tqdm import tqdm

# Replace these with your actual ADP API credentials and paths to your certificate and key files
token_url = "https://accounts.adp.com/auth/oauth/v2/token?grant_type=client_credentials"


# Function to get the OAuth 2.0 token using client certificate and key
def get_access_token(client_id, client_secret, cert_path, key_path):
    credentials = f"{client_id}:{client_secret}"
    encoded_auth = base64.b64encode(credentials.encode()).decode()
    auth_headers = {"Authorization": f"Basic {encoded_auth}"}
    response = requests.post(token_url, headers=auth_headers, cert=(cert_path, key_path))
    response.raise_for_status()
    return response.json()['access_token']


# Function to make a GET request with automatic token regeneration
def make_get_request(url, headers, params=None):
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 401:  # Unauthorized, likely due to expired token
        # Regenerate the token
        new_token = get_access_token(client_id, client_secret, cert_path, key_path)
        # new_token = ''
        headers['Authorization'] = f'Bearer {new_token}'
        response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response


# Function to get worker data from ADP API with pagination
def get_all_worker_data(access_token):
    api_url = 'https://api.adp.com/hr/v2/workers'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    all_data = []
    skip = 0
    page_size = 50  # Adjust the page size as per your API's pagination limit

    select_fields = (
        'workers/associateOID,workers/person/legalName/formattedName,'
        'workers/workerID/idValue,workers/workerStatus/statusCode/codeValue,'
        'workers/workAssignments/actualStartDate,workers/workAssignments/managementPositionIndicator,'
        'workers/workAssignments/positionTitle,workers/workAssignments/jobCode'
    )

    while True:
        # Construct parameters with $skip and $select
        params = {
            '$skip': skip,
            '$select': select_fields
        }

        # Make a GET request to the API
        response = make_get_request(api_url, headers=headers, params=params)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data.get('workers', []))  # Append current page data to all_data list

            # Check if there are more pages to fetch
            if len(data.get('workers', [])) < page_size:
                break  # Break the loop if the fetched data is less than page_size
            else:
                skip += page_size  # Increment $skip for the next page
        elif response.status_code == 204:
            break
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            print(response.text)  # Print the error message if available
            break

    return all_data


# Function to get certifications for a specific associate
def get_certifications(access_token, associate_oid):
    api_url = f'https://api.adp.com/talent/v2/associates/{associate_oid}/associate-certifications'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    response = make_get_request(api_url, headers=headers)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 204:
        return None  # No content
    else:
        print(f"Failed to retrieve certifications for {associate_oid}: {response.status_code}")
        return None


# Function to get pay distributions for a specific associate with select fields
def get_pay_distributions(access_token, associate_oid):
    api_url = f'https://api.adp.com/payroll/v2/workers/{associate_oid}/pay-distributions'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    params = {
        '$select': 'payDistributions/payrollFileNumber'
    }
    response = make_get_request(api_url, headers=headers, params=params)
    if response.status_code == 200:
        dic = dict()
        dic = {
            'payrollFileNumber': response.json().get('payDistributions', {})[0].get('payrollFileNumber'),
            'associateOID': associate_oid
        }

        return dic
    elif response.status_code == 204:
        return None  # No content
    else:
        print(f"Failed to retrieve pay distributions for {associate_oid}: {response.status_code}")
        return None


if __name__ == "__main__":
    try:
        # Step 1: Get the access token
        token = get_access_token(client_id, client_secret, cert_path, key_path)
        print(token)

        # Step 2: Retrieve all worker data
        workers_data = get_all_worker_data(token)
        print(f"Total workers retrieved: {len(workers_data)}")

        # Step 3: Convert worker data to pandas DataFrame
        workers_df = pd.json_normalize(workers_data)

        # Step 4: Extract associateOID for each worker
        associate_oids = [worker.get('associateOID') for worker in workers_data]

        # Step 5: Retrieve certifications and pay distributions for each worker
        certifications_list = []
        pay_distributions_list = []
        for oid in tqdm(associate_oids, desc="Fetching certifications and pay distributions"):
            certifications = get_certifications(token, oid)
            if certifications:
                certifications_list.append(certifications)

            pay_distributions = get_pay_distributions(token, oid)
            if pay_distributions:
                pay_distributions_list.append(pay_distributions)

        # Step 6: Convert certifications and pay distributions data to pandas DataFrames
        certifications_df = pd.DataFrame(certifications_list)
        pay_distributions_df = pd.DataFrame(pay_distributions_list)
        path = 'C:/Users/sturlapaty/VPCADPExtract/'
        workers_df.to_csv(path + 'workers_data.csv', index=False)
        certifications_df.to_csv(path + 'certification_data.csv', index=False)
        pay_distributions_df.to_csv(path + 'payrollFileNumber_data.csv', index=False)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"Other error occurred: {err}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import requests
import os
import pandas as pd
from tqdm import tqdm
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

token_url = "https://accounts.adp.com/auth/oauth/v2/token?grant_type=client_credentials"


def clean_secret_value(secret_value):
    """Cleans up the secret value by removing extraneous whitespace and ensuring proper formatting."""
    secret_value = secret_value.strip()
    clean_lines = [line.strip() for line in secret_value.splitlines() if line.strip()]
    return "\n".join(clean_lines)


def format_pem(secret_value, pem_type):
    """Ensures the secret value is properly formatted as a PEM file."""
    if not secret_value.startswith(f"-----BEGIN {pem_type}-----"):
        secret_value = f"-----BEGIN {pem_type}-----\n{secret_value}\n-----END {pem_type}-----"
    return secret_value


def write_pem_file(content, file_path):
    """Writes the PEM content to a file."""
    with open(file_path, 'wb') as pem_file:
        pem_file.write(content.encode('utf-8'))
    logging.info(f"PEM file written to {file_path}")


def delete_file(file_path):
    """Deletes a file if it exists."""
    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"Deleted file: {file_path}")
    else:
        logging.warning(f"File not found: {file_path}")


def get_access_token(client_id, client_secret, cert_path, key_path):
    """Obtains the OAuth 2.0 token using client certificate and key."""
    credentials = f"{client_id}:{client_secret}"
    encoded_auth = base64.b64encode(credentials.encode()).decode()
    auth_headers = {"Authorization": f"Basic {encoded_auth}"}
    
    try:
        response = requests.post(token_url, headers=auth_headers, cert=(cert_path, key_path))
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to obtain access token: {e}")
        raise


def make_get_request(url, headers, params=None):
    """Makes a GET request with automatic token regeneration."""
    try:
        response = requests.get(url, headers=headers, params=params, cert=(cert_path, key_path))
        if response.status_code == 401:  # Unauthorized, likely due to expired token
            logging.info("Token expired, regenerating...")
            new_token = get_access_token(client_id, client_secret, cert_path, key_path)
            headers['Authorization'] = f'Bearer {new_token}'
            response = requests.get(url, headers=headers, params=params, cert=(cert_path, key_path))
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to make GET request: {e}")
        raise

# Function to get worker data from ADP API with pagination
def get_all_worker_data(access_token):
    api_url = 'https://api.adp.com/hr/v2/workers'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    all_data = []
    skip = 0
    page_size = 50  # Adjust the page size as per your API's pagination limit

    select_fields = (
    'workers/associateOID,'
    'workers/person/legalName/formattedName,'
    'workers/workerID/idValue,'
    'workers/workerDates/originalHireDate,'
    'workers/workerDates/terminationDate,'
    'workers/workerStatus/statusCode/codeValue,'
    'workers/workAssignments/actualStartDate,'
    'workers/workAssignments/managementPositionIndicator,'
    'workers/workAssignments/positionID,'
    'workers/workAssignments/jobCode,'
    'workers/workAssignments/workerTypeCode,'
    'workers/workAssignments/seniorityDate,'
    'workers/workAssignments/jobTitle,'
    'workers/workAssignments/assignedOrganizationalUnits,'
    'workers/workAssignments/assignedWorkLocations,'
    'workers/customFieldGroup'
)


    while True:
        # Construct parameters with $skip and $select
        params = {
            '$skip': skip,
            '$select': select_fields
        }
 
        print(skip)
        # Make a GET request to the API
        response = make_get_request(api_url, headers=headers, params=params)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data.get('workers', []))  # Append current page data to all_data list

            # Check if there are more pages to fetch
            if len(data.get('workers', [])) < page_size:
                break  # Break the loop if the fetched data is less than page_size
            else:
                skip += page_size  # Increment $skip for the next page
        elif response.status_code == 204:
            break
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            print(response.text)  # Print the error message if available
            break

    return all_data



if __name__ == "__main__":
    # Obtain secrets
    client_id = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/', 'adp-client-id')
    client_secret = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/', 'adp-client-secret')
    adp_api_cert = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/', 'adp-cert-v2')
    adp_api_key = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/', 'adp-key-v2')

    # Prepare paths and clean values
    timestamp = int(time.time())
    cert_path = f'api_cert_{timestamp}.pem'
    key_path = f'api_key_{timestamp}.key'

    cleaned_cert_value = clean_secret_value(adp_api_cert)
    cleaned_key_value = clean_secret_value(adp_api_key)

    formatted_cert = format_pem(cleaned_cert_value, "CERTIFICATE")
    formatted_key = format_pem(cleaned_key_value, "PRIVATE KEY")

    # Write the PEM files
    write_pem_file(formatted_cert, cert_path)
    write_pem_file(formatted_key, key_path)

    try:
        # Step 1: Get the access token
        token = get_access_token(client_id, client_secret, cert_path, key_path)
        logging.info(f"Access token obtained: {token}")

        # Step 2: Retrieve all worker data
        workers_data = get_all_worker_data(token)
        print(f"Total workers retrieved: {len(workers_data)}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Clean up files
        delete_file(cert_path)
        delete_file(key_path)


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
