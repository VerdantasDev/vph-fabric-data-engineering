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

import base64
import requests
import os
import pandas as pd
from tqdm import tqdm
import time
import logging
import json
from datetime import datetime,timedelta
from notebookutils import fs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

# dates
today = datetime.today()
today_str = today.strftime("%Y-%m-%d")
year_str = today.year
month_str = today.strftime("%m")
day_str = today.strftime("%d")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_path = 'abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623'
path = f"{lakehouse_path}/Files/bronze/adp/v1/{year_str}/{month_str}/{day_str}/"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Define the paths for the data files
# data_paths = {
#     'certifications': f'{path}certifications.pkl',
#     'pay_distributions': f'{path}pay_distributions.pkl',
#     'skills': f'{path}skills.pkl',
#     'licenses': f'{path}licenses.pkl'
# }

# progress_file = f'{path}progress.pkl'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the folder exists
if not spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()).exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
    # Create the folder
    spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()).mkdirs(spark._jvm.org.apache.hadoop.fs.Path(path))
    print(f"Folder created: {path}")
else:
    print(f"Folder already exists: {path}")

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

progress_file = path + "progress.json"
output_file = path + "adp_data.pkl"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_secret_value(secret_value):
    """Cleans up the secret value by removing extraneous whitespace and ensuring proper formatting."""
    secret_value = secret_value.strip()
    clean_lines = [line.strip() for line in secret_value.splitlines() if line.strip()]
    return "\n".join(clean_lines)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def format_pem(secret_value, pem_type):
    """Ensures the secret value is properly formatted as a PEM file."""
    if not secret_value.startswith(f"-----BEGIN {pem_type}-----"):
        secret_value = f"-----BEGIN {pem_type}-----\n{secret_value}\n-----END {pem_type}-----"
    return secret_value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_pem_file(content, file_path):
    """Writes the PEM content to a file."""
    with open(file_path, 'wb') as pem_file:
        pem_file.write(content.encode('utf-8'))
    logging.info(f"PEM file written to {file_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def delete_file(file_path):
    """Deletes a file if it exists."""
    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"Deleted file: {file_path}")
    else:
        logging.warning(f"File not found: {file_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def make_get_request(url, headers, params=None):
    """Makes a GET request with automatic token regeneration."""
    if params is None:
        params = {}

    try:
        response = requests.get(url, headers=headers, params=params, cert=(cert_path, key_path))
        
        if response.status_code == 401 or response.status_code == 403:  # Unauthorized, likely due to expired token
            logging.info("Token expired, regenerating...")
            try:
                new_token = get_access_token(client_id, client_secret, cert_path, key_path)
                headers['Authorization'] = f'Bearer {new_token}'
                time.sleep(60)  # Wait for 60 seconds before retrying
                response = requests.get(url, headers=headers, params=params, cert=(cert_path, key_path))
            except Exception as token_error:
                logging.error(f"Failed to regenerate token: {token_error}")
                raise
        
        response.raise_for_status()
        return response
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to make GET request: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
            # break

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

def get_licenses(access_token, associate_oid):
    api_url = f'https://api.adp.com/talent/v2/associates/{associate_oid}/associate-licenses'
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
        print(f"Failed to retrieve licenses for {associate_oid}: {response.status_code}")
        return None


def get_skills(access_token, associate_oid):
    api_url = f'https://api.adp.com/talent/v2/associates/{associate_oid}/associate-competencies'
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
        print(f"Failed to retrieve skills for {associate_oid}: {response.status_code}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_progress(progress_data):
    """Saves the progress of the data fetching process."""
    json_string = json.dumps(progress_data, indent=4)
    fs.put(progress_file, json_string, True)

#    notebookutils.fs.put(progress_file, progress_data, True)
    # with open(progress_file, 'w') as f:
    #     json.dump(progress_data, f)

def load_progress():
    """Loads the progress of the data fetching process."""
    if any(f.name == "progress.json" for f in fs.ls(path)):
        json_string = fs.head(progress_file)
        return json.loads(json_string)
    else:
        # If the file doesn't exist, create an empty progress structure
        return {
            'last_processed_index': 0,
            'certifications': [],
            'pay_distributions': [],
            # 'skills': [],
            'licenses': [],
            'associate_oids': []
        }

def load_existing_data(file_path):
    """Loads existing data from a .pkl file."""
    if os.path.exists(file_path):
        return pd.read_pickle(file_path)
    return pd.DataFrame()

def save_data(dataframe, file_path):
    """Saves data to a .pkl file, appending to existing data if the file exists."""
    if os.path.exists(file_path):
        existing_df = pd.read_pickle(file_path)
        combined_df = pd.concat([existing_df, dataframe], ignore_index=True)
    else:
        combined_df = dataframe
    combined_df.to_pickle(file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
        print(token)
        progress = load_progress()
       # print(progress)
        associate_oids = progress.get('associate_oids', [])

        if not associate_oids:
            logging.info("No previous progress found. Fetching all worker data.")
            all_workers = get_all_worker_data(token)

            workers_df = pd.json_normalize(all_workers)

            workers_df.to_csv(path + 'workers_data.csv',index=False)

            associate_oids = [worker['associateOID'] for worker in all_workers]

            # Initialize progress data
            progress = {
                'last_processed_index': 0,
                'certifications': [],
                'pay_distributions': [],
                # 'skills': [],
                'licenses': [],
                'associate_oids': associate_oids,
                'processed_oids': []
            }
            save_progress(progress)
        else:
            logging.info("Continuing from saved progress.")

        # Retrieve processed associate OIDs
        processed_oids = set(progress['processed_oids'])

        # Data lists
        certifications_list = progress.get('certifications', [])
        pay_distributions_list = progress.get('pay_distributions', [])
        licenses_list = progress.get('licenses', [])
        # skills_list = progress.get('skills', [])

        # Fetch data
        start_index = progress.get('last_processed_index', 0)

        for i, oid in enumerate(tqdm(associate_oids[start_index:], desc="Fetching data"), start=start_index):
            print(i)
            if oid in processed_oids:
                logging.info(f"Skipping already processed associateOID: {oid}")
                continue

            try:
                certs = get_certifications(token, oid)
                pay_dist = get_pay_distributions(token, oid)
                licenses = get_licenses(token, oid)
              #  skills = get_skills(token, oid)

                if certs:
                    certifications_list.append({'associateOID': oid, 'certifications': certs})
                if pay_dist:
                    pay_distributions_list.append(pay_dist)
                if licenses:
                    licenses_list.append({'associateOID': oid, 'licenses': licenses})
                # if skills:
                #     skills_list.append({'associateOID': oid, 'skills': skills})

                # Save progress every 5 requests to avoid data loss
                if i % 5 == 0:
                    progress = {
                        'last_processed_index': i,
                        'certifications': certifications_list,
                        'pay_distributions': pay_distributions_list,
                        # 'skills': skills_list,
                        'licenses': licenses_list,
                        'associate_oids': associate_oids,
                        'processed_oids': list(processed_oids.union({oid}))
                    }
                    save_progress(progress)

            except Exception as e:
                logging.error(f"Error processing associateOID {oid}: {e}")
                continue

        # # Final save of progress and data
        # progress = {
        #     'last_processed_index': len(associate_oids) - 1,
        #     'certifications': certifications_list,
        #     'pay_distributions': pay_distributions_list,
        #     'skills': skills_list,
        #     'licenses': licenses_list,
        #     'associate_oids': associate_oids,
        #     'processed_oids': list(processed_oids)
        # }
        # save_progress(progress)

        # Combine all data into a single DataFrame and save to a .pkl file
        cert_df = pd.DataFrame(certifications_list)
        pay_dist_df = pd.DataFrame(pay_distributions_list)
        licenses_df = pd.DataFrame(licenses_list)
        # skills_df = pd.DataFrame(skills_list)

        cert_df.to_csv(path + 'certification_data.csv',index=False)
        pay_dist_df.to_csv(path + 'payroll_data.csv',index=False)
        licenses_df.to_csv(path + 'licenses_data.csv',index=False)
        # skills_df.to_csv(path + 'skills_data.csv',index=False)

        # combined_df = pd.concat([cert_df, pay_dist_df, licenses_df, skills_df], ignore_index=True)
        # save_data(combined_df, output_file)
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
