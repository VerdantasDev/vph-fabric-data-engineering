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

import requests
import logging
from datetime import datetime
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Setup logging
LOG_FILENAME = 'common.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_headers(access_token=None):
    """
    Generate headers for LinkedIn API requests.

    Args:
        access_token (str): Optional. LinkedIn API access token. If not provided, the function 
                            uses the default token, which should be stored in a secure location.
    
    Returns:
        dict: Headers containing the necessary authorization for API requests.
    
    Raises:
        ValueError: If the access token is not provided or invalid.
    """
    if access_token is None:
        # Fetch the access token from environment variables or a secure place
      #  access_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
        access_token = 'AQUv_3yCocXZAdTZysrOx97fiqJMe26r1zGTbEitTg_aCDTbGrDUe0IF42WObbe5jThccYWk_uxpAxhBI-pB1vUzlOLsbJkMRF7mvlbdA0khLJNXZV4gZ1p-y1I69vqmL7PDMN5KJDInrDjLS7wz590_IN1VGGU_A1zworpAGZQULAKw1h1XW8-naQr36IeMor_j9I95nMJJzR3m_TmFjiibHP6xihmnj_trGFg833kGG7I_KYtXTSwN8s9yJikiuCYzjyl9jXB_xGcyJh5_hy9XOUQmK-2JzPqAQm01mp2IdKqwySafpWdPMNm0wIm4PHAZ85K3Tww-lUGUaLm6kAhx3a1k_A'
    
    if not access_token:
        logging.error("Access token is missing. Please provide a valid LinkedIn API token.")
        raise ValueError("Access token is required for API requests.")

    headers = {
        'Linkedin-Version': '202405',
        'X-Restli-Protocol-Version': '2.0.0',
        'Authorization': f'Bearer {access_token}',
    }

    logging.info("Headers generated successfully for API request.")
    return headers


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def make_get_request(url, headers, payload=None):
    """
    Make a GET request to the specified URL with headers and an optional payload.

    Args:
        url (str): The URL to which the GET request is made.
        headers (dict): Headers to include in the GET request.
        payload (dict): Optional. A dictionary of additional parameters for the request.

    Returns:
        dict: The response content in JSON format if successful.
    
    Raises:
        requests.exceptions.RequestException: If the request fails.
    """
    if payload is None:
        payload = {}
    
    try:
        logging.info(f"Making GET request to URL: {url}")
        response = requests.get(url, headers=headers, params=payload)
        response.raise_for_status()  # Raise an exception for 4XX/5XX errors
        logging.info(f"GET request to {url} succeeded.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error making GET request to {url}: {e}")
        raise  # Let the calling function handle the exception


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_nested_dicts_from_list(dict_list):
    """
    Extract nested dictionaries from a list of dictionaries.

    Args:
        dict_list (list): A list of dictionaries from which nested dictionaries will be extracted.

    Returns:
        list: A list of nested dictionaries extracted from the input list.
    """
    if not isinstance(dict_list, list):
        logging.error("Input to extract_nested_dicts_from_list must be a list.")
        raise ValueError("Input should be a list of dictionaries.")

    nested_dicts = []
    for d in dict_list:
        for v in d.values():
            if isinstance(v, dict):
                nested_dicts.append(v)
    
    logging.info(f"Extracted {len(nested_dicts)} nested dictionaries.")
    return nested_dicts


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_directory(directory):
    """
    Creates a directory if it doesn't exist.

    Args:
        directory (str): The path of the directory to create.

    Returns:
        None
    """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info(f"Directory '{directory}' created successfully.")
            print(f"Directory '{directory}' created successfully!")
        else:
            logging.info(f"Directory '{directory}' already exists.")
            print(f"Directory '{directory}' already exists.")
    except OSError as e:
        logging.error(f"Failed to create directory '{directory}': {e}")
        raise



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def date_folder(today=None):
    """
    Generate a date-based folder structure (Year/Month/Day).

    Args:
        today (datetime): Optional. The date object to format. Defaults to the current date.

    Returns:
        str: A folder path based on the date.
    """
    if today is None:
        today = datetime.today()
    
    year_str = today.year
    month_str = today.strftime("%m")
    day_str = today.strftime("%d")

    date_path = f'{year_str}/{month_str}/{day_str}'
    logging.info(f"Generated date folder path: {date_path}")
    
    return date_path


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_directory(initial_path):
    """
    Create and return the full directory path for saving files. The path is composed of 
    the initial base path, a predefined pipeline path, and a date folder.

    Args:
        initial_path (str): The base directory path to start from.

    Returns:
        str: The final directory path, including pipeline and date folder.
    """
    pipeline_name_path = 'Files/bronze/linkedin/v1'
    final_path = os.path.join(initial_path, pipeline_name_path, date_folder())

    try:
        create_directory(final_path)
    except Exception as e:
        logging.error(f"Failed to create directory structure: {e}")
        raise

    logging.info(f"Final directory path generated: {final_path}")
    return final_path


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
