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

%run VPC_Linkedin_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import logging
import concurrent.futures
from tqdm import tqdm
import os
import traceback

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Setup logging
logging.basicConfig(filename='organization_posts.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_post_content_media(post_id, headers):
    """
    Fetches media content for a specific LinkedIn post.

    Args:
        post_id (str): The ID of the post.
        headers (dict): The headers required for the LinkedIn API request.

    Returns:
        dict or None: The media data for the post if found, otherwise None.
    """
    url = f"https://api.linkedin.com/rest/dmaPosts?ids=List({post_id})"
    try:
        data = make_get_request(url, headers)
        if data:
            return data.get('results', [])
        return None
    except Exception as e:
        logging.info(f"Error fetching post content media for post_id {post_id}: {e}")
        return None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_post_data(org_post_list, headers, processed_post_ids, processed_data_file, processed_posts_file):
    """
    Retrieves media content for all posts in the provided list, skipping already processed posts.
    Saves each successfully processed post to a file immediately after it is processed.

    Args:
        org_post_list (list): List of post IDs to process.
        headers (dict): Headers for API requests.
        processed_post_ids (set): Set of already processed post IDs to skip.
        processed_data_file (str): The path to save processed post data.
        processed_posts_file (str): The path to save processed post IDs.

    Returns:
        tuple: DataFrame of processed posts and a list of failed post IDs.
    """
    posts_data = []
    failed_posts = []  # To track failed post IDs

    # Filter out posts that have already been processed
    filtered_post_list = [post_id for post_id in org_post_list if post_id not in processed_post_ids]

    # Sequential processing version

    for post_id in tqdm(filtered_post_list, total=len(filtered_post_list), desc="Processing Posts"):
        # Get content and media for each post (sequentially)
        data = get_post_content_media(post_id, headers)
        
        if data:
            # Process the data into a dataframe row format
            processed_data_row = process_single_post_data(data.get(post_id.replace('%3A', ':')))
            save_processed_data(processed_data_file, processed_data_row)
            
            # Save the processed post ID immediately after it has been successfully processed
            save_processed_post_id(processed_posts_file, post_id)
        else:
            failed_posts.append(post_id)  # Append failed post_id to list

   # nested_dicts = extract_nested_dicts_from_list(posts_data)
    #df = pd.DataFrame(nested_dicts)
    return failed_posts  # Return both the dataframe and the list of failed posts


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_single_post_data(data):
    """
    Processes a single post's raw data into a structured dictionary.

    Args:
        data (dict): Raw post data from the LinkedIn API.

    Returns:
        dict: Processed post data in a structured format.
    """
    try:
        post_info = {
            'id': data['id'],
            'publishedAt': data.get('publishedAt', None),
            'commentary': data.get('commentary', ''),
            'download_urls': extract_media_url(data),
            'isReshareDisabledByAuthor': data.get('isReshareDisabledByAuthor', False),
            'visibility': data.get('visibility', None)
        }
        return post_info
    except KeyError as e:
        logging.error(f"KeyError while processing post data: {e}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_media_url(d):
    """
    Extracts media download URLs from a given dictionary.

    Args:
        d (dict): A dictionary containing media information.

    Returns:
        str: Comma-separated download URLs, or None if no URLs are found.
    """
    try:
        urls = [
            item['media']['media']['image']['downloadUrl']
            for item in [d]
            if isinstance(item, dict) and 'media' in item and 'media' in item['media'] and 'image' in item['media']['media']
        ]
        return ', '.join(urls)
    except KeyError as e:
        logging.warning(f"KeyError while extracting media URL: {e}")
        return None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def post_data_processing(post_content_df):
    """
    Processes the post data to extract relevant fields and media URLs.

    Args:
        post_content_df (DataFrame): DataFrame containing the raw post data.

    Returns:
        DataFrame: Processed DataFrame with relevant fields.
    """
    post_content_df['download_urls'] = post_content_df['content'].apply(extract_media_url)
    post_content_df = post_content_df[['id', 'publishedAt', 'commentary', 'download_urls', 'isReshareDisabledByAuthor', 'visibility']]
    return post_content_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_processed_post_ids(file_path):
    """
    Loads the IDs of already processed posts from a file.

    Args:
        file_path (str): The path to the CSV file containing processed post IDs.

    Returns:
        set: A set of post IDs that have already been processed.
    """
    if spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).exists(spark._jvm.org.apache.hadoop.fs.Path(file_path)):
        df = pd.read_csv(file_path)
        return set(df['post_id'])
    else:
        return set()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_processed_post_id(file_path, post_id):
    """
    Saves a single processed post ID to a file immediately after it has been processed.

    Args:
        file_path (str): The path to the CSV file.
        post_id (str): The post ID that has been processed.
    """
    pd.DataFrame([post_id], columns=['post_id']).to_csv(file_path, mode='a', header=not spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).exists(spark._jvm.org.apache.hadoop.fs.Path(file_path)), index=False)
    logging.info(f"Processed post ID {post_id} saved to {file_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_processed_data(file_path, post_data):
    """
    Saves the processed post data to a CSV file immediately after it has been processed.

    Args:
        file_path (str): The path to the CSV file.
        post_data (dict): The processed post data to be saved.
    """
    if post_data:
        if  spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).exists(spark._jvm.org.apache.hadoop.fs.Path(file_path)):
            final_df = pd.read_csv(file_path)
        else:
            final_df = pd.DataFrame(columns= ['id', 'publishedAt', 'commentary', 'download_urls','isReshareDisabledByAuthor', 'visibility'])
        df = pd.DataFrame([post_data])
        final_df = pd.concat([final_df,df])
        final_df.to_csv(file_path, index=False)
        logging.info(f"Processed post data saved for post ID {post_data['id']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

get_onelake_path()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if __name__ == "__main__":
    try:
        # Define file paths and initialize headers
        initial_path = get_onelake_path()
        output_path = get_directory(initial_path)
        processed_posts_file = os.path.join(output_path, 'processed_organization_post_ids.csv')
        processed_data_file = os.path.join(output_path, 'processed_organization_post_data.csv')

        # Load already processed post IDs to skip them in future runs
        processed_post_ids = load_processed_post_ids(processed_posts_file)

        # Load the organization post IDs to be processed
        org_post_list = list(pd.read_csv(output_path + '/organization_post_id.csv')['post_id'])
        headers = get_headers()

        # Limit to a subset of posts if necessary for testing or pagination (optional)
        org_post_list = org_post_list[:400]
        print(org_post_list)
        # Process post data, skipping already processed posts
        failed_posts = get_post_data(org_post_list, headers, processed_post_ids, processed_data_file, processed_posts_file)

        if failed_posts:
            logging.warning(f"Post data extraction failed for the following post IDs: {failed_posts}")
        
        logging.info("Post data processing completed successfully.")
        
    except Exception as e:
        logging.error(f"An error occurred in the main execution: {e}")
        traceback.print_exc()
        

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
