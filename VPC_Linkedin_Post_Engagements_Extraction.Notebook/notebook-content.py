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
from tqdm import tqdm
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Setup logging
logging.basicConfig(filename='posts_engagements.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_post_engagements(post_id, headers):
    """
    Fetch engagement details for a specific post.
    """
    try:
        url = f"https://api.linkedin.com/rest/dmaSocialMetadata?ids=List({post_id})"
        data = make_get_request(url, headers)
        return data.get('results', {}) if data else {}
    except Exception as e:
        logging.error(f"Error fetching post engagements for post ID {post_id}: {e}")
        return {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_posts_engagements_data(org_post_list, headers, processed_post_ids, processed_engagements_file, processed_posts_file):
    """
    Retrieve and process engagement data for a list of post IDs, skipping already processed posts.
    Saves each successfully processed post's engagement data to a file immediately.
    """
    posts_data = []
    failed_posts = []  # To track failed post IDs

    # Filter out posts that have already been processed
    filtered_post_list = [post_id for post_id in org_post_list if post_id not in processed_post_ids]

    for post_id in tqdm(filtered_post_list, desc="Processing Engagements"):
        try:
            data = get_post_engagements(post_id, headers)
            if data:
                # Process and save each post's engagement data immediately
                engagement_data = process_single_post_engagement(data,post_id)
                engagement_data['post_id'] = post_id
                save_post_data(processed_engagements_file, engagement_data)
                
                # Save processed post ID
                save_processed_post_id(processed_posts_file, post_id)
            else:
                failed_posts.append(post_id)  # Append failed post_id to list
        except Exception as e:
            logging.error(f"Error processing engagements data for post ID {post_id}: {e}")
            failed_posts.append(post_id)

    engagements_dict = extract_nested_dicts_from_list(posts_data)
    return engagements_dict, failed_posts  # Return both the data and the failed posts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_single_post_engagement(data,post_id):
    """
    Process a single post's engagement data into a structured dictionary.
    """
    try:
        post_id = post_id.replace('%3A',":")
        result = extract_engagements_from_dict(data,post_id)
        return result
    except KeyError as e:
        logging.error(f"KeyError while processing engagement data: {e}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_engagements_from_dict(d,post_id):
    """
    Extract engagement metrics such as reaction counts and comment summaries from a dictionary.
    """
    try:
        
        reaction_summaries = d.get(post_id,{}).get('reactionSummaries', {})
        reactions_counts = {reaction['reactionType']: reaction['count'] for reaction in reaction_summaries.values()}
        result = {
            **reactions_counts,
            'entity': d.get('entity'),
            'commentSummary_count': d.get('commentSummary', {}).get('count', None)
        }
        return result
    except KeyError as e:
        logging.error(f"KeyError extracting engagements: {e}")
        return {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def post_engagements_data_processing(engagements_dict):
    """
    Convert engagement data into a structured DataFrame.
    """
    engagements_df = pd.DataFrame([extract_engagements_from_dict(d) for d in engagements_dict])
    return engagements_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_processed_post_ids(file_path):
    """
    Loads the IDs of already processed posts from a file.
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
    """
    pd.DataFrame([post_id], columns=['post_id']).to_csv(file_path, mode='a', header=not spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).exists(spark._jvm.org.apache.hadoop.fs.Path(file_path)), index=False)
    logging.info(f"Processed post ID {post_id} saved to {file_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_post_data(file_path, post_data):
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
            final_df = pd.DataFrame(columns= ['post_id','LIKE','PRAISE','APPRECIATION','commentSummary_count','EMPATHY','INTEREST','MAYBE','ENTERTAINMENT'])
        df = pd.DataFrame([post_data])
        final_df = pd.concat([final_df,df])
        final_df.to_csv(file_path, index=False)
        logging.info(f"Processed post data saved for post ID {post_data['post_id']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if __name__ == "__main__":
    """
    Main function to fetch and process post engagements.
    """
    try:
        # Define file paths and initialize headers
        initial_path = get_onelake_path()
        output_path = get_directory(initial_path)
        processed_posts_file = os.path.join(output_path, 'processed_organization_post_engagement_ids.csv')
        processed_engagements_file = os.path.join(output_path, 'processed_organization_post_engagement_data.csv')

        # Load already processed post IDs to skip them in future runs
        processed_post_ids = load_processed_post_ids(processed_posts_file)

        # Load the organization post IDs to be processed
        org_post_list = list(pd.read_csv(output_path + '/organization_post_id.csv')['post_id'])
        headers = get_headers()

        # Limit to a subset of posts if necessary for testing or pagination (optional)
        org_post_list = org_post_list[:400]

        # Process post engagements, skipping already processed posts
        engagements_dict, failed_posts = get_posts_engagements_data(org_post_list, headers, processed_post_ids, processed_engagements_file, processed_posts_file)

        if failed_posts:
            logging.warning(f"Engagement data extraction failed for the following post IDs: {failed_posts}")
        
        logging.info("Engagement data processing completed successfully.")
        
    except Exception as e:
        logging.error(f"An error occurred in the main execution: {e}")

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
