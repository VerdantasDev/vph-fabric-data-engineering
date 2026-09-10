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
#from VPC_Linkedin_utils import make_get_request, get_headers, get_directory
import logging
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Setup logging
logging.basicConfig(filename='organization_posts.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_organization_posts(org_page_id, headers):
    """
    Fetch all LinkedIn posts for a specific organization using its LinkedIn page ID.
    
    Args:
        org_page_id (int): LinkedIn page ID of the organization.
        headers (dict): Dictionary containing the necessary headers for API authentication.

    Returns:
        list: A list of post IDs from the organization.
        str: A status message indicating the result of the operation ('success' or 'failure').
    """
    base_url = f"https://api.linkedin.com/rest/dmaFeedContentsExternal?author=urn%3Ali%3Aorganization%3A{org_page_id}&maxPaginationCount=1000&q=postsByAuthor"
    all_posts = []
    next_cursor = None
    params = {}

    try:
        while True:
            url = base_url
            if next_cursor:
                url += f"&paginationCursor={next_cursor}"
            
            logging.info(f"Fetching data from URL: {url}")
            data = make_get_request(url, headers, params)
            
            # Check if the response contains elements
            if data and 'elements' in data:
                posts = data['elements']
                if not posts:
                    logging.info("No more posts available.")
                    break
                
                all_posts.extend(posts)
                next_cursor = data.get('metadata', {}).get('paginationCursorMetdata', {}).get('nextPaginationCursor')
                
                if not next_cursor:
                    logging.info("No further pagination cursor available. Data fetching completed.")
                    break
            else:
                logging.warning(f"No data returned for organization posts with ID: {org_page_id}")
                return [], 'failure: no data returned'
        
        # Convert post IDs to URL-encoded format and return the list
        if all_posts:
            org_post_list = pd.DataFrame(all_posts)['id']
            org_post_list = [url.replace(':', '%3A') for url in org_post_list]
            logging.info(f"Fetched {len(org_post_list)} posts for organization {org_page_id}.")  # Number of posts logged here
            return org_post_list, 'success'
        else:
            logging.warning(f"No posts found for organization {org_page_id}.")
            return [], 'failure: no posts found'
    
    except Exception as e:
        logging.error(f"An error occurred while fetching posts: {str(e)}")
        return [], f'failure: {str(e)}'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    """
    Main function to fetch and process post content.
    
    Returns:
        str: Status message indicating success or failure of the process.
    """
    try:
        # Get authentication headers
        headers = get_headers()
        
        # Organization LinkedIn ID
        organization_id = 80975288  # Verdantas
        
        # Get directory to save the output file
        initial_path = get_onelake_path()
        output_path = get_directory(initial_path)
        output_file = os.path.join(output_path, 'organization_post_id.csv')
        
        # Check if the CSV file already exists
        if os.path.exists(output_file):
            logging.info(f"File {output_file} already exists. Skipping data fetching.")
            return 'success: file already exists, process skipped'
        
        # Fetch posts from LinkedIn
        org_post_list, status = get_organization_posts(organization_id, headers)
        
        if status == 'success':
            # Save the post IDs to a CSV file
            org_post_df = pd.DataFrame(org_post_list, columns=['post_id'])
            org_post_df.to_csv(output_file, index=False)
            logging.info(f"Post IDs saved to {output_file}")
            logging.info(f"Total number of posts extracted: {len(org_post_list)}")  # Logging the number of posts extracted
            
            return 'success: post data fetched and saved successfully'
        
        else:
            logging.error(f"Failed to fetch posts: {status}")
            return status
    
    except Exception as e:
        logging.error(f"An error occurred in the main function: {str(e)}")
        return f'failure: {str(e)}'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if __name__ == "__main__":
    # Run the main function and get the status
    status = main()
    print(f"Process completed with status: {status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
