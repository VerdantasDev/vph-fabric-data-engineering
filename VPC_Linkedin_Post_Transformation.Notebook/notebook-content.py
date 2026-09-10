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
from datetime import datetime
import traceback

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_post_engagements(df):
    df['post_id'] = df['post_id'].apply(lambda x:x.replace('%3A',':'))
    df = df[['post_id', 'LIKE', 'PRAISE', 'APPRECIATION','EMPATHY', 'INTEREST', 'MAYBE', 'ENTERTAINMENT', 'commentSummary_count']]
    df.columns = ['postId', 'like','praise', 'appreciation', 'empathy','interest','maybe','entertainment','comments']

    columns_to_sum = ['like','praise', 'appreciation', 'empathy','interest','maybe','entertainment','comments']

    # Sum the values of the specified columns
    df['total_engagements'] = df[columns_to_sum].fillna(0).astype(float).astype(int).sum(axis= 1)

    finalDf = pd.DataFrame(columns = ['postId', 'like','praise', 'appreciation', 'empathy','interest','maybe','entertainment','comments'])

    finalDf = pd.concat([finalDf,df])

    return finalDf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_post_content(df):
    df['id'] = df['id'].apply(lambda x:x.replace('%3A',':'))
    df['postType'] = df['id'].apply(lambda x: x.split(':')[-2])
    # Convert milliseconds to seconds
    df['publishedAt'] = df['publishedAt'] / 1000
    
    # Convert to datetime
    df['publishedDate'] = pd.to_datetime(df['publishedAt'], unit='s')
    df = df[['id', 'publishedDate','postType', 'commentary', 'download_urls',
       'isReshareDisabledByAuthor', 'visibility']]
    df.columns = ['postId', 'publishedDate','postType', 'postContent', 'imageURLs',
       'isReshareDisabledByAuthor', 'visibilityType']
    
    return df

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

        post_content_file = os.path.join(output_path, 'processed_organization_post_data.csv')
        post_enagagements_file = os.path.join(output_path, 'processed_organization_post_engagement_data.csv')

        post_content_df = pd.read_csv(post_content_file)

        final_post_content = transform_post_content(post_content_df)
        final_post_content['dateMeasured'] =  datetime.now().date()
        final_post_content = spark.createDataFrame(final_post_content)
        write_to_target(final_post_content,'Tables/br_linkedin_post_content',['postId'])
        post_enagagements_df = pd.read_csv(post_enagagements_file)
        final_post_engagements = transform_post_engagements(post_enagagements_df)
        final_post_engagements['dateMeasured'] =  datetime.now().date()
        final_post_engagements = spark.createDataFrame(final_post_engagements)
        write_to_target(final_post_engagements,'Tables/br_linkedin_post_engagements',['postId'])
    except Exception as e:
        logging.error(f"An error occurred in the main execution: {e}")
        traceback.print_exc()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
