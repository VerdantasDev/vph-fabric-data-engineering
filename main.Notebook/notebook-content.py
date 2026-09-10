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

%pip install --upgrade pip
%pip install openai==1.12.0
%pip install langchain==0.0.354

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run logging_config


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run index_schemas


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run content_chunker

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run content_summarizer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run data_uploader

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install azure-identity azure-keyvault-secrets



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install azure-synapse-spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

key_vault_uri = 'https://vpc-dev-keyvault.vault.azure.net/'
 
ai_search_key = mssparkutils.credentials.getSecret(key_vault_uri, 'azure-aisearch-key')
openai_key = mssparkutils.credentials.getSecret(key_vault_uri, 'azure-openai-key')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ai_search_key

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential



azure_openai_key = "c9ce949e276146d4a66b3e16b32613d3" if len("c9ce949e276146d4a66b3e16b32613d3") > 0 else None

search_openai_credential = AzureKeyCredential("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") if len("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") > 0 else DefaultAzureCredential()
EMBEDDING_LENGTH = 1536
# change var
index_name = 'continuous_indexing_fabric'
data_name = 'website'
data_table = 'sv_verdantas_website_data'
index_api_version = 'api-version=2023-11-01'
client = AzureOpenAI(
  api_key = azure_openai_key,  
  api_version = azure_openai_api_version,
  azure_endpoint =azure_openai_endpoint
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import json
import re
import logging

setup_logging()
# from dotenv import load_dotenv
# load_dotenv(override=True) # take environment variables from .env.
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace


# The following variables from your .env file are used in this notebook
# from config import (
# azure_openai_chat_deployment,
# index_name,
# data_name,
# data_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Read the data 
# Define paths to your Lakehouse tables 
lakehouse_path = "Tables"
table_paths = {
    "website": f"{lakehouse_path}/{data_table}",
}
website = spark.read.format("delta").load(table_paths["website"])
df = website.toPandas().fillna('NA')
# df['UniqueId'] = df['UniqueId'].astype(str)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_unique_values(row):
    unique_values = []
    for key in row:
        unique_values.extend(row[key])
    return list(set(unique_values))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Apply the function to the 'tags_associated' column
if data_name=='document':
    df['tags_associated'] = df['tags_associated'].apply(lambda x: extract_unique_values(eval(x)))

content_columns = ['ProjectContent','ExpertiseContent','ServiceContent','SolutionContent','MarketContent']
chunk_group = 'ExpertiseName'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##website


import pandas as pd
import re

# df = data.toPandas()
df =df.head()
def website_preprocessing(df):
    df['id'] = range(1, len(df) + 1)
    df['id'] = df['id'].astype(str)
    rename_dict = {
        'project_name':'ProjectName',
        'project_url':'ProjectUrl',
        'project_content':'ProjectContent',
        'expertise_name':'ExpertiseName',
        'expertise_url':'ExpertiseUrl',
        'expertise_content':'ExpertiseContent',
        'expertise_contact_person':'ExpertiseContactPerson',
        'expertise_contact_person_designation':'ExpertiseContactDesignation',
        'service_name':'ServiceName',
        'service_content':'ServiceContent',
        'market_name':'MarketName',
        'market_url':'MarketUrl',
        'market_content':'MarketContent',
        'market_contact_person':'MarketContactPerson',
        'market_contact_person_designation':'MarketContactDesignation',
        'solution_name':'SolutionName',
        'solution_url':'SolutionUrl',
        'solution_content':'SolutionContent'
    }
    df = df.rename(columns=rename_dict)
    return df
df = website_preprocessing(df)

chunk_group = 'ExpertiseName'

content_columns = ['ProjectContent','ExpertiseContent','ServiceContent','SolutionContent','MarketContent']

# result_df, status = webs(df=df, columns=content_columns, chunk_group=chunk_group)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# schema_status = index_schemas_main(data_name,index_name)

# df,summary_status = content_summarizer.summarize_content(df,azure_openai_chat_deployment,content_columns,detail=0.75,minimum_chunk_size=200,additional_instructions="Write in sections having heading and subheading in structured format without whitespace and avoid duplicate headings.")

# df,chunk_status = content_chunker.main(df,content_columns,chunk_group)

# upload_status = data_uploader_main(df,data_name)

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
