# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install openai==0.28.1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
pd.set_option('display.max_columns', None)  # or 1000
pd.set_option('display.max_rows', None)  # or 1000
pd.set_option('display.max_colwidth', None)  # or 199

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    # "adp": f"{lakehouse_path}/br_deltek_merged_data",
    "deltek": f"{lakehouse_path}/br_deltek_merged_data"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read tables into DataFrames
# df_ADP = spark.read.format("delta").load(table_paths["adp"])
df_Deltek = spark.read.format("delta").load(table_paths["deltek"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_Deltek)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltek = df_Deltek.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

endpoint = os.getenv("ENDPOINT_URL", "https://vpc-dev-azureopenai.openai.azure.com/")
deployment = os.getenv("DEPLOYMENT_NAME", "gpt-4o")
search_endpoint = os.getenv("SEARCH_ENDPOINT", "https://vpc-dev-aisearch.search.windows.net/")
search_key = os.getenv("SEARCH_KEY", "8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u")

# token_provider = get_bearer_token_provider(
#     DefaultAzureCredential(),
#     "https://cognitiveservices.azure.com/.default")
      
# client = AzureOpenAI(
#     azure_endpoint=endpoint,
#     azure_ad_token_provider=token_provider,
#     api_version="2024-05-01-preview",
# )

client = AzureOpenAI(
    azure_endpoint=endpoint,
    # azure_ad_token_provider=token_provider,
    api_version="2024-05-01-preview",
    api_key="c9ce949e276146d4a66b3e16b32613d3"
)
      
completion = client.chat.completions.create(
    model=deployment,
    messages= [
    {
      "role": "user",
      "content": "What are the differences between Azure Machine Learning and Azure AI services?"
    }],
    max_tokens=800,
    temperature=0.7,
    top_p=0.95,
    frequency_penalty=0,
    presence_penalty=0,
    stop=None,
    stream=False,
    extra_body={
      "data_sources": [{
          "type": "azure_search",
          "parameters": {
            "filter": None,
            "endpoint": f"{search_endpoint}",
            "index_name": "vpc-adp-deltek-updated-index",
            "semantic_configuration": "azureml-default",
            "authentication": {
              "type": "api_key",
              "key": f"{search_key}"
            },
            "embedding_dependency": {
              "type": "endpoint",
              "endpoint": "https://vpcdevaihub2889568068.openai.azure.com/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-07-01-preview",
              "authentication": {
                "type": "api_key",
                "key": "d02cbbc8443d4a95ba6d01a7e70d9a01"
              }
            },
            "query_type": "vector_simple_hybrid", # Hybrid
            "query_type":"vector_semantic_hybrid", # Hybrid+Semantic
            "in_scope": True,
            "role_information": "You are an AI assistant that helps people find information.",
            "strictness": 3,
            "top_n_documents": 5
          }
        }]
    }
)
print(completion.to_json())

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
