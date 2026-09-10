# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential


azure_search_endpoint = "https://vpc-dev-aisearch.search.windows.net"
credential = AzureKeyCredential("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") if len("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") > 0 else DefaultAzureCredential()
azure_openai_endpoint = "https://vpc-dev-azureopenai.openai.azure.com/"
azure_openai_key = "c9ce949e276146d4a66b3e16b32613d3" if len("c9ce949e276146d4a66b3e16b32613d3") > 0 else None
azure_openai_chat_deployment = 'gpt-4o'
azure_openai_api_version = "2022-12-01"
embedding_model_name = "text-embedding-ada-002"
search_api_key = "8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u"
search_openai_credential = AzureKeyCredential("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") if len("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") > 0 else DefaultAzureCredential()
EMBEDDING_LENGTH = 1536
# change var
index_name = 'continuous_indexing'
data_name = 'website'
data_table = 'sv_verdantas_website_data'

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

import os
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential

# azure_search_endpoint = ""
# credential = AzureKeyCredential("") if len("") > 0 else DefaultAzureCredential()
# azure_openai_endpoint = ""
# azure_openai_key = "" if len("") > 0 else None
# azure_openai_chat_deployment = ''
# azure_openai_api_version = ""
# embedding_model_name = ""
# search_api_key = ""
# search_openai_credential = AzureKeyCredential("") if len("") > 0 else DefaultAzureCredential()
# index_api_version = ''


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
