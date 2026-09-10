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

from openai import AzureOpenAI

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import openai

# Set the base URL and API version for Azure
openai.api_type = "azure"
openai.api_key = "b19770cbae334eccbc0ace41b7e23438"
openai.api_base = "https://devops-test-openai.openai.azure.com"  # Example: https://myopenai.openai.azure.com
openai.api_version = "2023-03-15-preview"  # Update based on the latest Azure OpenAI version


azure_openai_chat_deployment = 'gpt-4o-dev'

azure_openai_api_version = "2023-03-15-preview"
azure_openai_endpoint = "https://devops-test-openai.openai.azure.com"
azure_openai_key = "b19770cbae334eccbc0ace41b7e23438" if len("c9ce949e276146d4a66b3e16b32613d3") > 0 else None



# Example API call to get a response
response = openai.Completion.create(
    engine=azure_openai_chat_deployment,  # Azure OpenAI deployment name
    prompt="Hello, how are you?",
    max_tokens=50
)

print(response.choices[0].text.strip())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
