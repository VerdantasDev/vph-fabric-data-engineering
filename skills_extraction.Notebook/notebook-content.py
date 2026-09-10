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

%pip install openai==1.12.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import ast
from openai import AzureOpenAI

from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import from_json, col, regexp_replace

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.appName("SkillsExtraction").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_chat_completion(messages,model):
    response = client.chat.completions.create(
        model=model,
        messages=messages
    )
    return response.choices[0].message.content

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def model_sills(text):
    messages = [
        {"role": "system", "content": """Analyze the given content summary and provide consolidated list of skills.             
            Do not provide any other details.
            
            Follow below response format only -
            
            ['geostatic engineer','project management']
            """},
        {"role": "user", "content": text}
    ]

    # Assuming this function gets the completion and works as expected
    response = get_chat_completion(messages, model=azure_openai_chat_deployment)

    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def safe_apply(content):
    try:
        return ast.literal_eval(model_sills(content))
    except Exception as e:
        print(f"Error: {e}")
        try:
            return ast.literal_eval(model_sills(content))
        except Exception as e:
            print(f"Error: {e}")
            return ['issue']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

azure_openai_chat_deployment = 'gpt-4o-dev'

azure_openai_api_version = "2023-03-15-preview"
azure_openai_endpoint = "https://devops-test-openai.openai.azure.com"
azure_openai_key = "b19770cbae334eccbc0ace41b7e23438" if len("c9ce949e276146d4a66b3e16b32613d3") > 0 else None


client = AzureOpenAI(
        api_key = azure_openai_key,  
        api_version=azure_openai_api_version,
        azure_endpoint = azure_openai_endpoint)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load('Tables/br_acquia_content_ADI_v2')
df = df.filter(df.collection_name == 'Verdantas Resumes')
df = df.select('filename', 'content')

df = df.toPandas()
df['skills'] = df['content'].apply(safe_apply)

sparkDF=spark.createDataFrame(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sparkDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
