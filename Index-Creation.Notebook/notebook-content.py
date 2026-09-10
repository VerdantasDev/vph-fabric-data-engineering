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

from pyspark.sql import SparkSession
import pandas as pd
pd.set_option('display.max_columns', None)  # or 1000
pd.set_option('display.max_rows', None)  # or 1000
pd.set_option('display.max_colwidth', None)  # or 199


# from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
import os

from config import (
    azure_search_endpoint,
    credential,
    azure_openai_endpoint,
    azure_openai_key,
    azure_openai_chat_deployment,
    azure_openai_chat_deployment,
    azure_openai_api_version,
    embedding_model_name,
    openai_credential,
    search_api_key,

   
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LakehouseTables") \
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
    "adp-deltek": f"{lakehouse_path}/sv_adp_deltek_data",
    
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read tables into DataFrames
df_ADP_deltek = spark.read.format("delta").load(table_paths["adp-deltek"])
df = df_ADP_deltek.toPandas()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df["maximumRegularPayRate"].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df["currentRegularPay"] = df["currentRegularPay"].astype(float)
df["maximumRegularPayRate"] = df["maximumRegularPayRate"].astype(str)

df = remove_doller(df, ["maximumRegularPayRate"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import math
def custom_rounding(value):
    if pd.isna(value):
        return value
    if float(value) - math.floor(float(value)) < 0.5:
        return math.floor(float(value))
    else:
        return math.ceil(float(value))




def remove_doller(df, col_list):
    for col in col_list:
        df[col] = df[col].str.replace('$', '', regex=False).astype(float)
        df[col] = df[col].apply(custom_rounding)

    return df

def rounding(df,col_list):
    for col in col_list:
        df[col] = df[col].astype(float)
        df[col] = df[col].apply(custom_rounding)

    return df
    
df["minimumRegularPayRate"] = df["minimumRegularPayRate"].astype(str)

df = remove_doller(df,["minimumRegularPayRate","averageRegularPayRate","maximumOvertimePayRate",	
"minimumOvertimePayRate","averageOvertimePayRate","averageProjectFee", "maximumHourlyEmployeeRate",
 "provisionalBillingRate"])

# df = remove_doller(df,["minimumRegularPayRate"])


# df = rounding(df,["regularHours","overtimeHours","totalHours","hoursPerDay"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for column in df.columns:
    if df[column].dtype == 'float64':
        df[column].fillna(0, inplace=True)
    elif df[column].dtype == 'object':
        df[column].fillna('NA', inplace=True)

# df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = rounding(df,["regularHours","overtimeHours","totalHours","hoursPerDay"])

df = rounding(df,["regularHours","overtimeHours","totalHours","hoursPerDay","minimumRegularPayRate","averageRegularPayRate","maximumOvertimePayRate",	
"minimumOvertimePayRate","averageOvertimePayRate","averageProjectFee", "maximumHourlyEmployeeRate",
 "provisionalBillingRate"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.rename(columns = {"otherFirmsExpYears": "ExperienceInOtherFirm"}, inplace = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = rounding(df,["maximumRegularPayRate","ExperienceInOtherFirm", "activeProjectCount","inactiveProjectCount",
    "dormantProjectCount", "adhocProjectCount","proposalWonProjectCount","currentRegularPay"])

# df = rounding(df,["maximumRegularPayRate"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.isna().sum()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.rename(columns={"employeeId": "EmployeeID","adhocProjectCount": "DirectProjectCounts",}, inplace = True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df.drop(columns=["EmployeeID"], inplace = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def convert_to_str(df):
#     for col in df.columns:
#         if df[col].dtype in ["datetime64[ns]", "float64","bool"]:
#             df[col] = df[col].astype(str)
        
#     return df

# # Convert int and float columns to str
# df = convert_to_str(df)

def datatype_conversion(df):
    for col in df.columns:
        if df[col].dtype in ["datetime64[ns]","bool"]:
            df[col] = df[col].astype(str)
        if df[col].dtype in ["float64"]:
            df[col] = df[col].astype(int)
        
    return df

# Convert int and float columns to str
df = datatype_conversion(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.isna().sum()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## old approach

# CELL ********************

import os
import math
import json
import pandas as pd
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient


search_service_name = "vpc-dev-aisearch"

endpoint = f"https://{search_service_name}.search.windows.net"
admin_client = SearchIndexClient(endpoint=endpoint, credential=AzureKeyCredential(search_api_key))



def custom_rounding(value):
    if pd.isna(value):
        return value
    if float(value) - math.floor(float(value)) < 0.5:
        return math.floor(float(value))
    else:
        return math.ceil(float(value))

# Function to convert object columns to integers with custom rounding
def convert_object_to_int(df, col_list):
    for col in col_list:
        # Try to convert the column to numeric, coerce errors to NaN
        df[col] = pd.to_numeric(df[col], errors='coerce')
        # Apply custom rounding
        df[col] = df[col].apply(custom_rounding)
        # Convert to integers
        df[col] = df[col].astype('Int64')
    return df



df = convert_object_to_int(df, ["activeProjectCount","inactiveProjectCount","dormantProjectCount","DirectProjectCounts","proposalWonProjectCount","ExperienceInOtherFirm"])

def convert_to_str(df):
    for col in df.columns:
        if df[col].dtype in ["datetime64[ns]", "float64","bool"]:
            df[col] = df[col].astype(str)
        
    return df

# Convert int and float columns to str
df = convert_to_str(df)

print(df.info())







# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def convert_to(df):
    for col in df.columns:
        if df[col].dtype in ["Int64"]:
            df[col] = df[col].astype(str)
        
    return df

# Convert int and float columns to str
df = convert_to(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.fillna("NA",inplace = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from decimal import Decimal
documents = []
for index, row in df.iterrows():
    document = row.to_dict()
    # Remove NaN values from the dictionary
    cleaned_document = {k: v for k, v in document.items() if pd.notna(v)}
    for key, value in cleaned_document.items():
        if isinstance(value, Decimal):
            cleaned_document[key] = str(value)
    cleaned_document["id"] = str(index)  # Ensure each document has a unique ID
    documents.append(cleaned_document)

# Convert the list of dictionaries to a JSON string
documents_json = json.dumps(documents)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

documents_json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SimpleField,
    SearchFieldDataType,
    SearchableField,
    SearchField,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
    SemanticConfiguration,
    SemanticPrioritizedFields,
    SemanticField,
    SemanticSearch,
    SearchIndex
)


# Create a search index
index_client = SearchIndexClient(
    endpoint=endpoint, credential=credential)
fields = [
    # SearchField(name="id", type=SearchFieldDataType.String, key=True),
    SearchableField(name="EmployeeID", type=SearchFieldDataType.String, filterable=True, key=True),
    SearchableField(name="EmployeeName", type=SearchFieldDataType.String, filterable=True, retrievable = True),
    SearchableField(name="workerId", type=SearchFieldDataType.String, filterable=True),
    SearchableField(name="EmploymentStatus", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="EmployeeDesignationStartDate", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="EmployeeDesignationCode", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="EmployeeDesignation", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="EmployeeManagementsPositionIndicator", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="CertificationCode", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="NameOfCertification", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="EmployeeCode", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="projectType", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="projectArea", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="regularHours", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="overtimeHours", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="totalHours", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="maximumRegularPayRate", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="minimumRegularPayRate", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="averageRegularPayRate", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="maximumOvertimePayRate", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="minimumOvertimePayRate", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="averageOvertimePayRate", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="averageProjectFee", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="activeProjectCount", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="inactiveProjectCount", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="dormantProjectCount", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="DirectProjectCounts", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="proposalWonProjectCount", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="currentRegularPay", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="companyId", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="billingType", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="hoursPerDay", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="ExperienceInOtherFirm", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="practiceGroupCode", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="officeDepartment", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="overtimeAllowed", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="customerBillingType", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="companyName", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="maximumHourlyEmployeeRate", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="payrollEnabledFlag", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
    SearchableField(name="payrollFrequency", type=SearchFieldDataType.String,filterable=True,  retrievable = True),
       
]





semantic_config = SemanticConfiguration(
    name="azureml-default",
    prioritized_fields=SemanticPrioritizedFields(
        title_field=SemanticField(field_name="EmployeeID"),
        content_fields=[SemanticField(field_name="EmployeeDesignation")]
    )
)

# Create the semantic settings with the configuration
semantic_search = SemanticSearch(configurations=[semantic_config])

# Create the search index with the semantic settings
index = SearchIndex(name=index_name, fields=fields, semantic_search=semantic_search)
result = index_client.create_or_update_index(index)
print(f' {result.name} created')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.isnull().sum()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.search.documents import SearchClient


search_client = SearchClient(endpoint=endpoint, index_name=index_name, credential=credential)
result = search_client.upload_documents(documents_json)
print(f"Uploaded {len(documents)} documents")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## New Approach

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

# Length of the embedding vector (OpenAI ada-002 generates embeddings of length 1536)
EMBEDDING_LENGTH = 1536

search_service_name = "vpc-dev-aisearch"


# Create index for AI Search with fields id, content, and contentVector
# Note the datatypes for each field below
url = f"https://{search_service_name}.search.windows.net/indexes/{index_name}?api-version=2023-11-01"
payload = json.dumps(
    {
        "name": index_name,
        "fields": [
            # Unique identifier for each document

            {
                "name": "id",
                "type": "Edm.String",
                "key": True,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True
                
            },
            {
                "name": "associateOID",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : False
                
            },

    
            {
                "name": "EmployeeName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False
            },
            # {
            #     "name": "Working_Hours_Per_Day",
            #     "type": "Edm.String",
            #     "key": False,
            #     "filterable": True,
            #     "searchable": True,
            #     "retrievable": False,
            #     "sortable" : True
            # },
            {
                "name": "WorkerID",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False
            },
            {
                "name": "EmploymentStatus",
                "type": "Edm.String",
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False
            },
            {
                "name": 'EmployeeDesignationStartDate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False
            },
            {
                "name": 'EmployeeDesignationCode',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False
            },
            {
                "name": 'EmployeeDesignation',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False
            },
            {
                "name": 'EmployeeManagementsPositionIndicator',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False

            },
            {
                "name": 'CertificationCode',
                "type": "Edm.String",
                "key": False,
                "searchable": True,
                "retrievable": False,
                "sortable" : False,
                "filterable": True
            },

           {
                "name": 'NameOfCertification',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False,
            },
            
            {
                "name": 'EmployeeCode',
                "type": "Edm.String",
                "key": False,
                "searchable": True,
                "filterable": True,
                "retrievable": False,
                "sortable": True
            },
            {
                "name": 'projectType',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            {
                "name": 'projectArea',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            {
                "name": 'regularHours',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
            {
                "name": 'overtimeHours',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
            {
                "name": 'totalHours',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : False,
            },
            {
                "name": 'maximumRegularPayRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "filterable": True,
                "retrievable": False,
                "sortable" : True,
            },
            
       
            {
                "name": 'minimumRegularPayRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
            {
                "name": 'averageRegularPayRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
            {
                "name": 'maximumOvertimePayRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },

             {
                "name": 'minimumOvertimePayRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
             {
                "name": 'averageOvertimePayRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,

            },
             {
                "name": 'averageProjectFee',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },

             
             {
                "name": 'activeProjectCount',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
             {
                "name": 'inactiveProjectCount',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
             {
                "name": 'dormantProjectCount',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
            {
                "name": 'DirectProjectCounts',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },

            {
                "name": 'proposalWonProjectCount',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
             {
                "name": 'currentRegularPay',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
            {
                "name": 'companyId',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : True,
            },
             {
                "name": 'billingType',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False,
            },
            
             {
                "name": 'hoursPerDay',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
            {
                "name": 'provisionalBillingRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
            {
                "name": 'ExperienceInOtherFirm',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
            
       
            {
                "name": 'practiceGroupCode',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : True,
            },
            {
                "name": 'profitAndLossCentre',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False,
            },
            {
                "name": 'officeDepartment',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            {
                "name": 'overtimeAllowed',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False,
            },
            {
                "name": 'customerBillingType',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False,
            },
            
            {
                "name": 'companyName',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False,
            },
              {
                "name": 'maximumHourlyEmployeeRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": False,
                "sortable" : True,
            },
              {
                "name": 'payrollEnabledFlag',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False,
            },
              {
                "name": 'payrollFrequency',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": False,
                "sortable" : False,
            },
            
        ],
       

        "semantic": {
            "configurations": [
                {
                    "name": "azureml-default",
                    "prioritizedFields": {
                        "titleField": {"fieldName": "associateOID"},
                        "prioritizedContentFields": [{'fieldName': 'EmployeeName'}, 
                                                    {'fieldName': 'WorkerID'},
                                                    {'fieldName': 'EmploymentStatus'},
                                                    {'fieldName': 'EmployeeDesignationStartDate'},
                                                    {'fieldName': 'EmployeeDesignationCode'},
                                                    {'fieldName': 'EmployeeDesignation'},
                                                    {'fieldName': 'EmployeeManagementsPositionIndicator'},
                                                    {'fieldName': 'CertificationCode'},
                                                    {'fieldName': 'NameOfCertification'},
                                                    {'fieldName': 'EmployeeCode'},
                                                    {'fieldName': 'projectType'},
                                                    {'fieldName': 'projectArea'},
                                                    # {'fieldName': 'regularHours'},
                                                    # {'fieldName': 'overtimeHours'},
                                                    # {'fieldName': 'totalHours'},
                                                    # {'fieldName': 'maximumRegularPayRate'},                                                    {'fieldName': 'minimumRegularPayRate'},
                                                    # {'fieldName': 'averageRegularPayRate'},
                                                    # {'fieldName': 'maximumOvertimePayRate'},
                                                    # {'fieldName': 'minimumOvertimePayRate'},
                                                    # {'fieldName': 'averageOvertimePayRate'},
                                                    # {'fieldName': 'averageProjectFee'},
                                                    # {'fieldName': 'activeProjectCount'},
                                                    # {'fieldName': 'inactiveProjectCount'},
                                                    # {'fieldName': 'dormantProjectCount'},
                                                    # {'fieldName': 'DirectProjectCounts'},
                                                    # {'fieldName': 'proposalWonProjectCount'},
                                                    # {'fieldName': 'currentRegularPay'},
                                                    {'fieldName': 'companyId'},
                                                    {'fieldName': 'billingType'},
                                                    # {'fieldName': 'hoursPerDay'},
                                                    # {'fieldName': 'provisionalBillingRate'},
                                                    # {'fieldName': 'ExperienceInOtherFirm'},
                                                    {'fieldName': 'practiceGroupCode'},
                                                    {'fieldName': 'profitAndLossCentre'},
                                                    {'fieldName': 'officeDepartment'},
                                                    {'fieldName': 'overtimeAllowed'},
                                                    {'fieldName': 'customerBillingType'},
                                                    {'fieldName': 'companyName'},
                                                    # {'fieldName': 'maximumHourlyEmployeeRate'},
                                                    {'fieldName': 'payrollEnabledFlag'},
                                                    {'fieldName': 'payrollFrequency'},               
                        ],
                        # "prioritizedKeywordsFields": [
                        #     {"fieldName": "Environmental_keyword"},
                        #     {"fieldName": "Engineering_keyword"},
                        #     {"fieldName": "Water_keyword"},
                        #     {"fieldName": "Project_and_Management_keyword"},
                        #     {"fieldName": "O_and_G_keyword"}
                        # ]
                    }
                }
            ]
    }
    }
)
headers = {"Content-Type": "application/json", "api-key": search_api_key}

response = requests.request("PUT", url, headers=headers, data=payload)
if response.status_code == 201:
    print("Index created!")
elif response.status_code == 204:
    print("Index updated!")
else:
    print(f"HTTP request failed with status code {response.status_code}")
    print(f"HTTP response body: {response.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
import json
import requests
import pandas as pd



def insert_into_index(documents):
    """Uploads a list of 'documents' to Azure AI Search index."""
    url = f"https://{search_service_name}.search.windows.net/indexes/{index_name}/docs/index?api-version=2023-11-01"
    payload = json.dumps({"value": documents})
    headers = {
        "Content-Type": "application/json",
        "api-key": search_api_key,
    }
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200 or response.status_code == 201:
        return "Success"
    else:
        return f"Failure: {response.text}"

def make_safe_id(row_id: str):
    """Strips disallowed characters from row id for use as Azure AI search document ID."""
    return re.sub("[^0-9a-zA-Z_-]", "_", row_id)

def upload_rows(rows):
    """Uploads the rows in a dataframe to Azure AI Search.
    Limits uploads to 1000 rows at a time due to Azure AI Search API limits.
    """
    BATCH_SIZE = 1000
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:

            documents.append(
                {
                    "id": make_safe_id(row["row_index"]),
                    "associateOID": row["associateOID"],
                    "EmployeeName": row["EmployeeName"],
                    "WorkerID": row["WorkerID"],
                    "EmploymentStatus": row["EmploymentStatus"],
                    "EmployeeDesignationStartDate": row["EmployeeDesignationStartDate"],
                    "EmployeeDesignationCode": row["EmployeeDesignationCode"],
                    "EmployeeDesignation": row["EmployeeDesignation"],
                    "EmployeeManagementsPositionIndicator": row["EmployeeManagementsPositionIndicator"],
                    "CertificationCode": row["CertificationCode"],
                    "NameOfCertification": row["NameOfCertification"],
                    "EmployeeCode": row["EmployeeCode"],
                    # "EmployeeID": row["EmployeeID"],
                    "projectType": row["projectType"],
                    "projectArea": row["projectArea"],
                    "regularHours": row["regularHours"],
                    "overtimeHours": row["overtimeHours"],
                    "totalHours": row["totalHours"],
                    "maximumRegularPayRate": row["maximumRegularPayRate"],
                    "minimumRegularPayRate": row["minimumRegularPayRate"],
                    "averageRegularPayRate": row["averageRegularPayRate"],
                    "maximumOvertimePayRate": row["maximumOvertimePayRate"],
                    "minimumOvertimePayRate": row["minimumOvertimePayRate"],
                    "averageOvertimePayRate": row["averageOvertimePayRate"],
                    "averageProjectFee": row["averageProjectFee"],
                    "activeProjectCount": row["activeProjectCount"],
                    "inactiveProjectCount": row["inactiveProjectCount"],
                    "dormantProjectCount": row["dormantProjectCount"],
                    "DirectProjectCounts": row["DirectProjectCounts"],
                    "proposalWonProjectCount": row["proposalWonProjectCount"],
                    "currentRegularPay": row["currentRegularPay"],
                    "companyId": row["companyId"],
                    "billingType": row["billingType"],
                    "hoursPerDay": row["hoursPerDay"],
                    "provisionalBillingRate": row["provisionalBillingRate"],
                    "ExperienceInOtherFirm": row["ExperienceInOtherFirm"],
                    "practiceGroupCode": row["practiceGroupCode"],
                    "profitAndLossCentre": row["profitAndLossCentre"],
                    "officeDepartment": row["officeDepartment"],
                    "overtimeAllowed": row["overtimeAllowed"],
                    "customerBillingType": row["customerBillingType"],
                    "companyName": row["companyName"],
                    "maximumHourlyEmployeeRate": row["maximumHourlyEmployeeRate"],
                    "payrollEnabledFlag": row["payrollEnabledFlag"],
                    "payrollFrequency": row["payrollFrequency"],
                    "@search.action": "upload",
                }
            )
        status = insert_into_index(documents)
        print([row_batch[0]["row_index"], row_batch[-1]["row_index"], status])
        yield [row_batch[0]["row_index"], row_batch[-1]["row_index"], status]


df['row_index'] = range(len(df))
df['row_index'] = df['row_index'].astype(str)
# print(df)
# Convert DataFrame rows to list of dicts
rows = df.to_dict(orient='records')
# print(type(rows))

# Run upload_batch on partitions of the dataframe
print(list(upload_rows(rows)))
results = list(upload_rows(rows))

# Convert results to DataFrame for display
res_df = pd.DataFrame(results, columns=["start_index", "end_index", "insertion_status"])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df["regularHours"].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Index V3

# CELL ********************

def string_conversion(df, column_list):
    for col_name in column_list:
        df[col_name] = df[col_name].astype(str)

    return df

df1 = string_conversion(df, df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

# Length of the embedding vector (OpenAI ada-002 generates embeddings of length 1536)
EMBEDDING_LENGTH = 1536

search_service_name = "vpc-dev-aisearch"
index_name = "vpc-new-adp-deltek-index-v3"

# Create index for AI Search with fields id, content, and contentVector
# Note the datatypes for each field below
url = f"https://{search_service_name}.search.windows.net/indexes/{index_name}?api-version=2023-11-01"
payload = json.dumps(
    {
        "name": index_name,
        "fields": [
            # Unique identifier for each document

            {
                "name": "id",
                "type": "Edm.String",
                "key": True,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True
                
            },
            {
                "name": "associateOID",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False
                
            },

    
            {
                "name": "EmployeeName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False
            },
            # {
            #     "name": "Working_Hours_Per_Day",
            #     "type": "Edm.String",
            #     "key": False,
            #     "filterable": True,
            #     "searchable": True,
            #     "retrievable": False,
            #     "sortable" : True
            # },
            {
                "name": "WorkerID",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False
            },
            {
                "name": "EmploymentStatus",
                "type": "Edm.String",
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False
            },
            {
                "name": 'EmployeeDesignationStartDate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False
            },
            {
                "name": 'EmployeeDesignationCode',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False
            },
            {
                "name": 'EmployeeDesignation',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False
            },
            {
                "name": 'EmployeeManagementsPositionIndicator',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False

            },
            {
                "name": 'CertificationCode',
                "type": "Edm.String",
                "key": False,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
                "filterable": True
            },

           {
                "name": 'NameOfCertification',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            
            {
                "name": 'EmployeeCode',
                "type": "Edm.String",
                "key": False,
                "searchable": True,
                "filterable": True,
                "retrievable": True,
                "sortable": True
            },
            {
                "name": 'projectType',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            {
                "name": 'projectArea',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            {
                "name": 'regularHours',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            {
                "name": 'overtimeHours',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            {
                "name": 'totalHours',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            {
                "name": 'maximumRegularPayRate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            
       
            {
                "name": 'minimumRegularPayRate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            {
                "name": 'averageRegularPayRate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            {
                "name": 'maximumOvertimePayRate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },

             {
                "name": 'minimumOvertimePayRate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
             {
                "name": 'averageOvertimePayRate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,

            },
             {
                "name": 'averageProjectFee',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },

             
             {
                "name": 'activeProjectCount',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
             {
                "name": 'inactiveProjectCount',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
             {
                "name": 'dormantProjectCount',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True, 
                "sortable" : True,
            },
            {
                "name": 'DirectProjectCounts',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },

            {
                "name": 'proposalWonProjectCount',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
             {
                "name": 'currentRegularPay',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            {
                "name": 'companyId',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
             {
                "name": 'billingType',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            
             {
                "name": 'hoursPerDay',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            {
                "name": 'provisionalBillingRate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            {
                "name": 'ExperienceInOtherFirm',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            
       
            {
                "name": 'practiceGroupCode',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
            {
                "name": 'profitAndLossCentre',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            {
                "name": 'officeDepartment',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            {
                "name": 'overtimeAllowed',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            {
                "name": 'customerBillingType',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            
            {
                "name": 'companyName',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
              {
                "name": 'maximumHourlyEmployeeRate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
            },
              {
                "name": 'payrollEnabledFlag',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
              {
                "name": 'payrollFrequency',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
            },
            
        ],
       

        "semantic": {
            "configurations": [
                {
                    "name": "azureml-default",
                    "prioritizedFields": {
                        "titleField": {"fieldName": "associateOID"},
                        "prioritizedContentFields": [{'fieldName': 'EmployeeName'}, 
                                                    {'fieldName': 'WorkerID'},
                                                    {'fieldName': 'EmploymentStatus'},
                                                    {'fieldName': 'EmployeeDesignationStartDate'},
                                                    {'fieldName': 'EmployeeDesignationCode'},
                                                    {'fieldName': 'EmployeeDesignation'},
                                                    {'fieldName': 'EmployeeManagementsPositionIndicator'},
                                                    {'fieldName': 'CertificationCode'},
                                                    {'fieldName': 'NameOfCertification'},
                                                    {'fieldName': 'EmployeeCode'},
                                                    {'fieldName': 'projectType'},
                                                    {'fieldName': 'projectArea'},
                                                    {'fieldName': 'regularHours'},
                                                    {'fieldName': 'overtimeHours'},
                                                    {'fieldName': 'totalHours'},
                                                    {'fieldName': 'maximumRegularPayRate'},                                                    {'fieldName': 'minimumRegularPayRate'},
                                                    {'fieldName': 'averageRegularPayRate'},
                                                    {'fieldName': 'maximumOvertimePayRate'},
                                                    {'fieldName': 'minimumOvertimePayRate'},
                                                    {'fieldName': 'averageOvertimePayRate'},
                                                    {'fieldName': 'averageProjectFee'},
                                                    {'fieldName': 'activeProjectCount'},
                                                    {'fieldName': 'inactiveProjectCount'},
                                                    {'fieldName': 'dormantProjectCount'},
                                                    {'fieldName': 'DirectProjectCounts'},
                                                    {'fieldName': 'proposalWonProjectCount'},
                                                    {'fieldName': 'currentRegularPay'},
                                                    {'fieldName': 'companyId'},
                                                    {'fieldName': 'billingType'},
                                                    {'fieldName': 'hoursPerDay'},
                                                    {'fieldName': 'provisionalBillingRate'},
                                                    {'fieldName': 'ExperienceInOtherFirm'},
                                                    {'fieldName': 'practiceGroupCode'},
                                                    {'fieldName': 'profitAndLossCentre'},
                                                    {'fieldName': 'officeDepartment'},
                                                    {'fieldName': 'overtimeAllowed'},
                                                    {'fieldName': 'customerBillingType'},
                                                    {'fieldName': 'companyName'},
                                                    {'fieldName': 'maximumHourlyEmployeeRate'},
                                                    {'fieldName': 'payrollEnabledFlag'},
                                                    {'fieldName': 'payrollFrequency'},               
                        ],
                        # "prioritizedKeywordsFields": [
                        #     {"fieldName": "Environmental_keyword"},
                        #     {"fieldName": "Engineering_keyword"},
                        #     {"fieldName": "Water_keyword"},
                        #     {"fieldName": "Project_and_Management_keyword"},
                        #     {"fieldName": "O_and_G_keyword"}
                        # ]
                    }
                }
            ]
    }
    }
)
headers = {"Content-Type": "application/json", "api-key": search_api_key}

response = requests.request("PUT", url, headers=headers, data=payload)
if response.status_code == 201:
    print("Index created!")
elif response.status_code == 204:
    print("Index updated!")
else:
    print(f"HTTP request failed with status code {response.status_code}")
    print(f"HTTP response body: {response.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
import json
import requests
import pandas as pd



def insert_into_index(documents):
    """Uploads a list of 'documents' to Azure AI Search index."""
    url = f"https://{search_service_name}.search.windows.net/indexes/{index_name}/docs/index?api-version=2023-11-01"
    payload = json.dumps({"value": documents})
    headers = {
        "Content-Type": "application/json",
        "api-key": search_api_key,
    }
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200 or response.status_code == 201:
        return "Success"
    else:
        return f"Failure: {response.text}"

def make_safe_id(row_id: str):
    """Strips disallowed characters from row id for use as Azure AI search document ID."""
    return re.sub("[^0-9a-zA-Z_-]", "_", row_id)

def upload_rows(rows):
    """Uploads the rows in a dataframe to Azure AI Search.
    Limits uploads to 1000 rows at a time due to Azure AI Search API limits.
    """
    BATCH_SIZE = 1000
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:

            documents.append(
                {
                    "id": make_safe_id(row["row_index"]),
                    "associateOID": row["associateOID"],
                    "EmployeeName": row["EmployeeName"],
                    "WorkerID": row["WorkerID"],
                    "EmploymentStatus": row["EmploymentStatus"],
                    "EmployeeDesignationStartDate": row["EmployeeDesignationStartDate"],
                    "EmployeeDesignationCode": row["EmployeeDesignationCode"],
                    "EmployeeDesignation": row["EmployeeDesignation"],
                    "EmployeeManagementsPositionIndicator": row["EmployeeManagementsPositionIndicator"],
                    "CertificationCode": row["CertificationCode"],
                    "NameOfCertification": row["NameOfCertification"],
                    "EmployeeCode": row["EmployeeCode"],
                    # "EmployeeID": row["EmployeeID"],
                    "projectType": row["projectType"],
                    "projectArea": row["projectArea"],
                    "regularHours": row["regularHours"],
                    "overtimeHours": row["overtimeHours"],
                    "totalHours": row["totalHours"],
                    "maximumRegularPayRate": row["maximumRegularPayRate"],
                    "minimumRegularPayRate": row["minimumRegularPayRate"],
                    "averageRegularPayRate": row["averageRegularPayRate"],
                    "maximumOvertimePayRate": row["maximumOvertimePayRate"],
                    "minimumOvertimePayRate": row["minimumOvertimePayRate"],
                    "averageOvertimePayRate": row["averageOvertimePayRate"],
                    "averageProjectFee": row["averageProjectFee"],
                    "activeProjectCount": row["activeProjectCount"],
                    "inactiveProjectCount": row["inactiveProjectCount"],
                    "dormantProjectCount": row["dormantProjectCount"],
                    "DirectProjectCounts": row["DirectProjectCounts"],
                    "proposalWonProjectCount": row["proposalWonProjectCount"],
                    "currentRegularPay": row["currentRegularPay"],
                    "companyId": row["companyId"],
                    "billingType": row["billingType"],
                    "hoursPerDay": row["hoursPerDay"],
                    "provisionalBillingRate": row["provisionalBillingRate"],
                    "ExperienceInOtherFirm": row["ExperienceInOtherFirm"],
                    "practiceGroupCode": row["practiceGroupCode"],
                    "profitAndLossCentre": row["profitAndLossCentre"],
                    "officeDepartment": row["officeDepartment"],
                    "overtimeAllowed": row["overtimeAllowed"],
                    "customerBillingType": row["customerBillingType"],
                    "companyName": row["companyName"],
                    "maximumHourlyEmployeeRate": row["maximumHourlyEmployeeRate"],
                    "payrollEnabledFlag": row["payrollEnabledFlag"],
                    "payrollFrequency": row["payrollFrequency"],
                    "@search.action": "upload",
                }
            )
        status = insert_into_index(documents)
        print([row_batch[0]["row_index"], row_batch[-1]["row_index"], status])
        yield [row_batch[0]["row_index"], row_batch[-1]["row_index"], status]


df['row_index'] = range(len(df))
df['row_index'] = df['row_index'].astype(str)
# print(df)
# Convert DataFrame rows to list of dicts
rows = df.to_dict(orient='records')
# print(type(rows))

# Run upload_batch on partitions of the dataframe
print(list(upload_rows(rows)))
results = list(upload_rows(rows))

# Convert results to DataFrame for display
res_df = pd.DataFrame(results, columns=["start_index", "end_index", "insertion_status"])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizableTextQuery
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
import pandas as pd
import json


# from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
import os
from config import (
    azure_search_endpoint,
    credential,
    azure_openai_endpoint,
    azure_openai_key,
    azure_openai_chat_deployment,
    azure_openai_chat_deployment,
    azure_openai_api_version,
    embedding_model_name,
    openai_credential,
    search_api_key,

   
)


token_provider = get_bearer_token_provider(openai_credential, "https://cognitiveservices.azure.com/.default")
index_name = "vpc-new-adp-deltek-index-v2"

client = AzureOpenAI(
    api_version=azure_openai_api_version,
    azure_endpoint=azure_openai_endpoint,
    api_key=azure_openai_key,
    azure_ad_token_provider=token_provider if not azure_openai_key else None
)

# See https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/function-calling for more information
# NOTE: Updating the tool definition with specific examples related to your data will help improve the accuracy.
tools = [
    {
        "type": "function",
        "function": {
            "name": "get_query_options",
            "description": "Given a question, get any additional Azure Search query parameters required to answer the question. If no additional query parameters are required to answer the question, don't return any.",
            "parameters": {
                "type": "object",
                "properties": {
                    "orderBy": {
                        "type": "string",
                        "description": "Specify a custom sort order for search results. Format is a comma-separated list of up to 32 order-by clauses. If a direction is not specified, the default is ascending. Example: ID, Age desc, Title asc",
                    },
                    "filter": {
                        "type": "string",
                        "description": "Specify inclusion or exclusion criteria for search results. Format is an Azure Search OData boolean expression. Example: Age le 4 or not (Age gt 8)"
                    },
                    "search": {
                        "type": "string",
                        "description": "Specify a query string used to search text and vectors in an Azure Search index in order to answer the provided question. If no query string is required to answer the question, return * or no query string at all"
                    }
                }
            },
        }
    }
]

# Specifically instruct the model to only use filterable fields when creating query options
filterable_fields = ", ".join([field.name for field in fields if field.filterable])
query_options_system_prompt = f"Create options for Azure Search queries. If you are creating filters, you may only use the following fields: {filterable_fields}."
def get_query_options(query: str) -> dict:
    response = client.chat.completions.create(
        model=azure_openai_chat_deployment,
        messages=[
            {"role": "system", "content": query_options_system_prompt},
            {"role": "user", "content": query}
        ],
        tools=tools,
        tool_choice={ "type": "function", "function": { "name": "get_query_options" } },
    )
    response_message = response.choices[0].message

    # Only include query options if the model provides them
    if len(response_message.tool_calls) == 1:
        try:
            return json.loads(response_message.tool_calls[0].function.arguments)
        except:
            return {}

    return {}


answer_query_results_system_prompt = f"The following question requires search results to provide an answer. Use the provided search results to answer the question. If you can't answer the question using the search results, say I don't know."
search_client = SearchClient(endpoint, index_name, credential=credential)
def answer_query(query: str) -> str:
    # Parse the query options returned by the model
    query_options = get_query_options(query)
    query_option_search = query_options.get("search")
    vector_queries = None
    # if query_option_search and query_option_search != "*":
    #     vector_queries = [VectorizableTextQuery(text=query_option_search, k_nearest_neighbors=50, fields="TitleVector,DescriptionVector")]

    query_option_order_by = query_options.get("orderBy")
    order_by = None
    if query_option_order_by:
        try:
            order_by = query_option_order_by.split(",")
        except:
            order_by = None

    # This sample only uses specific fields to answer questions. Update these fields for your own data
    columns = ['associateOID', 'EmployeeName', 'WorkerID', 'EmploymentStatus',
                'EmployeeDesignationStartDate', 'EmployeeDesignationCode',
                'EmployeeDesignation', 'EmployeeManagementsPositionIndicator',
                'CertificationCode', 'NameOfCertification', 'EmployeeCode',
                'employeeId', 'projectType', 'projectArea', 'regularHours',
                'overtimeHours', 'totalHours', 'maximumRegularPayRate',
                'minimumRegularPayRate', 'averageRegularPayRate',
                'maximumOvertimePayRate', 'minimumOvertimePayRate',
                'averageOvertimePayRate', 'averageProjectFee', 'activeProjectCount',
                'inactiveProjectCount', 'dormantProjectCount', 'adhocProjectCount',
                'proposalWonProjectCount', 'currentRegularPay', 'companyId',
                'billingType', 'hoursPerDay', 'provisionalBillingRate',
                'otherFirmsExpYears', 'practiceGroupCode', 'profitAndLossCentre',
                'officeDepartment', 'overtimeAllowed', 'customerBillingType',
                'companyName', 'maximumHourlyEmployeeRate', 'payrollEnabledFlag',
                'payrollFrequency']


    search_results = search_client.search(
        search_text=query_option_search,
        vector_queries=vector_queries,
        top=5,
        order_by=order_by,
        filter=query_options.get("filter"),
        select=columns
    )

    # Convert the search results to markdown for use by the model
    results = [ { column: result[column] for column in columns } for result in search_results ]
    results_markdown_table = pd.DataFrame(results).to_markdown(index=False)

    response = client.chat.completions.create(
        model=azure_openai_chat_deployment,
        messages=[
            {"role": "system", "content": answer_query_results_system_prompt},
            {"role": "user", "content": results_markdown_table },
            {"role": "user", "content": query}
        ]
    )
    # Return the generated answer, query options, and results table for analysis
    return response.choices[0].message.content, query_options, results_markdown_table

def print_answer(answer, query_options, results):
    print("Generated Answer:", answer)
    print("Generated Query Options:", query_options)
    print("Search Results")
    print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install azure.search.documents

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
