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

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables


lakehouse_path = "Tables"
table_paths = {
    "deltek": f"{lakehouse_path}/br_deltek_DemoData_v3"
}
# Read tables into DataFrames
deltek = spark.read.format("delta").load(table_paths["deltek"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#convertion to the pandas dataframe
deltek = deltek.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#sample data
# deltek_sample = deltek.sample(5,random_state=42)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltek.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltek.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
pd.set_option('display.max_columns', 500)
display(deltek.sample(5,random_state=42))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltek.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltek['BillingCategory'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import pandas as pd

# Define a function to process the dataframe to json format
def excel_to_json(df):
    # Ensure numeric fields are actually numeric
    numeric_fields = ['ProjectManagerID','Budget', 'RevenueToDate', 'BilledToDate', 'UnbilledToDate', 
                      'BacklogToDate', 'TotalRevenueBudget', 'EmployeeJobCostRate', 
                      'EmployeeUtilization', 'TargetUtilization']
    
    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')

    clients = {}

    for _, row in df.iterrows():
        client_id = row['ClientID']

        # If the client is not already in the dictionary, add it
        if client_id not in clients:
            clients[client_id] = {
                'ClientID': row['ClientID'],
                'ClientName': row['ClientName'],
                'Worked': row['Worked'],
                'ClientAddressCity': row['ClientAddressCity'],
                'ClientAddressState': row['ClientAddressState'],
                'Projects': []
            }

        # Extract project information
        project_number = row['ProjectID']
        existing_project = None

        # Check if the project already exists
        for project in clients[client_id]['Projects']:
            if project['ProjectID'] == project_number:
                existing_project = project
                break

        # If the project does not exist, create a new one
        if not existing_project:
            project = {
                'ProjectID': row['ProjectID'],
                'ProjectName': row['ProjectName'],
                'ProjectStatus': row['ProjectStatus'],
                'ProjectAddressStreet': row['ProjectAddressStreet'],
                'ProjectAddressCity': row['ProjectAddressCity'],
                'ProjectAddressState': row['ProjectAddressState'],
                'ProjectManager': row['ProjectManager'],
                'ProjectManagerID': row['ProjectManagerID'],
                'ProjectBudget': row['Budget'],
                'ProjectRevenueToDate': row['RevenueToDate'],
                'ProjectBilledToDate': row['BilledToDate'],
                'ProjectUnbilledToDate': row['UnbilledToDate'],
                'ProjectBacklogToDate': row['BacklogToDate'],
                'ProjectTotalRevenueBudget': row['TotalRevenueBudget'],
                'ProjectArea': row['ProjectArea'],
                'ProjectType': row['ProjectType'],
                'ProjectPracticeServiceGroup': row['ProjectPracticeServiceGroup'],
                'ProjectPracticeGroup': row['ProjectPracticeGroup'],
                'ProjectServiceGroup': row['ProjectServiceGroup'],
                'Employees': []
            }
            clients[client_id]['Projects'].append(project)
            existing_project = project

        # Add employee information if available
        if pd.notna(row['EmployeeID']):
            employee = {
                'EmployeeID': row['EmployeeID'],
                'EmployeeName': row['EmployeeName'],
                'EmployeePosition': row['Title'],
                'EmployeePracticeGroupADP': row['PracticeGroupADP'],
                'EmployeePracticeGroupDeltek': row['PracticeGroupDeltek'],
                'EmployeeBusinessUnit': row['BusinessUnit'],
                'EmployeeJobCostRate': row['EmployeeJobCostRate'],
                'EmployeeUtilization': row['EmployeeUtilization'],
                'EmployeeTargetUtilization': row['TargetUtilization'],
                'EmployeeNameOfCertification': row['NameOfCertification'],
                'EmployeeCertificationCode': row['CertificationCode'],
            }
            existing_project['Employees'].append(employee)

    clients_lst = [client for client in clients.values()]

    return json.dumps(clients_lst, indent=4)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# creating a document from dataframe
document = excel_to_json(deltek_sample)
doc = json.loads(document)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Index creation

# CELL ********************


# from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
import os

import requests
import json

# load_dotenv(override=True) # take environment variables from .env.

# The following variables from your .env file are used in this notebook

# from config import (
# credential,
# azure_openai_endpoint,
# azure_openai_key,
# azure_openai_embedding_deployment,
# embedding_model_name,
# azure_openai_api_version,
# search_api_key)



search_api_key = "8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u"

search_service_name = "vpc-dev-aisearch"
index_name = "adp-deltek-hierarchical-fabric-v1"
endpoint = f"https://{search_service_name}.search.windows.net"

EMBEDDING_LENGTH = 1536

# Create index for AI Search with fields id, content, and contentVector
# Note the datatypes for each field below
url = f"https://{search_service_name}.search.windows.net/indexes/{index_name}?api-version=2023-11-01"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payload = json.dumps(
    {
        "name": index_name,
        "fields": [
            # Unique identifier for each document
            {
                "name": "ClientID",
                "type": "Edm.String",
                "key": True,
                "filterable": True,
                "searchable": True, 
                "retrievable": True,               
                "sortable": True,
                "facetable": True                
            },
            # {
            #     "name": "ClientFolderID",
            #     "type": "Edm.String",
            #     "key": False,
            #     "filterable": True,
            #     "searchable": True,
            #     "retrievable": True,
            #     "sortable": True,
            #     "facetable": True               
            # },            
            {
                "name": "ClientName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True
            },
            {
                "name": "Worked",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True 

            },            
            {
                "name": "ClientAddressCity",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True
            },
            {
                "name": "ClientAddressState",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True 

            }, 
            {
                "name": "Projects",
                "type": "Collection(Edm.ComplexType)",
                "fields": [
                        {
                        "name": "ProjectID",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 

                        },
                        {
                            "name": "ProjectName",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        # {
                        #     "name": "LaborBudget",
                        #     "type": "Edm.Double",
                        #     "key": False,
                        #     "filterable": True,
                        #     "searchable": False,
                        #     "retrievable": True,
                        #     "sortable": False,
                        #     "facetable": False 

                        # },
                        # {
                        #     "name": "LaborRemaining",
                        #     "type": "Edm.Double",
                        #     "key": False,
                        #     "filterable": True,
                        #     "searchable": False,
                        #     "retrievable": True,
                        #     "sortable": False,
                        #     "facetable": False 

                        # },
                        {
                            "name": "ProjectStatus",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectAddressStreet",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectAddressCity",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectAddressState",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        # {
                        #     "name": "ProjectType",
                        #     "type": "Edm.String",
                        #     "key": False,
                        #     "filterable": True,
                        #     "searchable": True,
                        #     "retrievable": True,
                        #     "sortable": False,
                        #     "facetable": True 

                        # },
                        {
                            "name": "ProjectManager",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectManagerID",
                            "type": "Edm.Int64",
                            "key": False,
                            "filterable": True,
                            "searchable": False,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectBudget",
                            "type": "Edm.Int64",
                            "key": False,
                            "filterable": True,
                            "searchable": False,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectRevenueToDate",
                            "type": "Edm.Int64",
                            "key": False,
                            "filterable": True,
                            "searchable": False,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectBilledToDate",
                            "type": "Edm.Int64",
                            "key": False,
                            "filterable": True,
                            "searchable": False,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                         {
                            "name": "ProjectUnbilledToDate",
                            "type": "Edm.Double",
                            "key": False,
                            "filterable": True,
                            "searchable": False,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                         {
                            "name": "ProjectBacklogToDate",
                            "type": "Edm.Int64",
                            "key": False,
                            "filterable": True,
                            "searchable": False,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectTotalRevenueBudget",
                            "type": "Edm.Double",
                            "key": False,
                            "filterable": True,
                            "searchable": False,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },

                        {
                            "name": "ProjectArea",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectType",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectPracticeServiceGroup",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                            "name": "ProjectPracticeGroup",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },



                        {
                            "name": "ProjectServiceGroup",
                            "type": "Edm.String",
                            "key": False,
                            "filterable": True,
                            "searchable": True,
                            "retrievable": True,
                            "sortable": False,
                            "facetable": True 

                        },
                        {
                        "name": "Employees",
                        "type": "Collection(Edm.ComplexType)",
                        "fields": [
                                {
                                "name": "EmployeeID",
                                "type": "Edm.String",
                                "key": False,
                                "filterable": True,
                                "searchable": True,
                                "retrievable": True,
                                "sortable": False,
                                "facetable": True 
                                },
                                {
                                    "name": "EmployeeName",
                                    "type": "Edm.String",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": True,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                },
                                {
                                    "name": "EmployeePosition",
                                    "type": "Edm.String",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": True,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                },
                                {
                                    "name": "EmployeePracticeGroupADP",
                                    "type": "Edm.String",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": True,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                },
                                {
                                    "name": "EmployeePracticeGroupDeltek",
                                    "type": "Edm.String",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": True,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                },
                                {
                                    "name": "EmployeeBusinessUnit",
                                    "type": "Edm.String",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": True,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                },
                                {
                                    "name": "EmployeeJobCostRate",
                                    "type": "Edm.Double",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": False,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                },
                                {
                                    "name": "EmployeeUtilization",
                                    "type": "Edm.Double",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": False,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                },
                                {
                                    "name": "EmployeeTargetUtilization",
                                    "type": "Edm.Int64",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": False,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                },
                                {
                                    "name": "EmployeeNameOfCertification",
                                    "type": "Edm.String",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": True,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                },
                                {
                                    "name": "EmployeeCertificationCode",
                                    "type": "Edm.String",
                                    "key": False,
                                    "filterable": True,
                                    "searchable": True,
                                    "retrievable": True,
                                    "sortable": False,
                                    "facetable": True 

                                }
                                
                              ]}
                            ]}

        ],
        "vectorSearch": {
            "algorithms": [{"name": "hnswConfig", "kind": "hnsw", "hnswParameters": {"metric": "cosine"}}],
            "profiles": [{"name": "vectorConfig", "algorithm": "hnswConfig"}],
        },
        "semantic": {
            "configurations": [
                {
                    "name": "azure-ml-default",
                    "prioritizedFields": {
                        "titleField": {"fieldName": "ClientID"},                
                        "prioritizedContentFields": [],
                        "prioritizedKeywordsFields": [
                        {"fieldName": "ClientID"},
                        # {"fieldName": "ClientFolderID"},
                        {"fieldName": "ClientName"},
                        # {"fieldName": "Worked"},
                        {"fieldName": "ClientAddressCity"},
                        {"fieldName": "ClientAddressState"},
                        # {"fieldName": 'ProjectID'},                        
                        # {"fieldName": "ProjectName"},
                        # {"fieldName": "ProjectStatus"},
                        # {"fieldName": "ProjectAddressStreet"},
                        # {"fieldName": "ProjectAddressCity"},
                        # {"fieldName": "ProjectManager"},
                        # {"fieldName": "ProjectManagerID"},
                        # {"fieldName": "ProjectBudget"},
                        # {"fieldName": "ProjectRevenueToDate"},
                        # {"fieldName": "ProjectBilledToDate"},
                        # {"fieldName": "ProjectUnbilledToDate"},
                        # {"fieldName": "ProjectBacklogToDate"},
                        # {"fieldName": "ProjectTotalRevenueBudget"},
                        # {"fieldName": "ProjectArea"},
                        # {"fieldName": "ProjectType"},
                        # {"fieldName": "ProjectPracticeServiceGroup"},
                        # {"fieldName": "ProjectPracticeGroup"},
                        # {"fieldName": "ProjectServiceGroup"},
                        # {"fieldName": "EmployeeID"},
                        # {"fieldName": "EmployeeName"},
                        # {"fieldName": "EmployeePosition"},
                        # {"fieldName": "EmployeePracticeGroupADP"},
                        # {"fieldName": "EmployeePracticeGroupDeltek"},
                        # {"fieldName": "EmployeeBusinessUnit"},
                        # {"fieldName": "EmployeeJobCostRate"},
                        # {"fieldName": "EmployeeUtilization"},
                        # {"fieldName": "EmployeeTargetUtilization"},
                        # {"fieldName": "EmployeeNameOfCertification"},
                        # {"fieldName": "EmployeeCertificationCode"}


                        ]

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


url = f"https://{search_service_name}.search.windows.net/indexes/{index_name}/docs/index?api-version=2023-11-01"
payload = json.dumps({"value": doc})
headers = {
    "Content-Type": "application/json",
    "api-key": search_api_key,
}
response = requests.post(url, headers=headers, data=payload)
response.text


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
