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
import pandas as pd
import numpy as np
df = deltek.toPandas()
print("Converted to Pandas DataFrame:")


rename_dict = {'ClientID':'ClientId',
 'ClientFolderID':'ClientFolderId',
 'ClientName': 'ClientName',
 'ProjectID':'ProjectId',
 'Budget':'LaborBudget',
 'BacklogToDate':'LaborRemaining',
 'ProjectArea':'ProjectBusinessUnit',
 'EmployeeID':'EmployeeId',
 'Title':'EmployeePosition',
 'PracticeGroupDeltek':'EmployeePracticeGroupDeltek',
 'BusinessUnit':'EmployeeBusinessUnitDeltek',
 'PracticeGroupADP':'EmployeeDepartmentAdp',
 'BillingCategory':'BillingLaborCategoryDeltek',
 'CertificationCode':'CertificationCodeAdp',
 'EmploymentStatus':'EmployeeStatus',
 'LicenseCertificationID':'LicenseCertificationId',
 'ResumeURL':'ResumeUrl',
 'ProjectManagerID':'ProjectManagerId',
 'RevenueToDate':'ProjectRevenueToDate',
 'BilledToDate':'ProjectBilledToDate',
 'TotalRevenueBudget':'ProjectTotalRevenueBudget'
 }
print("Renamed columns as per the dictionary:")
df = df.rename(columns=rename_dict)



df['Id'] = range(1, len(df) + 1)
df['Id'] = df['Id'].astype(str)
print("Added 'Id' column and converted to string:")

df['IndexFlag'] = 0
print("Added 'IndexFlag' column with default value 0:")

df = df[['Id','ClientId', 'ClientFolderId', 'ClientName', 'Worked',
       'ClientAddressCity', 'ClientAddressState', 'ProjectId', 'ProjectName',
       'ProjectStatus', 'ProjectAddressStreet', 'ProjectAddressCity',
       'ProjectAddressState', 'ProjectManager', 'ProjectManagerId',
       'LaborBudget', 'ProjectRevenueToDate', 'ProjectBilledToDate',
       'UnbilledToDate', 'LaborRemaining', 'ProjectTotalRevenueBudget',
       'ProjectBusinessUnit', 'ProjectType', 'ProjectPracticeServiceGroup',
       'ProjectPracticeGroup', 'ProjectServiceGroup', 'EmployeeId',
       'EmployeeName', 'EmployeePosition', 'EmployeeDepartmentAdp',
       'EmployeePracticeGroupDeltek', 'EmployeeBusinessUnitDeltek',
       'EmployeeJobCostRate', 'EmployeeUtilization', 'TargetUtilization',
       'ResumeUrl', 'NameOfCertification', 'CertificationCodeAdp',
       'EmployeeStatus', 'LicenseCertificationDescription',
       'LicenseCertificationState', 'LicenseCertificationId',
       'BillingLaborCategoryDeltek']]

# Replace 'NA' with 0 for Int64 columns and 0.0 for Double columns
int64_columns = [
    'LaborBudget', 'LaborRemaining', 'ProjectManagerId', 
    'ProjectRevenueToDate', 'ProjectBilledToDate', 'TargetUtilization'
]

double_columns = [
    'BillingLaborCategoryDeltek', 'UnbilledToDate', 
    'ProjectTotalRevenueBudget', 'EmployeeJobCostRate', 'EmployeeUtilization'
]

df[int64_columns] = df[int64_columns].replace(['NA', 'NaN'], np.nan)  # Replace with NaN
df[int64_columns] = df[int64_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype('Int64')

df[double_columns] = df[double_columns].replace(['NA', 'NaN'], np.nan)  # Replace with NaN
df[double_columns] = df[double_columns].apply(pd.to_numeric, errors='coerce').fillna(0.0).astype('float64')


# df_sample = df.sample(5,random_state=42)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import pandas as pd
search_api_key = "8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u"

search_service_name = "vpc-dev-aisearch"
index_name = "simple_fields_adp-deltek-fabric-final"
endpoint = f"https://{search_service_name}.search.windows.net"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




# Length of the embedding vector (OpenAI ada-002 generates embeddings of length 1536)
EMBEDDING_LENGTH = 1536

# Create index for AI Search with fields id, content, and contentVector
# Note the datatypes for each field below
url = f"https://{search_service_name}.search.windows.net/indexes/{index_name}?api-version=2023-11-01"
payload = json.dumps(
    {
        "name": index_name,
        "fields": [
            # Unique identifier for each document
            {
                "name": "Id",
                "type": "Edm.String",
                "key": True,
                "filterable": True,
                "searchable": True, 
                "retrievable": True,               
                "sortable": True,
                "facetable": True                
            },
            # {
            #     "name": "IndexFlag",
            #     "type": "Edm.Boolean",
            #     "key": False,
            #     "filterable": True,
            #     "searchable": False, 
            #     "retrievable": True,               
            #     "sortable": True,
            #     "facetable": True                
            # },
            {
                "name": "ClientId",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True, 
                "retrievable": True,               
                "sortable": True,
                "facetable": True                
            },
            {
                "name": "ClientFolderId",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True               
            },            
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
            "name": "ProjectId",
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
            {
                "name": "LaborBudget",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable": False,
                "facetable": False 

            },
            {
                "name": "LaborRemaining",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable": False,
                "facetable": False 

            },
            # {
            #     "name": "RevenueTotal",
            #     "type": "Edm.String",
            #     "key": False,
            #     "filterable": True,
            #     "searchable": True,
            #     "retrievable": True,
            #     "sortable": False,
            #     "facetable": True 
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
                "name": "ProjectBusinessUnit",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            
                    {
                    "name": "EmployeeId",
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
                    # {
                    #     "name": "EmployeeNameDeltek",
                    #     "type": "Edm.String",
                    #     "key": False,
                    #     "filterable": True,
                    #     "searchable": True,
                    #     "retrievable": True,
                    #     "sortable": False,
                    #     "facetable": True 

                    # },
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
                    # {
                    #     "name": "EmployeePositionADP",
                    #     "type": "Edm.String",
                    #     "key": False,
                    #     "filterable": True,
                    #     "searchable": True,
                    #     "retrievable": True,
                    #     "sortable": False,
                    #     "facetable": True 

                    # },
                    # {
                    #     "name": "EmployeePracticeGroupADP",
                    #     "type": "Edm.String",
                    #     "key": False,
                    #     "filterable": True,
                    #     "searchable": True,
                    #     "retrievable": True,
                    #     "sortable": False,
                    #     "facetable": True 

                    # },
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
                    
                    # {
                    #     "name": "BillingLaborCategoryDeltek",
                    #     "type": "Edm.String",
                    #     "key": False,
                    #     "filterable": True,
                    #     "searchable": True,
                    #     "retrievable": True,
                    #     "sortable": False,
                    #     "facetable": True 

                    # },
                    # {
                    #     "name": "SkillsResume",
                    #     "type": "Edm.String",
                    #     "key": False,
                    #     "filterable": True,
                    #     "searchable": True,
                    #     "retrievable": True,
                    #     "sortable": False,
                    #     "facetable": True
                    #     },
                    {
                        "name": "EmployeeBusinessUnitDeltek",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 

                    },
                    {
                        "name": "EmployeeDepartmentAdp",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 

                    },
                    {
                        "name": "BillingLaborCategoryDeltek",
                        "type": "Edm.Double",
                        "key": False,
                        "filterable": True,
                        "searchable": False,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": False 

                    },
                    # {
                    #     "name": "Skills",
                    #     "type": "Edm.String",
                    #     "key": False,
                    #     "filterable": True,
                    #     "searchable": True,
                    #     "retrievable": True,
                    #     "sortable": False,
                    #     "facetable": True 
                    # },
                    {
                        "name": "CertificationCodeAdp",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 

                    },
                    
                    # {
                    #     "name": "CertificationADP",
                    #     "type": "Edm.String",
                    #     "key": False,
                    #     "filterable": True,
                    #     "searchable": True,
                    #     "retrievable": True,
                    #     "sortable": False,
                    #     "facetable": True
                    # },
                    {
                    "name": "NameOfCertification",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    
                    {
                    "name": "EmployeeStatus",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "LicenseCertificationDescription",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "LicenseCertificationState",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "LicenseCertificationId",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "ResumeUrl",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
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
                    "name": "ProjectManagerId",
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
                    "name": "UnbilledToDate",
                    "type": "Edm.Double",
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
                    "name": "TargetUtilization",
                    "type": "Edm.Int64",
                    "key": False,
                    "filterable": True,
                    "searchable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },        

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
                        "titleField": {"fieldName": "Id"},                
                        "prioritizedContentFields": [],
                        "prioritizedKeywordsFields": [
                        {"fieldName":"Id"},
                        # {"fieldName":"IndexFlag"},
                        {"fieldName": "ClientId"},
                        {"fieldName": "ClientFolderId"},
                        {"fieldName": "ClientName"},
                        {"fieldName": "Worked"},
                        {"fieldName": "ClientAddressCity"},
                        {"fieldName": "ClientAddressState"},
                        {"fieldName": 'ProjectId'},                        
                        {"fieldName": "ProjectName"},
                        # {"fieldName": "RevenueTotal"},
                        # {"fieldName": "LaborBudget"},
                        # {"fieldName": "LaborRemaining"},
                        {"fieldName": "ProjectStatus"},
                        {"fieldName": "ProjectAddressStreet"},
                        {"fieldName": "ProjectAddressCity"},
                        {"fieldName": "ProjectAddressState"},
                        {"fieldName": "ProjectType"},
                        {"fieldName": "ProjectPracticeGroup"},
                        {"fieldName": "ProjectBusinessUnit"},
                        {"fieldName": "EmployeeId"},
                        {"fieldName": "EmployeeName"},
                        {"fieldName": "EmployeePosition"},
                        # {"fieldName": "EmployeePracticeGroupADP"},
                        {"fieldName": "EmployeePracticeGroupDeltek"},
                        {"fieldName": "EmployeeBusinessUnitDeltek"},
                        {"fieldName": "EmployeeDepartmentAdp"},
                        # {"fieldName": "BillingLaborCategoryDeltek"},
                        # {"fieldName": "Skills"},
                        {"fieldName": "CertificationCodeAdp"},
                        {"fieldName": "NameOfCertification"},
                        {"fieldName": "EmployeeStatus"},
                        {"fieldName": "LicenseCertificationDescription"},
                        {"fieldName": "LicenseCertificationState"},
                        {"fieldName": "LicenseCertificationId"},
                        {"fieldName": "ResumeUrl"},
                        {"fieldName": "ProjectManager"},
                        # {"fieldName": "ProjectManagerId"},
                        # {"fieldName": "ProjectRevenueToDate"},
                        # {"fieldName": "ProjectBilledToDate"},
                        # {"fieldName": "UnbilledToDate"},
                        # {"fieldName": "ProjectTotalRevenueBudget"},
                        {"fieldName": "ProjectPracticeServiceGroup"},
                        {"fieldName": "ProjectServiceGroup"},
                        # {"fieldName": "EmployeeJobCostRate"},
                        # {"fieldName": "EmployeeUtilization"},
                        # {"fieldName": "TargetUtilization"}
                        
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

import re
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
    BATCH_SIZE = 300
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:
            documents.append({
            "Id": make_safe_id(row["Id"]),
            "ClientId": row["ClientId"],
            "ClientFolderId": row["ClientFolderId"],
            "ClientName": row["ClientName"],
            "Worked": row["Worked"],
            "ClientAddressCity": row["ClientAddressCity"],
            "ClientAddressState": row["ClientAddressState"],
            "ProjectId": row["ProjectId"],
            "ProjectName": row["ProjectName"],
            "LaborBudget": row["LaborBudget"],
            "LaborRemaining": row["LaborRemaining"],
            "ProjectStatus": row["ProjectStatus"],
            "ProjectAddressStreet": row["ProjectAddressStreet"],
            "ProjectAddressCity": row["ProjectAddressCity"],
            "ProjectAddressState": row["ProjectAddressState"],
            "ProjectType": row["ProjectType"],
            "ProjectPracticeGroup": row["ProjectPracticeGroup"],
            "ProjectBusinessUnit": row["ProjectBusinessUnit"],
            "EmployeeId": row["EmployeeId"],
            "EmployeeName": row["EmployeeName"],
            "EmployeePosition": row["EmployeePosition"],
            "EmployeePracticeGroupDeltek": row["EmployeePracticeGroupDeltek"],
            "EmployeeBusinessUnitDeltek": row["EmployeeBusinessUnitDeltek"],
            "EmployeeDepartmentAdp": row["EmployeeDepartmentAdp"],
            "BillingLaborCategoryDeltek": row["BillingLaborCategoryDeltek"],
            # "Skills": row["Skills"],
            "CertificationCodeAdp": row["CertificationCodeAdp"],
            "NameOfCertification": row["NameOfCertification"],
            "EmployeeStatus": row["EmployeeStatus"],
            "LicenseCertificationDescription": row["LicenseCertificationDescription"],
            "LicenseCertificationState": row["LicenseCertificationState"],
            "LicenseCertificationId": row["LicenseCertificationId"],
            "ResumeUrl": row["ResumeUrl"],
            "ProjectManager": row["ProjectManager"],
            "ProjectManagerId": row["ProjectManagerId"],
            "ProjectRevenueToDate": row["ProjectRevenueToDate"],
            "ProjectBilledToDate": row["ProjectBilledToDate"],
            "UnbilledToDate": row["UnbilledToDate"],
            "ProjectTotalRevenueBudget": row["ProjectTotalRevenueBudget"],
            "ProjectPracticeServiceGroup": row["ProjectPracticeServiceGroup"],
            "ProjectServiceGroup": row["ProjectServiceGroup"],
            "EmployeeJobCostRate": row["EmployeeJobCostRate"],
            "EmployeeUtilization": row["EmployeeUtilization"],
            "TargetUtilization": row["TargetUtilization"],
            "@search.action": "upload",
            }
    )
        status = insert_into_index(documents)
        print([row_batch[0]["row_index"], row_batch[-1]["row_index"], status])
        yield [row_batch[0]["row_index"], row_batch[-1]["row_index"], status]
        
        
df['row_index'] = range(len(df))

# Convert DataFrame rows to list of dicts
rows = df.to_dict(orient='records')

# Run upload_batch on partitions of the dataframe
results = list(upload_rows(rows))

# Convert results to DataFrame for display
res_df = pd.DataFrame(results, columns=["start_index", "end_index", "insertion_status"])


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

# CELL ********************


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
