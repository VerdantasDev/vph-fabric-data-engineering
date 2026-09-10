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
pd.set_option('display.max_columns', None) 

pd.set_option('display.max_colwidth', None) 

import os
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "adp-deltek": f"{lakehouse_path}/sv_adp_deltek_merged_v2",
    
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

display(df)

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

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.drop(columns= ["associateOID","OfficeLocation","EmployeeCode","Employee"], axis = 1, inplace = True)

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

# df.drop(columns=["EmployeeCode"],axis = 1, inplace =True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df.rename(columns ={"Employee": "EmployeeCode"}, inplace = True)
# df.drop(columns=["EmployeeCode"], axis = 1, inplace = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df[['ProvisionalBillingRate','RegularHoursChargeTypeRegular',
       'OvertimeHoursChargeTypeRegular', 'TotalHoursChargeTypeRegular',
       'RegularHoursChargeTypeOverhead', 'OvertimeHoursChargeTypeOverhead',
       'TotalHoursChargeTypeOverhead', 'RegularHoursChargeTypePromotional',
       'OvertimeHoursChargeTypePromotional', 'TotalHoursChargeTypePromotional',
       'RegularHoursChargeTypeN', 'OvertimeHoursChargeTypeN',
       'TotalHoursChargeTypeN']] = df[['ProvisionalBillingRate','RegularHoursChargeTypeRegular',
       'OvertimeHoursChargeTypeRegular', 'TotalHoursChargeTypeRegular',
       'RegularHoursChargeTypeOverhead', 'OvertimeHoursChargeTypeOverhead',
       'TotalHoursChargeTypeOverhead', 'RegularHoursChargeTypePromotional',
       'OvertimeHoursChargeTypePromotional', 'TotalHoursChargeTypePromotional',
       'RegularHoursChargeTypeN', 'OvertimeHoursChargeTypeN',
       'TotalHoursChargeTypeN']].fillna(0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df["VerdantasStartDate"] = df["VerdantasStartDate"].fillna("No Date")
df["ReportingMangaer"] = df["ReportingMangaer"].fillna("No Reporting Mangaer")
df["HomeDepartmentEmailList"] = df["HomeDepartmentEmailList"].fillna("No List")
df["ExperienceDate"] = df["ExperienceDate"].fillna("No Experience Date")
df["PracticeGroup"] = df["PracticeGroup"].fillna("No Practice Group")
df["CertificationCode"] = df["CertificationCode"].fillna("No Certification Code")
df["NameOfCertification"] = df["NameOfCertification"].fillna("No Certification")
df["PracticeGroupCode"] = df["PracticeGroupCode"].fillna("No Code")
df["OvertimeAllowed"] = df["OvertimeAllowed"].fillna("No Overtime Allowed")
df["CustomBillingType"] = df["CustomBillingType"].fillna("No Custom Billing Type")
df["ProjectID_LD"] = df["ProjectID_LD"].fillna("No Project ID")

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

import math

def custom_rounding(value):
    if pd.isna(value):
        return value
    if float(value) - math.floor(float(value)) < 0.5:
        return math.floor(float(value))
    else:
        return math.ceil(float(value))


def rounding(df,col_list):
    for col in col_list:
        df[col] = df[col].astype(float)
        df[col] = df[col].apply(custom_rounding)
        df[col] = df[col].astype(int)

    return df


df = rounding(df,['ProvisionalBillingRate','RegularHoursChargeTypeRegular',
       'OvertimeHoursChargeTypeRegular', 'TotalHoursChargeTypeRegular',
       'RegularHoursChargeTypeOverhead', 'OvertimeHoursChargeTypeOverhead',
       'TotalHoursChargeTypeOverhead', 'RegularHoursChargeTypePromotional',
       'OvertimeHoursChargeTypePromotional', 'TotalHoursChargeTypePromotional',
       'RegularHoursChargeTypeN', 'OvertimeHoursChargeTypeN',
       'TotalHoursChargeTypeN'])

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

def datatype_conversion(df):
    for col in df.columns:
        if df[col].dtype in ["datetime64[ns]"]:
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

df.columns

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

df_unique = df.drop_duplicates(subset=['EmployeeName', 'Title',"CertificationCode"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_unique.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_unique.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



def remove_doller(df, col_list):
    for col in col_list:
        df[col] = df[col].str.replace('$', '', regex=False).astype(float)
        df[col] = df[col].apply(custom_rounding)

    return df



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
                "name": "EmployeeName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
                
            },

    
            {
                "name": "EmploymentStatus",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
            },
             {
                 "name": "PositionStartDate",
                 "type": "Edm.String",
                 "key": False,
                 "filterable": True,
                 "searchable": True,
                 "retrievable": True,
                 "sortable" : True,
		 "facetable" : True,
             },
            {
                "name": "Position",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },
            {
                "name": "VerdantasStartDate",
                "type": "Edm.String",
                 "key": False,
                 "filterable": True,
                 "searchable": True,
                 "retrievable": True,
                 "sortable" : True,
		 "facetable" : True,
            },
            {
                "name": 'EmployeeBillType',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
            },
            {
                "name": 'Title',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },
	  {
                "name": 'ReportingMangaer',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
            },
            {
                "name": 'BusinessUnit',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,

            },
	    {
                "name": 'Department',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
	   },

	{
                "name": 'HomeDepartmentEmailList',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
	},
	{
                "name": 'LegacyCompany',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
	},
	{
                "name": 'ExperienceDate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
	},
	{
                "name": 'PracticeGroup',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
		},
            {
                "name": 'CertificationCode',
                "type": "Edm.String",
                "key": False,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
                "filterable": True,
		"facetable" : True,
            },

           {
                "name": 'NameOfCertification',
                "type": "Edm.String",
                "key": False,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
                "filterable": True,
		"facetable" : True,
            },
            
            {
                "name": 'ProvisionalBillingRate',
                "type": "Edm.Int64",
                "key": False,
                "searchable": True,
                "filterable": True,
                "retrievable": True,
                "sortable": True,
		"facetable" : True,
            },

            {
                "name": 'PracticeGroupCode',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
            },
            {
                "name": 'OvertimeAllowed',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
            },
	    {
                "name": 'CustomBillingType',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
            },
	  {
                "name": 'ProjectID_LD',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		"facetable" : True,
            },

            {
                "name": 'RegularHoursChargeTypeRegular',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },
            {
                "name": 'OvertimeHoursChargeTypeRegular',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },
            {
                "name": 'TotalHoursChargeTypeRegular',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },

            {
                "name": 'RegularHoursChargeTypeOverhead',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },
            
       
            {
                "name": 'OvertimeHoursChargeTypeOverhead',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },

            {
                "name": 'TotalHoursChargeTypeOverhead',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },
            {
                "name": 'RegularHoursChargeTypePromotional',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },

             {
                "name": 'OvertimeHoursChargeTypePromotional',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },
             {
                "name": 'TotalHoursChargeTypePromotional',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,

            },
             {
                "name": 'RegularHoursChargeTypeN',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },

             
             {
                "name": 'OvertimeHoursChargeTypeN',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            },
             {
                "name": 'TotalHoursChargeTypeN',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		"facetable" : True,
            }            
        ],
       

        "semantic": {
            "configurations": [
                {
                    "name": "azureml-default",
                    "prioritizedFields": {
                        "titleField": {"fieldName": "EmployeeName"},
                        "prioritizedContentFields": [
                                                    {'fieldName': 'EmploymentStatus'},
                                                    {'fieldName': 'PositionStartDate'},
                                                    {'fieldName': 'Position'},
                                                    {'fieldName': 'EmployeeNumber'},
                                                    {'fieldName': 'VerdantasStartDate'},
                                                    {'fieldName': 'EmployeeBillType'},
                                                    {'fieldName': 'Title'},
                                                    {'fieldName': 'ReportingMangaer'},
                                                    {'fieldName': 'BusinessUnit'},
                                                    {'fieldName': 'Department'},
                                                    {'fieldName': 'HomeDepartmentEmailList'},
                                                    {'fieldName': 'LegacyCompany'},
                                                    {'fieldName': 'ExperienceDate'},
                                                    {'fieldName': 'PracticeGroup'},
                                                    {'fieldName': 'CertificationCode'},
                                                    {'fieldName': 'NameOfCertification'},
                                                    # {'fieldName': 'ProvisionalBillingRate'},
                                                    {'fieldName': 'PracticeGroupCode'},
                                                    {'fieldName': 'OvertimeAllowed'},
                                                    {'fieldName': 'CustomBillingType'},
                                                    {'fieldName': 'ProjectID_LD'},
                                                    # {'fieldName': 'RegularHoursChargeTypeRegular'},
                                                    # {'fieldName': 'OvertimeHoursChargeTypeRegular'},
                                                    # {'fieldName': 'TotalHoursChargeTypeRegular'},
                                                    # {'fieldName': 'RegularHoursChargeTypeOverhead'},
                                                    # {'fieldName': 'OvertimeHoursChargeTypeOverhead'},
                                                    # {'fieldName': 'TotalHoursChargeTypeOverhead'},
                                                    # {'fieldName': 'RegularHoursChargeTypePromotional'},
                                                    # {'fieldName': 'OvertimeHoursChargeTypePromotional'},
                                                    # {'fieldName': 'TotalHoursChargeTypePromotional'},
                                                    # {'fieldName': 'RegularHoursChargeTypeN'},
                                                    # {'fieldName': 'OvertimeHoursChargeTypeN'},
                                                    # {'fieldName': 'TotalHoursChargeTypeN'}
                                                    ]
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ADP-Deltek Data with Project Details

# CELL ********************

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "adp-deltek": f"{lakehouse_path}/sv_deltek_adp_merged_v5",
    
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

df.shape

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

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df[["Employee_EMCompany","EmployeeCode","EmployeeNumber","Employee_LD","Employee_UTE","Employee_LD_PR"]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.drop(columns= ["associateOID","Employee_EMCompany","EmployeeCode","Employee_LD","Employee_UTE","Employee_LD_PR"], axis = 1, inplace = True)

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

import pandas as pd
pd.set_option('display.max_columns', 50) 
df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df[['ProvisionalBillingRate','RegularHours', 'OvertimeHours', 'SpecialOvertimeHours', 
'TotalHours', 'MaximumRate', 'MinimumRate', 'AverageRate', 'MaximumOvertimeRate', 
'MinimumOvertimeRate', 'AverageOvertimeRate', 'UTE', 'Target_UTE', 'Gap', 'Use', 
'Billability', 'STD_Hours']] = df[['ProvisionalBillingRate','RegularHours', 'OvertimeHours', 
                                'SpecialOvertimeHours', 'TotalHours', 'MaximumRate', 
                                'MinimumRate', 'AverageRate', 'MaximumOvertimeRate', 
                                'MinimumOvertimeRate', 'AverageOvertimeRate', 'UTE', 
                                'Target_UTE', 'Gap', 'Use', 'Billability', 'STD_Hours']].fillna(0)

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

df["VerdantasStartDate"] = df["VerdantasStartDate"].fillna("0000-00-00")
df["ExperienceDate"] = df["ExperienceDate"].fillna("0000-00-00")
df["PracticeGroup"] = df["PracticeGroup"].fillna("0000-00-00")
df["OfficeLocation"] = df["OfficeLocation"].fillna("No Location")


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

df.info()

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


def rounding(df,col_list):
    for col in col_list:
        df[col] = df[col].astype(float)
        df[col] = df[col].apply(custom_rounding)
        df[col] = df[col].astype(int)

    return df


df = rounding(df,['ProvisionalBillingRate','RegularHours', 'OvertimeHours', 
                                'SpecialOvertimeHours', 'TotalHours', 'MaximumRate', 
                                'MinimumRate', 'AverageRate', 'MaximumOvertimeRate', 
                                'MinimumOvertimeRate', 'AverageOvertimeRate', 'UTE', 
                                'Target_UTE', 'Gap', 'Use', 'Billability', 'STD_Hours'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df["Supervisor_ID"] = df["Supervisor_ID"].astype(str)

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

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json


# openai_credential = DefaultAzureCredential()

# Length of the embedding vector (OpenAI ada-002 generates embeddings of length 1536)
EMBEDDING_LENGTH = 1536

search_service_name = "vpc-dev-aisearch"
index_name = "vpc-adp-deltek-project-index-v1"

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
                "name": "EmployeeName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
                
            },
            {
                "name": "EmploymentStatus",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": "EmployeeNumber",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
             {
                 "name": "PositionStartDate",
                 "type": "Edm.String",
                 "key": False,
                 "filterable": True,
                 "searchable": True,
                 "retrievable": True,
                 "sortable" : True,
		        "facetable" : True,
             },
            {
                "name": "Position",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": "VerdantasStartDate",
                "type": "Edm.String",
                 "key": False,
                 "filterable": True,
                 "searchable": True,
                 "retrievable": True,
                 "sortable" : True,
		         "facetable" : True,
            },
            {
                "name": 'EmployeeBillType',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'Title',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
	        {
                "name": 'ReportingMangaer',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'BusinessUnit',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,

            },
	        {
                "name": 'Department',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
	        },
            {   
                "name": 'OfficeLocation',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,

            },

	        {
                "name": 'HomeDepartmentEmailList',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
	        },
	        {   
                "name": 'LegacyCompany',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
	        },
	        {
                "name": 'ExperienceDate',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
	        },
	        {
                "name": 'PracticeGroup',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
		    },
            {
                "name": 'certification_data',
                "type": "Edm.String",
                "key": False,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
                "filterable": True,
		        "facetable" : True,
            },
            
            {
                "name": 'ProvisionalBillingRate',
                "type": "Edm.Int64",
                "key": False,
                "searchable": False,
                "filterable": True,
                "retrievable": True,
                "sortable": True,
		        "facetable" : True,
            },

            {
                "name": 'PastYeardataPresent',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'RegularHours',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },

            {
                "name": 'OvertimeHours',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
	        {
                "name": 'SpecialOvertimeHours',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },


	        {
                "name": 'TotalHours',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },

            {
                "name": 'MaximumRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'MinimumRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
             
            {
                "name": 'AverageRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },

            {
                "name": 'MaximumOvertimeRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            
       
            {
                "name": 'MinimumOvertimeRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'AverageOvertimeRate',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'Supervisor_ID',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },

             {
                "name": 'Supervisor_Name',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },

             {
                "name": 'UTE',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
         
            {
                "name": 'Target_UTE',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
             {
                "name": 'Gap',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'Use',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'Billability',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'STD_Hours',
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,
            },
            {
                "name": 'ProjectDetails',
                "type": "Edm.String",
                "key": False,
                "filterable": False,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		        "facetable" : False,
            },
            {
                "name": 'ClientDetails',
                "type": "Edm.String",
                "key": False,
                "filterable": False,
                "searchable": True,
                "retrievable": True,
                "sortable" : False,
		        "facetable" : False,
            },
            {
                "name": 'UtilizationStatus',
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable" : True,
		        "facetable" : True,

            },      
        ],
       

        "semantic": {
            "configurations": [
                {
                    "name": "azureml-default",
                    "prioritizedFields": {
                        "titleField": {"fieldName": "EmployeeName"},
                        "prioritizedContentFields": [
                                                    {'fieldName': 'EmploymentStatus'},
                                                    {'fieldName': 'PositionStartDate'},
                                                    {'fieldName': 'Position'},
                                                    # {'fieldName': 'EmployeeNumber'},
                                                    {'fieldName': 'OfficeLocation'},
                                                    {'fieldName': 'VerdantasStartDate'},
                                                    {'fieldName': 'EmployeeBillType'},
                                                    {'fieldName': 'Title'},
                                                    {'fieldName': 'ReportingMangaer'},
                                                    {'fieldName': 'BusinessUnit'},
                                                    {'fieldName': 'Department'},
                                                    {'fieldName': 'HomeDepartmentEmailList'},
                                                    {'fieldName': 'LegacyCompany'},
                                                    {'fieldName': 'ExperienceDate'},
                                                    {'fieldName': 'PracticeGroup'},
                                                    {'fieldName': 'certification_data'},
                                                    # {'fieldName': 'ProvisionalBillingRate'},
                                                    {'fieldName': 'PastYeardataPresent'},
                                                    # {'fieldName': 'RegularHours'},
                                                    # {'fieldName': 'OvertimeHours'},
                                                    # {'fieldName': 'SpecialOvertimeHours'},
                                                    # {'fieldName': 'TotalHours'},
                                                    # {'fieldName': 'MaximumRate'},
                                                    # {'fieldName': 'MinimumRate'},
                                                    # {'fieldName': 'AverageRate'},
                                                    # {'fieldName': 'MaximumOvertimeRate'},
                                                    # {'fieldName': 'MinimumOvertimeRate'},
                                                    # {'fieldName': 'AverageOvertimeRate'},
                                                    {'fieldName': 'Supervisor_ID'},
                                                    {'fieldName': 'Supervisor_Name'},
                                                    # {'fieldName': 'UTE'},
                                                    # {'fieldName': 'Target_UTE'},
                                                    # {'fieldName': 'Gap'},
                                                    # {'fieldName': 'Use'},
                                                    # {'fieldName': 'Billability'},
                                                    # {'fieldName': 'STD_Hours'},
                                                    {'fieldName': 'ProjectDetails'},
                                                    {'fieldName': 'ClientDetails'},
                                                    {'fieldName': 'UtilizationStatus'},
                                                    ]
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

df.head()

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
from datetime import date

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
                    "EmployeeName": row["EmployeeName"],
                    "EmploymentStatus": row["EmploymentStatus"],
                    "PositionStartDate": row["PositionStartDate"],
                    "Position": row["Position"],
                    "EmployeeNumber": int(row["EmployeeNumber"]),
                    "VerdantasStartDate": row["VerdantasStartDate"],
                    "EmployeeBillType": row["EmployeeBillType"],
                    "Title": row["Title"],
                    "OfficeLocation": row["OfficeLocation"],
                    "ReportingMangaer": row["ReportingMangaer"],
                    "BusinessUnit": row["BusinessUnit"],
                    "Department": row["Department"],
                    "HomeDepartmentEmailList": row["HomeDepartmentEmailList"],
                    "LegacyCompany": row["LegacyCompany"],
                    "ExperienceDate": row["ExperienceDate"],
                    "PracticeGroup": row["PracticeGroup"],
                    "certification_data": row["certification_data"],
                    "ProvisionalBillingRate": int(row["ProvisionalBillingRate"]),
                    "PastYeardataPresent": row["PastYeardataPresent"],
                    "RegularHours": int(row["RegularHours"]),
                    "OvertimeHours": int(row["OvertimeHours"]),
                    "SpecialOvertimeHours": int(row["SpecialOvertimeHours"]),
                    "TotalHours": int(row["TotalHours"]),
                    "MaximumRate": int(row["MaximumRate"]),
                    "MinimumRate": int(row["MinimumRate"]),
                    "AverageRate": int(row["AverageRate"]),
                    "MaximumOvertimeRate": int(row["MaximumOvertimeRate"]),
                    "MinimumOvertimeRate": int(row["MinimumOvertimeRate"]),
                    "AverageOvertimeRate": int(row["AverageOvertimeRate"]),
                    "Supervisor_ID": row["Supervisor_ID"],
                    "Supervisor_Name": row["Supervisor_Name"],
                    "UTE": int(row["UTE"]),
                    "Target_UTE": int(row["Target_UTE"]),
                    "Gap": int(row["Gap"]),
                    "Use": int(row["Use"]),
                    "Billability": int(row["Billability"]),
                    "STD_Hours": int(row["STD_Hours"]),
                    "ProjectDetails": row["ProjectDetails"],
                    "UtilizationStatus": row["UtilizationStatus"],
                    "ClientDetails": row["ClientDetails"],
                    "@search.action": "upload",
                }
            )
        status = insert_into_index(documents)
        print([row_batch[0]["row_index"], row_batch[-1]["row_index"], status])
        yield [row_batch[0]["row_index"], row_batch[-1]["row_index"], status]



# Add row index for Azure Search
df1['row_index'] = range(len(df1))
df1['row_index'] = df1['row_index'].astype(str)

# Convert DataFrame rows to list of dicts
rows = df1.to_dict(orient='records')

# Run upload_rows on partitions of the dataframe
results = list(upload_rows(rows))

# Convert results to DataFrame for display
res_df = pd.DataFrame(results, columns=["start_index", "end_index", "insertion_status"])
print(res_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

res_df["insertion_status"][0]

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
from datetime import date

def insert_into_index(documents):
    """Uploads a list of 'documents' to Azure AI Search index and returns status and failed documents."""
    url = f"https://{search_service_name}.search.windows.net/indexes/{index_name}/docs/index?api-version=2023-11-01"
    payload = json.dumps({"value": documents})
    headers = {
        "Content-Type": "application/json",
        "api-key": search_api_key,
    }
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200 or response.status_code == 201:
        return "Success", []
    else:
        # Return the response text for debugging
        return f"Failure: {response.text}", documents

def make_safe_id(row_id: str):
    """Strips disallowed characters from row id for use as Azure AI search document ID."""
    return re.sub("[^0-9a-zA-Z_-]", "_", row_id)

def upload_rows(rows):
    """Uploads the rows in a dataframe to Azure AI Search.
    Limits uploads to 1000 rows at a time due to Azure AI Search API limits.
    """
    BATCH_SIZE = 1000
    all_results = []
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:
            documents.append(
                {
                    "id": make_safe_id(row["row_index"]),                    
                    "EmployeeName": row["EmployeeName"],
                    "EmploymentStatus": row["EmploymentStatus"],
                    "PositionStartDate": row["PositionStartDate"],
                    "Position": row["Position"],
                    "EmployeeNumber": int(row["EmployeeNumber"]),
                    "VerdantasStartDate": row["VerdantasStartDate"],
                    "EmployeeBillType": row["EmployeeBillType"],
                    "Title": row["Title"],
                    "OfficeLocation": row["OfficeLocation"],
                    "ReportingMangaer": row["ReportingMangaer"],
                    "BusinessUnit": row["BusinessUnit"],
                    "Department": row["Department"],
                    "HomeDepartmentEmailList": row["HomeDepartmentEmailList"],
                    "LegacyCompany": row["LegacyCompany"],
                    "ExperienceDate": row["ExperienceDate"],
                    "PracticeGroup": row["PracticeGroup"],
                    "certification_data": row["certification_data"],
                    "ProvisionalBillingRate": int(row["ProvisionalBillingRate"]),
                    "PastYeardataPresent": row["PastYeardataPresent"],
                    "RegularHours": int(row["RegularHours"]),
                    "OvertimeHours": int(row["OvertimeHours"]),
                    "SpecialOvertimeHours": int(row["SpecialOvertimeHours"]),
                    "TotalHours": int(row["TotalHours"]),
                    "MaximumRate": int(row["MaximumRate"]),
                    "MinimumRate": int(row["MinimumRate"]),
                    "AverageRate": int(row["AverageRate"]),
                    "MaximumOvertimeRate": int(row["MaximumOvertimeRate"]),
                    "MinimumOvertimeRate": int(row["MinimumOvertimeRate"]),
                    "AverageOvertimeRate": int(row["AverageOvertimeRate"]),
                    "Supervisor_ID": row["Supervisor_ID"],
                    "Supervisor_Name": row["Supervisor_Name"],
                    "UTE": int(row["UTE"]),
                    "Target_UTE": int(row["Target_UTE"]),
                    "Gap": int(row["Gap"]),
                    "Use": int(row["Use"]),
                    "Billability": int(row["Billability"]),
                    "STD_Hours": int(row["STD_Hours"]),
                    "ProjectDetails": row["ProjectDetails"],
                    "UtilizationStatus": row["UtilizationStatus"],
                    "ClientDetails": row["ClientDetails"],
                    "@search.action": "upload",
                }
            )
        status, failed_documents = insert_into_index(documents)
        if failed_documents:
            # Append failed rows with their status
            for doc in failed_documents:
                all_results.append({
                    "row_index": doc["id"],
                    "status": status
                })
        else:
            all_results.append({
                "start_index": row_batch[0]["row_index"],
                "end_index": row_batch[-1]["row_index"],
                "status": status
            })
    return all_results

# Add row index for Azure Search
df1['row_index'] = range(len(df1))
df1['row_index'] = df1['row_index'].astype(str)

# Convert DataFrame rows to list of dicts
rows = df1.to_dict(orient='records')

# Run upload_rows on partitions of the dataframe
results = upload_rows(rows)

# Convert results to DataFrame for display
res_df = pd.DataFrame(results)
print(res_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

res_df["status"][0]

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

df1 = df.copy()

def clean_json(json_str):
    data_list = json.loads(json_str)
    return json.dumps([item for item in data_list if item])

# Apply cleaning function to relevant columns
df1['ProjectDetails'] = df1['ProjectDetails'].apply(clean_json)
df1['ClientDetails'] = df1['ClientDetails'].apply(clean_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1["VerdantasStartDate"] = df1["VerdantasStartDate"].astype(str)
df1["ExperienceDate"] = df1["ExperienceDate"].astype(str)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

search_value = "IEEE754Compatible"
# List to hold columns that contain the search value
columns_with_value = []

# Iterate over columns
for column in df1.columns:
    if df1[column].eq(search_value).any():
        columns_with_value.append(column)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### sv-deltek-employee-v3 index creation

# CELL ********************

from pyspark.sql import SparkSession
import pandas as pd
pd.set_option('display.max_columns', 500) 

pd.set_option('display.max_colwidth', None) 

import os
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "deltek": f"{lakehouse_path}/sv_deltek_Employees_v3",
    
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read tables into DataFrames
df_ADP_deltek = spark.read.format("delta").load(table_paths["deltek"])
df = df_ADP_deltek.toPandas()

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

df.head()

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

df["RevenueTotal"].value_counts()

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





def rounding(df,col_list):
    for col in col_list:
        df[col] = df[col].astype(float)
        df[col] = df[col].apply(custom_rounding)

    return df
    

df = rounding(df,["BillingRate"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df["NameOfCertification"] = df["NameOfCertification"].fillna("No certification")
df["CertificationCode"] = df["CertificationCode"].fillna("No code")
df["ProjectAddressState"] = df["ProjectAddressState"].fillna("No Address available")
df["ProjectAddressCity"] = df["ProjectAddressCity"].fillna("No Address available")
df["ProjectAddressStreet"] = df["ProjectAddressStreet"].fillna("No Address available")


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

df["BillingRate"]= df["BillingRate"].astype(str)
df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

azure_search_endpoint ="https://vpc-dev-aisearch.search.windows.net"
# credential = AzureKeyCredential("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") if len("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") > 0 else DefaultAzureCredential()
search_service_name = "vpc-dev-aisearch"
azure_openai_endpoint = "https://vpc-dev-azureopenai.openai.azure.com/"
azure_openai_key = "c9ce949e276146d4a66b3e16b32613d3" if len("c9ce949e276146d4a66b3e16b32613d3") > 0 else None
azure_openai_chat_deployment = "gpt-4o"
embedding_model_name = "text-embedding-ada-002"
azure_openai_api_version = "2022-12-01"
search_api_key = "8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u"

# Length of the embedding vector (OpenAI ada-002 generates embeddings of length 1536)
EMBEDDING_LENGTH = 1536

search_service_name = "vpc-dev-aisearch"
index_name = "vpc-sv-deltek-employee-v3-index"

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
                        "name": "ClientID",
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
                        "name": "ProjectID",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "ProjectName",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "RevenueTotal",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": False
                    },
                    {
                        "name": "ProjectStatus",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "ProjectAddressStreet",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "ProjectAddressCity",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "ProjectAddressState",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "ProjectPracticeGroup",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "ProjectBusinessUnit",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "EmployeeID",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "EmployeeName",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "EmployeePosition",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "EmployeePracticeGroupADP",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "EmployeePracticeGroupDeltek",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "EmployeeBusinessUnit",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": True
                    },
                    {
                        "name": "BillingRate",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": True,
                        "facetable": False
                    },
                    {
                        "name": "CertificationCode",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": False
                    },
                    {
                        "name": "NameOfCertification",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": False
                    }
                            
        ],


        "semantic": {
            "configurations": [
                {
                    "name": "azureml-default",
                    "prioritizedFields": {
                        "titleField": {"fieldName": "ClientID"},
                        "prioritizedContentFields": [ 
                                                    {"fieldName": "ClientName"},
                                                    {"fieldName": "Worked"},
                                                    {"fieldName": "ClientAddressCity"},
                                                    {"fieldName": "ClientAddressState"},
                                                    {"fieldName": "ProjectID"},
                                                    {"fieldName": "ProjectName"},
                                                    {"fieldName": "RevenueTotal"},
                                                    {"fieldName": "ProjectStatus"},
                                                    {"fieldName": "ProjectAddressStreet"},
                                                    {"fieldName": "ProjectAddressCity"},
                                                    {"fieldName": "ProjectAddressState"},
                                                    {"fieldName": "ProjectPracticeGroup"},
                                                    {"fieldName": "ProjectBusinessUnit"},
                                                    {"fieldName": "EmployeeID"},
                                                    {"fieldName": "EmployeeName"},
                                                    {"fieldName": "EmployeePosition"},
                                                    {"fieldName": "EmployeePracticeGroupADP"},
                                                    {"fieldName": "EmployeePracticeGroupDeltek"},
                                                    {"fieldName": "EmployeeBusinessUnit"},
                                                    {"fieldName": "BillingRate"},
                                                    {"fieldName": "CertificationCode"},
                                                    {"fieldName": "NameOfCertification"}



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
                    "ClientID": row["ClientID"],
                    "ClientName": row["ClientName"],
                    "Worked": row["Worked"],
                    "ClientAddressCity": row["ClientAddressCity"],
                    "ClientAddressState": row["ClientAddressState"],
                    "ProjectID": row["ProjectID"],
                    "ProjectName": row["ProjectName"],
                    "RevenueTotal": row["RevenueTotal"],
                    "ProjectStatus": row["ProjectStatus"],
                    "ProjectAddressStreet": row["ProjectAddressStreet"],
                    "ProjectAddressCity": row["ProjectAddressCity"],
                    "ProjectAddressState": row["ProjectAddressState"],
                    "ProjectPracticeGroup": row["ProjectPracticeGroup"],
                    "ProjectBusinessUnit": row["ProjectBusinessUnit"],
                    "EmployeeID": row["EmployeeID"],
                    "EmployeeName": row["EmployeeName"],
                    "EmployeePosition": row["EmployeePosition"],
                    "EmployeePracticeGroupADP": row["EmployeePracticeGroupADP"],
                    "EmployeePracticeGroupDeltek": row["EmployeePracticeGroupDeltek"],
                    "EmployeeBusinessUnit": row["EmployeeBusinessUnit"],
                    "BillingRate": row["BillingRate"],
                    "CertificationCode": row["CertificationCode"],
                    "NameOfCertification": row["NameOfCertification"],
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
