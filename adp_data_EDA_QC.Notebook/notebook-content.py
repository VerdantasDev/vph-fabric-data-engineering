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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
    "adp_df": f"{lakehouse_path}/sv_adp_data_v1"
}
# Read tables into DataFrames
adp_df = spark.read.format("delta").load(table_paths["adp_df"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_df = adp_df.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
# pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
adp_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_df.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

missing = round((adp_df.isna().sum()/adp_df.shape[0])*100,2)
missing

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test = adp_df[['licenseID','licenseName','licenseExpirationDate']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test['licenseID'].nunique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test['licenseName'].nunique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test[test['licenseName'].isin(test['licenseName'].unique())]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test['licenseID'].value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test['licenseName'].value_counts().index[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test1 = test[test['licenseName'] == 'Professional Engineer - Pennsylvania']
test1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(test1.shape)
test1['licenseID'].nunique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test[test['licenseID'].isin(test['licenseID'].unique())]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_df[adp_df['EmployeeName'] == 'Derenick, Brian M'][['EmployeeName','licenseID','licenseName']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
# pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
adp_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

missing = round((adp_df.isnull().sum()/adp_df.shape[0])*100,2)
missing = missing[missing.values>0]
missing

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_df.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
