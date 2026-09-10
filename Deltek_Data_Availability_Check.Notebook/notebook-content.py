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
from pyspark.sql.functions import regexp_replace
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables


lakehouse_path = "Tables"
table_paths = {
    "deltek": f"{lakehouse_path}/br_deltek_DemoData_v3",
    "license":f"{lakehouse_path}/br_deltek_license_certification"
}
# Read tables into DataFrames
license1 = spark.read.format("delta").load(table_paths["license"])
deltek = spark.read.format("delta").load(table_paths["deltek"])
deltek_df = deltek.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltek_df = deltek.toPandas()
license_df = license1.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltek_df.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

license_df.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltek_df.drop(columns=['LicenseCertificationDescription','LicenseCertificationState', 'LicenseCertificationID'],inplace=True)
deltek_df.columns


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

license_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

license_df = license_df[['EmployeeID', 'LicenseCertificationDescription', 'LicenseCertificationState', 'LicenseCertificationID']]
license_df['LicenseCertificationState'].fillna('NA', inplace=True)
license_df['LicenseCertificationID'].fillna('NA', inplace=True)

# Aggregating the data into lists
license_agg_df = license_df.groupby('EmployeeID').agg({
    'LicenseCertificationDescription': lambda x: list(x), 
    'LicenseCertificationState': lambda x: list(x),        
    'LicenseCertificationID': lambda x: list(map(str, x)) 
}).reset_index()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

print(deltek_df.shape[0])
print(license_agg_df.shape[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
final_df = pd.merge(deltek_df, license_agg_df, on='EmployeeID', how='left')
final_df.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
final_df.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.isna().sum()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df['PracticeGroupDeltek'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df[final_df['LicenseCertificationID'].isna()]['EmployeeID'].nunique()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df[~final_df['LicenseCertificationID'].isna()]['EmployeeID'].nunique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df['EmployeeID'].nunique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df[final_df['ProjectID']== 'NA']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df[final_df['ClientID'].isin(['NA'])]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df['ClientID'].nunique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.groupby('EmployeeID')['ClientID'].nunique().sort_values(ascending=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.groupby('EmployeeID')['ProjectID'].nunique().sort_values(ascending=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df[final_df['ClientID'] == 'WATCON_103123_000173']['EmployeeID'].values

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

clientid, projectid, employeeid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.groupby('ClientID')['EmployeeID'].nunique().sort_values(ascending=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.groupby('ClientID')['ProjectID'].nunique().sort_values(ascending=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df[final_df['ClientID'] == 'LG2023_21858']['ProjectID'].values

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df['ResumeURL'].isna().sum()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df['ResumeURL'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df["ResumeURL"].sample(50)

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
