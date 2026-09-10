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

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,sum

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/bronze/adp/v1/2024/08/24/workers_data.csv")
# df now is a Spark DataFrame containing CSV data from "Files/bronze/adp/v1/2024/08/22/workers_data.csv".
display(df)

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

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "resume": f"{lakehouse_path}/br_m365_sharepoint_file_content_ADI",
    "ADP": f"{lakehouse_path}/sv_adp_data_v1",
}
# Read tables into DataFrames
df_resume = spark.read.format("delta").load(table_paths["resume"])
df_ADP = spark.read.format("delta").load(table_paths["ADP"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_ADP)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ADP_scope = df_ADP.select("EmployeeName","EmployeeNumber").drop_duplicates().toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_resume.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_file_name = df_resume.select("filename")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_file_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
filtered_df = df_file_name.filter(F.lower(F.col("filename")).contains("resume"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = filtered_df.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import numpy as np
import re

patterns = ['([A-Za-z]+),\s([A-Za-z]+)',
    r'resume_([A-Za-z]+),?\s([A-Za-z]+)', 
    r'([A-Za-z]+)_([A-Za-z]+)_resume', 
    r'resume_([A-Za-z]+)_([A-Za-z]+)',   
    r'resume\s-\s([A-Za-z]+)\s([A-Za-z]+)',
    r'([A-Za-z]+)\s([A-Za-z]+)\sresume',
    r'^([A-Za-z]+)\s([A-Za-z]+)(?:_|-).*?resume',
    r'resume-([A-Za-z]+)-([A-Za-z]+)'
]

# Function to apply regex patterns and extract matches
def apply_patterns(filename):
    for pattern in patterns:
        match = re.search(pattern, filename, re.IGNORECASE)
        if match:
            return ' '.join(match.groups())  # Combine matched groups
    return None

# Apply function to create a new column with combined results
df['combined_results'] = df['filename'].apply(apply_patterns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df[['name1', 'name2']] = df['combined_results'].str.split(' ', n=1, expand=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def find_matches(name1, name2, df_ADP, file_name):
    matches = []
    for _, adp_row in df_ADP.iterrows():
        employee_name = adp_row['EmployeeName']
        if pd.notna(name1) and pd.notna(name2):
            if name1.lower() in employee_name.lower() and name2.lower() in employee_name.lower():
                matches.append((employee_name, adp_row['EmployeeNumber'], file_name))
    return matches

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expanded_rows = []
for _, row in df.iterrows():
    matches = find_matches(row['name1'], row['name2'], df_ADP_scope, row['filename'])
    if matches:
        for match in matches:
            expanded_rows.append({'filename': row['filename'], 'name1': row['name1'], 'name2': row['name2'],
                                  'EmployeeName': match[0], 'EmployeeNumber': match[1]})
    else:
        expanded_rows.append({'filename': row['filename'], 'name1': row['name1'], 'name2': row['name2'],
                              'EmployeeName': pd.NA, 'EmployeeNumber': pd.NA})
result_df = pd.DataFrame(expanded_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = result_df.rename(columns={"filename": "filename2", "EmployeeNumber": "EmployeeNumber2"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_res = df_resume.filter(F.lower(F.col("filename")).contains("resume"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_res = df_res.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ADP = df_ADP.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Join df_res with df_result on df_res.filename and df_result.filename2
joined_df = pd.merge(df_res, result_df, left_on="filename", right_on="filename2", how = 'inner')

# Step 2: Join the resulting DataFrame with df_ADP on df_ADP.EmployeeNumber and joined_df.EmployeeNumber2
final_df = pd.merge(joined_df, df_ADP, left_on="EmployeeNumber2", right_on="EmployeeNumber", how = 'left')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = final_df.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df2 = final_df.drop(columns = ['content', 'CertificationCode','NameOfCertification','licenseName','licenseID','licenseExpirationDate'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df2 = final_df2.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("Sharepoint") \
    .getOrCreate()
spark_df = spark.createDataFrame(final_df)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_resume_adp_merge_sample"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
