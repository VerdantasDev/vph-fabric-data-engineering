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

%run ntbk_deltek_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ntbk_deltek_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, regexp_replace, lit, min, max, avg
from pyspark.sql import functions as F
from pyspark.sql import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = load_table(lakehouse_path, "br_m365_sharepoint_content_ADI")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

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

df_ADP = load_table(lakehouse_path, "sv_adp_data_v1")
df_EMAllCompany = load_table(lakehouse_path, "br_deltek_EMAllCompany_v3")
df_EmployeeCustomTabFields = load_table(lakehouse_path, "br_deltek_EmployeeCustomTabFields_v3")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# employee_numbers = [
#     "V6A080707", "V6A080365", "V6A001416", "V6A030651", "V6A001392", 
#     "V6A001378", "V6A001374", "V6A001373", "V6A001381", "V6A001386", 
#     "V6A200001", "V6A001486", "V6A020343"
# ]

# # Filter DataFrame based on EmployeeNumber values
# filtered_df = df_ADP.filter(df_ADP['EmployeeNumber'].isin(employee_numbers))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df_EMAllCompany.withColumn("is_duplicate", F.count("Employee").over(Window.partitionBy("Employee")) > 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_duplicates = df.filter(F.col("is_duplicate"))
df_non_duplicates = df.filter(~F.col("is_duplicate"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.partitionBy("Employee")

# Step 3: Add a column to mark if `EmployeeCompany` has "001" for any of the rows
df_with_has_001 = df_duplicates.withColumn(
    "has_001", F.max(F.when(F.col("EmployeeCompany") == "001", 1).otherwise(0)).over(window_spec)
)

# Step 4: Propagate the `has_001` flag as `True` for all rows if any row has "001"
df_with_has_001 = df_with_has_001.withColumn(
    "has_001", F.col("has_001") == 1
)

# Step 5: Separate DataFrames based on `has_001` column
df_has_001 = df_with_has_001.filter(F.col("has_001"))
df_no_001 = df_with_has_001.filter(~F.col("has_001"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_001_filtered = df_has_001.filter(F.col("EmployeeCompany") == "001")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_non_duplicates.unionByName(df_001_filtered.drop('has_001'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_EmployeeCustomTabFields = df_EmployeeCustomTabFields.withColumnRenamed('Employee','EmployeeCust')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final2 = df_final.join(df_EmployeeCustomTabFields, df_final['Employee'] == df_EmployeeCustomTabFields['EmployeeCust'], how = 'inner')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_select = df_final2.select("Employee", 'Select3', 'Title')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

code_mapping = {
    "009": "Geological and Geotechnical Engineering",
    "012": "Process Engineering",
    "013": "Site and Roadway Engineering",
    "015": "Shared Services",
    "006": "Natural Resources and Environmental Planning",
    "011": "Mechanical, Electrical, and Automation Engineering",
    "003": "Environmental Assessment & Remediation",
    "008": "Environmental Health and Safety",
    "014": "Structural Engineering and Architecture",
    "010": "Hydrology, Hydraulics, and Fluids",
    "007": "Sustainability",
    "002": "Precise Visual Technologies / Applied Data and Technology"
}

code_mapping_broadcast = spark.sparkContext.broadcast(code_mapping)

# Define a UDF that uses the broadcasted dictionary
def map_code_to_name(code):
    return code_mapping_broadcast.value.get(code, code)

# Register the UDF in Spark
map_code_to_name_udf = F.udf(map_code_to_name, StringType())

# Apply the UDF to the 'Code' column
df_final2 = df_final2.withColumn("EmployeePracticeGroupDeltek", map_code_to_name_udf(F.col("Select3")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

code_mapping = {
    "009": "Geological and Geotechnical Engineering",
    "012": "Process Engineering",
    "013": "Site and Roadway Engineering",
    "015": "Shared Services",
    "006": "Natural Resources and Environmental Planning",
    "011": "Mechanical, Electrical, and Automation Engineering",
    "003": "Environmental Assessment & Remediation",
    "008": "Environmental Health and Safety",
    "014": "Structural Engineering and Architecture",
    "010": "Hydrology, Hydraulics, and Fluids",
    "007": "Sustainability",
    "002": "Precise Visual Technologies / Applied Data and Technology"
}

code_mapping_broadcast = spark.sparkContext.broadcast(code_mapping)

# Define a UDF that uses the broadcasted dictionary
def map_code_to_name(code):
    return code_mapping_broadcast.value.get(code, code)

# Register the UDF in Spark
map_code_to_name_udf = F.udf(map_code_to_name, StringType())

# Apply the UDF to the 'Code' column
df_select = df_select.withColumn("EmployeePracticeGroupDeltek", map_code_to_name_udf(F.col("Select3")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ADP = df_ADP.withColumn("EmployeeNumber1", regexp_replace("EmployeeNumber", "^V6A0*", ""))
df_final2 = df_final2.withColumn("Employee1", regexp_replace("Employee", "^'?0*", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_merged = df_final2.join(df_ADP.withColumnRenamed("Title","Title_ADP"), df_final2['Employee1'] == df_ADP['EmployeeNumber1'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_merged1 = df_merged[[
#  'EmployeeName',
#  'Employee',
#  'Title',
#  'Title_ADP',
#  'EmployeePracticeGroupDeltek',
#  'PracticeGroup',
#  'Position',
#  'Status',
#  'Type',
#  'EmploymentStatus',
#  'PositionStartDate']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List of tuples with old column names and new column names
columns_to_rename = [
    ("EmployeeName", "EmployeeName"),
    ("Employee", "EmployeeID"),
    ("Title", "DeltekTitle"),
    ("Title_ADP", "ADPTitle"),
    ("EmployeePracticeGroupDeltek", "DeltekPracticeGroup"),
    ("PracticeGroup", "ADPPracticeGroup"),
    ("Position", "ADPPosition"),
    ("Status", "Status"),
    ("EmploymentStatus", "EmploymentStatus"),
    ("PositionStartDate", "PositionStartDate")
]

# Apply the renaming
for old_name, new_name in columns_to_rename:
    df_merged = df_merged.withColumnRenamed(old_name, new_name)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_merged)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_merged1 = df_merged[['EmployeeID',
 'LastName',
 'FirstName',
 'MiddleName',
 'DeltekPracticeGroup',
 'EmployeeName',
 'EmployeeNumber',
 'ADPPracticeGroup']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_merged1, lakehouse_path, "br_employee_deltek_adp_merged", overwriteSchema = True)

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
