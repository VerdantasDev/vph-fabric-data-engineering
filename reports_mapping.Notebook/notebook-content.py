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

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "PR": f"{lakehouse_path}/br_deltek_PR",
    "Clendor": f"{lakehouse_path}/br_deltek_Clendor",
    "CL": f"{lakehouse_path}/br_deltek_CL",
}
# Read tables into DataFrames
df_PR = spark.read.format("delta").load(table_paths["PR"])
df_Clendor = spark.read.format("delta").load(table_paths["Clendor"])
df_CL = spark.read.format("delta").load(table_paths["CL"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower, lit
f_df = df_Clendor.filter(lower(col("Name")).contains(lower(lit("STATE OF RHODE ISLAND"))))
display(f_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower
search_strings = ["GEO4059","ES00261"]
filtered_df1 = df_PR.filter(
    (lower(col("ClientID")).isin([lower(lit(s)) for s in search_strings])) &
    (col("WBS1").contains(".P"))
)
filtered_df2 = filtered_df1.filter(col("StartDate") >= '2020-01-01')
display(filtered_df2.select("ClientID").distinct())
# display(filtered_df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower
search_strings = ["ADVCLIENTC0165","3BD4071C7FCF44FC984F4073D679F522","ma3E812810564B0E9FA04375A7FF10D0","EF09463B5C3A4EE888D6100ED5415A1C"]
filtered_df1 = df_PR.filter(lower(col("ClientID")).isin([lower(lit(s)) for s in search_strings]))
filtered_df2 = filtered_df1.filter(col("StartDate") >= '2020-01-01')
display(filtered_df2.select("ClientID").distinct())
# display(filtered_df1)

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
