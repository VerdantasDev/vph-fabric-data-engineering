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
 
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LakehouseTables") \
    .getOrCreate()
 
# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "merged": f"{lakehouse_path}/sv_deltek_adp_merged_v5",
}
# Read tables into DataFrames
df_merged = spark.read.format("delta").load(table_paths["merged"])
display(df_merged)

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
