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
from pyspark.sql.functions import lit, col, when

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoadTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "Clendor": f"{lakehouse_path}/br_deltek_Clendor_v2",
    "PR": f"{lakehouse_path}/br_deltek_Project_demo",
    "spo": f"{lakehouse_path}/br_m365_sharepoint_download_file_status",
    "zdrive": f"{lakehouse_path}/br_zdrive_locations",
}
# Read tables into DataFrames
df_spo = spark.read.format("delta").load(table_paths["spo"])
# df_PR = spark.read.format("delta").load(table_paths["PR"])
# df_Clendor = spark.read.format("delta").load(table_paths["Clendor"])
df_z = spark.read.format("delta").load(table_paths["zdrive"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_spo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_z)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df = df_spo.join(df_z, df_spo.display_name == df_z.file_name, how='left')
display(joined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df.filter(df_z.file_name.isNull()).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import regexp_extract

# Add a new column 'extracted_numbers' where the regex '^\d+' is applied to 'Level_5' column
df_z = df_z.withColumn("extracted_numbers", regexp_extract(df_z["Level_5"], r'^\d+', 0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_z)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## zdrive clients join

# CELL ********************

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoadTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "client": f"{lakehouse_path}/sv_deltek_Clients_merged",
    "zdrive": f"{lakehouse_path}/br_zdrive_locations",
}
# Read tables into DataFrames
df_Clients = spark.read.format("delta").load(table_paths["client"])
df_z = spark.read.format("delta").load(table_paths["zdrive"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = df_Clients.filter(df_Clients['ClientFolderID'] == '2160New')
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_test = df_z.select("Level_4","Level_5").distinct()
display(df_test)

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
