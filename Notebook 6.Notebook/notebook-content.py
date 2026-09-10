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
from pyspark.sql.functions import regexp_replace, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoadTables") \
    .getOrCreate()

    # Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "EMCompany": f"{lakehouse_path}/br_deltek_EMCompany",
    "EMCompany_v2": f"{lakehouse_path}/br_deltek_trans_EMCompany_v3",
}

df = spark.read.format("delta").load(table_paths["EMCompany"])
df2 = spark.read.format("delta").load(table_paths["EMCompany_v2"])

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

display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum, when

null_counts = df2.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df2.columns])
display(null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df2.filter(df2.ADPFileNumber.isNull()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum, when

null_counts = df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])
display(null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Table Join

# CELL ********************

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "EMCompany": f"{lakehouse_path}/br_deltek_trans_EMCompany_v3",
    "ADP": f"{lakehouse_path}/sv_adp_deltek_data"
}
# Read tables into DataFrames
df_EMCompany_trans = spark.read.format("delta").load(table_paths["EMCompany"]).withColumnRenamed("Employee", "Employee_EMCompany")
df_ADP_trans = spark.read.format("delta").load(table_paths["ADP"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ADP_trans = df_ADP_trans.withColumn("EmployeeNumber", regexp_replace("EmployeeNumber", "^V6A0*", ""))
df_ADP_trans = df_ADP_trans.withColumn("EmployeeCode", regexp_replace("EmployeeCode", "^'?0*", ""))
df_EMCompany_trans = df_EMCompany_trans.withColumn("Employee_EMCompany", regexp_replace("Employee_EMCompany", "^'?0*", ""))
df_EMCompany_trans = df_EMCompany_trans.withColumn("ADPFileNumber", regexp_replace("ADPFileNumber", "^'?0*", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_emcompany_join = df_ADP_trans.alias("a") \
    .join(df_EMCompany_trans.alias("b"), df_ADP_trans["EmployeeNumber"] == df_EMCompany_trans["Employee_EMCompany"], how="left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
