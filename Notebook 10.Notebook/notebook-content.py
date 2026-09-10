# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c1440039-52c4-4208-ba78-0d2c13c5c8a3",
# META       "default_lakehouse_name": "VPC_BI_ODBC",
# META       "default_lakehouse_workspace_id": "297572de-b7d7-4285-a88e-1388e2598d4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "c1440039-52c4-4208-ba78-0d2c13c5c8a3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, regexp_replace, lit, min, max, avg, lower

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "employee": f"{lakehouse_path}/br_deltek_employee",
    "EMCompany": f"{lakehouse_path}/br_deltek_EMCompany",
    "EmployeeCustomTabFields": f"{lakehouse_path}/br_deltek_EmployeeCustomTabFields",
    "FW_CustomColumnValuesData": f"{lakehouse_path}/br_deltek_FWCustomColumnValuesData",
    "LD": f"{lakehouse_path}/br_deltek_LD",
    "PR": f"{lakehouse_path}/br_deltek_PR",
    "Clendor": f"{lakehouse_path}/br_deltek_Clendor"
}
# Read tables into DataFrames
df_PR = spark.read.format("delta").load(table_paths["PR"])
df_LD = spark.read.format("delta").load(table_paths["LD"])
df_Cl = spark.read.format("delta").load(table_paths["Clendor"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_show = df_PR[['WBS1',
 'WBS2',
 'WBS3',
 'Name',
 'Fee',
 'Description',
 'LongName',
 'ProjectType',
 'TotalProjectCost']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_show = df_show.filter(col("TotalProjectCost")!=0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_show)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mark = df_show[['WBS1','ClientID']].drop_duplicates().dropna(subset=["ClientID"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_employee_counts = df_mark.groupBy("WBS1").count().filter(col("count") > 1)

# Step 2: Join the original DataFrame with the duplicate counts DataFrame
duplicates_df = df_mark.join(duplicate_employee_counts, "WBS1", "inner").select(df_mark.columns)
display(duplicates_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dup_id = duplicates_df.select("WBS1").distinct().withColumnRenamed("WBS1","dup_pro_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_show.join(dup_id, df_show.WBS1 == dup_id.dup_pro_id, how = "inner").drop("dup_pro_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, regexp_replace, lit, min, max, avg

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "employee": f"{lakehouse_path}/br_deltek_employee",
    "EMCompany": f"{lakehouse_path}/br_deltek_EMCompany",
    "EmployeeCustomTabFields": f"{lakehouse_path}/br_deltek_EmployeeCustomTabFields",
    "FW_CustomColumnValuesData": f"{lakehouse_path}/br_deltek_FWCustomColumnValuesData",
    "LD": f"{lakehouse_path}/br_deltek_LD",
    "CL": f"{lakehouse_path}/br_deltek_Clendor",
    "CRCP": f"{lakehouse_path}/br_deltek_ClendorProjectAssoc",
    "PR": f"{lakehouse_path}/br_deltek_PR"
}
# Read tables into DataFrames
df_CR = spark.read.format("delta").load(table_paths["CL"])
df_CRPR = spark.read.format("delta").load(table_paths["CRCP"])
df_PR = spark.read.format("delta").load(table_paths["PR"])
df_LD = spark.read.format("delta").load(table_paths["LD"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_CR.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_CR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when, sum, regexp_replace, lit, min, max, avg, lower

filtered_df = df_CR.filter(lower(col("Name")).contains("ensafe".lower()))
display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM VPC_Dev_Fablh_data.sv_deltek_adp_merged_v5 LIMIT 1000")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower

# List of specific ClientID values
client_ids = [
    "LG2023_02403",
    "L011561",
    "LG2023_00137",
    "L008966",
    "ADMIN694430419528",
    "ADVCLIENT0511",
    "HL09504"
]

# Filter the DataFrame based on the ClientID column
filtered_df_CR = df_CR.filter(lower(col("ClientID")).isin([client_id.lower() for client_id in client_ids]))

display(filtered_df_CR)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower

# List of specific ClientID values
client_ids = [
    "LG2023_02403",
    "L011561",
    "LG2023_00137",
    "L008966",
    "ADMIN694430419528",
    "ADVCLIENT0511",
    "HL09504"
]

# Filter the DataFrame based on the ClientID column
filtered_df_PR = df_PR.filter(lower(col("ClientID")).isin([client_id.lower() for client_id in client_ids]))

display(filtered_df_PR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unique_proj = filtered_df_PR.select("WBS1").distinct().withColumnRenamed("WBS1","Project_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df_LD = df_LD.join(unique_proj, df_LD.WBS1 == unique_proj.Project_id, how = "inner").drop("Project_id")
display(filtered_df_LD)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df_CR1 = filtered_df_CR[['ClientID',"Name"]].withColumnRenamed("ClientID","ClientID_CR").withColumnRenamed("Name","ClientName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pr_cr_join = filtered_df_PR.join(filtered_df_CR1, filtered_df_PR.ClientID == filtered_df_CR1.ClientID_CR, how = "left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(pr_cr_join)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pr_cr_join.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pr_cr_join_filter = pr_cr_join[["WBS1","ClientID","Fee","TotalProjectCost","ClientName"]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum

# List of columns to group by
groupby_columns = ["ClientID", "ClientName", "WBS1"]

# Group by the specified columns and sum the 'Fee' and 'TotalProjectCost' columns
grouped_df = pr_cr_join_filter.groupBy(groupby_columns) \
    .agg(
        sum(col("Fee")).alias("TotalFee"),
        sum(col("TotalProjectCost")).alias("TotalProjectCostSum")
    )

# Display the result
display(grouped_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Assuming 'grouped_df' is the DataFrame resulting from the previous grouping operation

# Primary key and differing columns
primary_key = ["ClientID", "ClientName"]  # Corrected to match column names in your previous code
differing_columns = ["WBS1", "TotalFee", "TotalProjectCostSum"]

# Group by 'ClientID' and 'Name' and aggregate the differing columns into a list of structs
aggregated_df = grouped_df.groupBy(primary_key).agg(
    F.collect_list(
        F.struct(
            differing_columns[0],  # 'WBS1'
            differing_columns[1],  # 'TotalFee'
            differing_columns[2]   # 'TotalProjectCostSum'
        )
    ).alias("ProjectCostDetails")
)

# Convert the 'ProjectCostDetails' struct to JSON format
json_df = aggregated_df.withColumn(
    "ProjectCostDetails",
    F.to_json("ProjectCostDetails")
)

# Select only 'ClientID', 'Name', and 'ProjectCostDetails' columns
pr_client_det = json_df.select(*primary_key, "ProjectCostDetails")

# Show the result
display(pr_client_det)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df_LD_col = filtered_df_LD[['Employee','WBS1']].drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# new

# CELL ********************

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "PR_rollup": f"{lakehouse_path}/br_deltek_PR_joined",
    "LD_his": f"{lakehouse_path}/br_deltek_ld_alltime_projects",
    "final": f"{lakehouse_path}/sv_deltek_adp_merged_v3",
}
# Read tables into DataFrames
df_PR_rollup = spark.read.format("delta").load(table_paths["PR_rollup"]).withColumnRenamed("WBS1","ProjectID_PR")
df_LD_his = spark.read.format("delta").load(table_paths["LD_his"]).withColumnRenamed("Employee","Employee_LD")
final = spark.read.format("delta").load(table_paths["final"]).withColumnRenamed("WBS1","ProjectID_PR")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_PR_rollup)

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

import pandas as pd
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Define the file path for the source file and target Lakehouse path
source_file_path = "/synfs/nb_resource/builtin/CFGTimeAnalysisHeadings.xlsx"
lakehouse_table_path = "abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/c1440039-52c4-4208-ba78-0d2c13c5c8a3/Tables/CFGTimeAnalysisHeadings"  # Delta format

# Step 1: Read the Excel file as a pandas DataFrame
df_pandas = pd.read_excel(source_file_path)

# Step 2: Convert the pandas DataFrame to a PySpark DataFrame
df_spark = spark.createDataFrame(df_pandas)

# Step 3: Save the DataFrame to the Lakehouse as a Delta table
df_spark.write.format("delta").mode("overwrite").save(lakehouse_table_path)

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

display(df_spark)

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
