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
from pyspark.sql.functions import regexp_extract, substring, regexp_replace
from pyspark.sql.functions import lit, col, when, lower, trim, count
from pyspark.sql import functions as F

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "PR": f"{lakehouse_path}/br_deltek_Project_demo",
    "zdrive":f"{lakehouse_path}/br_zdrive_locations",
    "m365": f"{lakehouse_path}/br_m365_sharepoint_download_file_status_v2",
    "pr_cust": f"{lakehouse_path}/br_deltek_ProjectCustomTabFields_v3",
    "Clendor": f"{lakehouse_path}/br_deltek_Clendor_v2",
    }
# Read tables into DataFrames
df_PR = spark.read.format("delta").load(table_paths["PR"])
df_z = spark.read.format("delta").load(table_paths["zdrive"])
df_m365 = spark.read.format("delta").load(table_paths["m365"])
df_PRCust = spark.read.format("delta").load(table_paths["pr_cust"])
df_Clendor = spark.read.format("delta").load(table_paths["Clendor"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Separated unique and duplicates from PR Custom Tab Fields
wbs1_counts = df_PRCust.groupBy("WBS1").agg(count("WBS1").alias("count"))

duplicate_wbs1 = wbs1_counts.filter(col("count") > 1).select("WBS1")
unique_wbs1 = wbs1_counts.filter(col("count") == 1).select("WBS1")

df_PRCust_duplicates = df_PRCust.join(duplicate_wbs1, on="WBS1", how="inner")
df_PRCust_unique = df_PRCust.join(unique_wbs1, on="WBS1", how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# for duplicate removed the rows where WBS2 and WBS3 is null
df_PRCust_filtered = df_PRCust_duplicates.filter(
    (trim(col("WBS2")).isNull() | (trim(col("WBS2")) == "")) &
    (trim(col("WBS3")).isNull() | (trim(col("WBS3")) == ""))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Combined both
df_PRCust_combined = df_PRCust_unique.unionByName(df_PRCust_filtered)
df_PRCust_selected = df_PRCust_combined.select("WBS1","CustClientFolderID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Joined PR with ProjectCustomTabFields
df_PR_joined = df_PR.join(df_PRCust_selected, df_PR["ProjectID_PR"] == df_PRCust_selected["WBS1"], "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_PR_joined.select("ProjectID_PR").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtered out rows from z-drive where filename is empty
df_z_filtered = df_z.filter(
    (df_z["file_name"] != "") & df_z["file_name"].isNotNull()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_z_filtered = df_z_filtered.withColumn("extracted_numbers", regexp_extract(df_z["Level_5"], r'(^.*?)(\s|$)', 1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_z_filtered = df_z_filtered.withColumn("project_last_five_numbers", substring(col("extracted_numbers"), -5, 5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_z_filtered.filter(
    col("extracted_numbers").rlike(r'[A-Za-z\.]')
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_empty_z_last_five_numbers = df_z_filtered.filter(
    (df_z_filtered["project_last_five_numbers"] == "") | df_z_filtered["project_last_five_numbers"].isNull()
)
display(df_empty_z_last_five_numbers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_PR_joined = df_PR_joined.withColumn("project_last_five_numbers", regexp_extract(df_PR_joined["ProjectID_PR"], r'([a-zA-Z0-9]{5})$', 0))
# df_PR_joined = df_PR_joined.withColumn("project_last_five_numbers", regexp_replace(df_PR_joined["project_last_five_numbers"], "\-", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_empty_project_last_five_numbers = df_PR_joined.filter(
    (df_PR_joined["project_last_five_numbers"] == "") | df_PR_joined["project_last_five_numbers"].isNull()
)
display(df_empty_project_last_five_numbers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = dict()
summary['Unique projects in original PR table'] = df_PR.select("ProjectID_PR").distinct().count()
summary['Unique projects in cleaned PR table'] = df_PR_joined.select("ProjectID_PR").distinct().count()
summary['Unique projects in original Z drive'] = df_z.select("Level_5", "Level_4").distinct().count()
summary['Unique projects in cleaned Z drive'] = df_z_filtered.select("Level_5", "Level_4").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

z_PR_df = df_z_filtered.join(
    df_PR_joined,
    (df_z_filtered["project_last_five_numbers"] == df_PR_joined["project_last_five_numbers"]) &
    (F.lower(df_z_filtered["Client_Folder_ID"]) == F.lower(df_PR_joined["CustClientFolderID"])),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

z_PR_Cl_df = z_PR_df.join(df_Clendor, z_PR_df["ClientID_PR"] == df_Clendor["ClientID"], "left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

z_PR_Cl_m365_df = z_PR_Cl_df.join(df_m365, z_PR_Cl_df["file_name"] == df_m365["display_name"], "left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

z_PR_Cl_m365_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(z_PR_Cl_m365_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matched_count = z_PR_Cl_m365_df.filter(z_PR_Cl_m365_df["WBS1"].isNotNull()).select("ProjectID_PR").distinct().count()
matched_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matched_count1 = z_PR_Cl_m365_df.filter(z_PR_Cl_m365_df["Level_5"].isNotNull()).select("ProjectID_PR").distinct().count()
matched_count1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Unique projects in PR table mapped with Z drive'] = matched_count1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matched_count2 = z_PR_Cl_m365_df.filter(z_PR_Cl_m365_df["Client_Folder_ID"].isNotNull()).select("Client_Folder_ID").distinct().count()
matched_count2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Unique ClientFolderIds in PR table mapped with Z drive'] = matched_count2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matched_count3 = z_PR_Cl_m365_df.filter(z_PR_Cl_m365_df["Client_Folder_ID"].isNotNull()).select("file_name").distinct().count()
matched_count3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Unique files in Z drive mapped with PR table'] = matched_count3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matched_count4 = z_PR_Cl_m365_df.filter(z_PR_Cl_m365_df["Client_Folder_ID"].isNotNull()).select("ClientID").distinct().count()
matched_count4

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Unique clients mapped with Z drive files'] = matched_count4

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matched_count5 = z_PR_Cl_m365_df.filter(z_PR_Cl_m365_df["display_name"].isNotNull()).select("ProjectID_PR").distinct().count()
matched_count5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['SharePoint files mapped with Z drive files'] = matched_count5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matched_count6 = z_PR_Cl_m365_df.filter(z_PR_Cl_m365_df["display_name"].isNotNull()).select("display_name").distinct().count()
matched_count6

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_m365)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = z_PR_Cl_m365_df.filter(z_PR_Cl_m365_df["display_name"].isNotNull())
display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from itertools import chain
from pyspark.sql.types import StringType

# Define the mapping dictionary
project_type_mapping = {
    "NECS01": "General building",
    "NECS02": "Industrial process",
    "NECS03": "Manufacturing",
    "NECS04": "Water Supply",
    "NECS05": "Sewage / Solid Waste Disposal",
    "NECS06": "Transportation",
    "NECS08": "Power",
    "NECS09": "Petroleum",
    "NECS10": "telecomunucation",
    "NECS11": "Other",
    "NECS12": "Hazardous waste",
    "Envi0024": "Environment"
}

code_mapping_broadcast = spark.sparkContext.broadcast(project_type_mapping)

# Define a UDF that uses the broadcasted dictionary
def map_code_to_name(code):
    return code_mapping_broadcast.value.get(code, "Unknown Code")

# Register the UDF in Spark
map_code_to_name_udf = F.udf(map_code_to_name, StringType())

# Apply the UDF to the 'Code' column
final_df = final_df.withColumn("ProjectType_PR", map_code_to_name_udf(F.col("ProjectType_PR")))
display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extra Codes

# CELL ********************

from pyspark.sql.functions import length

df_z_filtered_4_char = df_z_filtered.filter(
    (length(df_z_filtered["extracted_numbers"]) <= 4) & (df_z_filtered["extracted_numbers"].isNotNull())
)
display(df_z_filtered_4_char)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

value_to_filter = "f0000000.ptd"

# Filter out rows where WBS1 is equal to the specific value
df_f = df_z_filtered.filter(F.col("file_name") == value_to_filter)

# Show the results
display(df_f)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Define a condition to check if a column is null or empty after trimming
def is_null_or_empty(column):
    return (F.col(column).isNull()) | (F.trim(F.col(column)) == "")

# Apply the condition to filter out rows
df_PRCust_filtered = df_PRCust_combined.filter(
    (is_null_or_empty("CustClientFolderID"))
)
display(df_PRCust_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Step 1: Identify duplicate WBS1 values
df_duplicates = df_PRCust.groupBy("WBS1").agg(
    F.count("*").alias("count")
).filter(F.col("count") > 1)

# Step 2: Join with the original DataFrame to get the rows with duplicate WBS1 values
df_PRCust_duplicates = df_PRCust.join(df_duplicates, on="WBS1", how="inner")
display(df_PRCust_duplicates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Step 1: Identify `extracted_numbers` and `Client_Folder_ID` with different `Level_5` values
df_groups = df_z_filtered.groupBy("extracted_numbers", "Client_Folder_ID").agg(
    F.countDistinct("Level_5").alias("distinct_levels")
).filter(F.col("distinct_levels") > 1)

# Step 2: Join this result back with the original DataFrame to get the desired rows
df_result = df_z_filtered.join(df_groups, on=["extracted_numbers", "Client_Folder_ID"], how="inner")
display(df_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_PR = df_PR.withColumn("proposal_flag", when(col("ProjectID_PR").contains(".P"), lit("yes")).otherwise(lit("no")))
df_proposal = df_PR.filter(col("proposal_flag").isin("yes"))
distinct_count = proposal_z_df.select("ProjectID_PR").distinct().count()
distinct_count
distinct_count = PR_z_df.select("ProjectID_PR").distinct().count()
distinct_count = no_proposal_z_df.select("ProjectID_PR").distinct().count()
distinct_count
no_proposal_z_df = df_wo_proposal.join(df_z, df_wo_proposal["project_last_five_numbers"] == df_z["project_last_five_numbers"], "inner")
distinct_count = df_wo_proposal.select("ProjectID_PR").distinct().count()
distinct_count
df_wo_proposal = df_PR.filter(col("proposal_flag").isin("no"))
distinct_count = df_proposal.select("ProjectID_PR").distinct().count()
distinct_count
proposal_z_df = df_proposal.join(df_z, df_proposal["project_last_five_numbers"] == df_z["project_last_five_numbers"], "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
