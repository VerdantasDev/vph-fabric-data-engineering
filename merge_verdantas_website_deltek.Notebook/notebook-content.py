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
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "V1": f"{lakehouse_path}/sv_verdantas_website_data",
    "PR": f"{lakehouse_path}/br_deltek_Project_merge_test",
    "PR_ori": f"{lakehouse_path}/br_deltek_PR_v2"
}
# Read tables into DataFrames
df_web = spark.read.format("delta").load(table_paths["V1"])
df_PR = spark.read.format("delta").load(table_paths["PR"])
df_PR_ori = spark.read.format("delta").load(table_paths["PR_ori"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfw = df_web.drop("service_content","market_content","solution_content","project_content","expertise_content")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Group by 'project_name' and aggregate all columns by concatenating distinct values with ';' separator
grouped_df = dfw.groupBy("project_name").agg(
    *[
        F.concat_ws(";", F.array_distinct(F.collect_list(col))).alias(col)
        for col in dfw.columns if col != "project_name"
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(grouped_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower, lit
from pyspark.sql import functions as F
from functools import reduce

word_list = ['modular and scalable']

# Create a filter condition using `contains()` for all words in the list
filter_condition = reduce(
    lambda acc, word: acc & F.lower(col("Name")).contains(F.lower(F.lit(word))),
    word_list,
    F.lit(True)  # Start with True as the accumulator
)

# Apply the filter condition to the DataFrame
f_df = df_PR.filter(filter_condition)

display(f_df.select("WBS1","Name","LongName","ProjectType"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower, lit
df1 = df_PR_ori.filter(lower(col("Name")).contains(lower(lit("bald eagle"))))
display(df1)

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
