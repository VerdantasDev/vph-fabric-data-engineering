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
from pyspark.sql.functions import col,when,sum

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
    "CGFMain": f"{lakehouse_path}/br_m365_sharepoint_download_file_status",
}
# Read tables into DataFrames
df_CFGmain = spark.read.format("delta").load(table_paths["CGFMain"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_CFGmain)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_CFGmain)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM VPC_Dev_Fablh_data.br_m365_sharepoint_download_file_status LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cfg_fields = {
    'Company': 'company_id',
    'FirmName': 'company_name',
    'MaxHourlyRate':'maximum_hourly_employee_rate',
    'PayrollEnabled':'payroll_enabled_flag',
    'JobCostFrequency':'payroll_frequency'
}

# Rename columns
for old_name, new_name in cfg_fields.items():
    df_CFGmain = df_CFGmain.withColumnRenamed(old_name, new_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_CFGmain)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select columns and create an aggregation expression for each column to count null values
null_counts = df_CFGmain.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_CFGmain.columns])

# Show the results
null_counts.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

value_counts = df_CFGmain.groupBy('payroll_enabled_flag').count()

# Show the results
value_counts.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_CFGmain = df_CFGmain.withColumn('payroll_enabled_flag',
    when(col('payroll_enabled_flag') == 'Y', 'yes').when(col('payroll_enabled_flag') == 'N', 'no')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

value_counts = df_CFGmain.groupBy('payroll_frequency').count()

# Show the results
value_counts.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_CFGmain = df_CFGmain.withColumn('payroll_frequency',
    when(col('payroll_frequency') == 'W', 'weekly').when(col('payroll_frequency') == 'B', 'biweekly').when(col('payroll_frequency') == 'M', 'monthly').when(col('payroll_frequency') == 'S', 'semimonthly')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_CFGmain)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_CFGmain.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_trans_CFGmain" 

# Write data to Delta Lake table
df_CFGmain.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
