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
from delta.tables import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DeltaLakeUpsert") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark session
# spark = SparkSession.builder \
#     .appName("DeltaLakeUpsert") \
#     .getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define JDBC connection parameters
#jdbcHostname = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','arcgis-deltek-sql-server')
jdbcHostname = 'arcgis-sql-east.public.cbcafbd70d9b.database.windows.net'
jdbcPort = 3342  # Replace with your port number if different
jdbcDatabase = 'vpc-vantagepoint'
jdbcUsername = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','arcgis-deltek-sql-username')
jdbcPassword = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','arcgis-deltek-sql-password')
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# Define JDBC connection properties
connectionProperties = {
    
    "user" : jdbcUsername,
    "password" : jdbcPassword,
    "driver" : jdbcDriver
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

EMCompany_df = spark.read.jdbc(url=jdbcUrl, table="[vpc-vantagepoint].[dbo].[VPC_DEV_CORPORATE_INFO.vw_EMCompany]", properties=connectionProperties)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#EMCompany Data

# Load data from Azure SQL Database into a Spark DataFrame
EMCompany_df = spark.read.jdbc(url=jdbcUrl, table="[vpc-vantagepoint].[dbo].[VPC_DEV_CORPORATE_INFO.vw_EMCompany]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_EMCompany"  # Example Delta Lake table path in Azure Data Lake Storage

# # Read target table
#target_df = DeltaTable.forPath(spark, delta_table_path)


# merge_condition =  "target.Employee = source.Employee"

# Write data to Delta Lake table

# target_df.alias("target").merge(
#     EMCompany_df.alias("source"),
#     merge_condition
# ).whenMatchedUpdateAll(
# ).whenNotMatchedInsertAll(
# ).execute()

EMCompany_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
employee_df = spark.read.jdbc(url=jdbcUrl, table="[vpc-vantagepoint].[dbo].[VPC_DEV_CORPORATE_INFO.vw_employee]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_employee"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
employee_df.write.format("delta").mode("overwrite").save(delta_table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
CFGMainData_df = spark.read.jdbc(url=jdbcUrl, table="[vpc-vantagepoint].[dbo].[VPC_DEV_CORPORATE_INFO.vw_CFGMainData]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_CFGMainData"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
CFGMainData_df.write.format("delta").mode("overwrite").save(delta_table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
LD_df = spark.read.jdbc(url=jdbcUrl, table="[vpc-vantagepoint].[dbo].[VPC_DEV_CORPORATE_INFO.vw_LD]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_LD"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
LD_df.write.format("delta").mode("overwrite").save(delta_table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
PR_df = spark.read.jdbc(url=jdbcUrl, table="[vpc-vantagepoint].[dbo].[VPC_DEV_CORPORATE_INFO.vw_PR]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_PR"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
PR_df.write.format("delta").mode("overwrite").save(delta_table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
EmployeeCustomTabFields_df = spark.read.jdbc(url=jdbcUrl, table="[vpc-vantagepoint].[dbo].[VPC_DEV_CORPORATE_INFO.vw_EmployeeCustomTabFields]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_EmployeeCustomTabFields"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
EmployeeCustomTabFields_df.write.format("delta").mode("overwrite").save(delta_table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
FWCustomColumnValuesData_df = spark.read.jdbc(url=jdbcUrl, table="[vpc-vantagepoint].[dbo].[VPC_DEV_CORPORATE_INFO.vw_FWCustomColumnValuesData]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_FWCustomColumnValuesData"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
FWCustomColumnValuesData_df.write.format("delta").mode("overwrite").save(delta_table_path)


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
