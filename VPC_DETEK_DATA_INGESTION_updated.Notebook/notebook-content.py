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

# Define JDBC connection parameters
#jdbcHostname = mssparkutils.credentials.getSecret('https://vpc-dev-keyvault.vault.azure.net/','arcgis-deltek-sql-server')
jdbcHostname = 'arcgis-sql-east.public.cbcafbd70d9b.database.windows.net'
jdbcPort = 3342  # Replace with your port number if different
jdbcDatabase = 'VPC-Vantagepoint3'
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

# Load data from Azure SQL Database into a Spark DataFrame
LD_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_LD]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_LD_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
LD_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
FWCustomColumnValuesData_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_FWCustomColumnValuesData]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_FWCustomColumnValuesData_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
FWCustomColumnValuesData_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
EmployeeCustomTabFields_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_EmployeeCustomTabFields]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_EmployeeCustomTabFields_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
EmployeeCustomTabFields_df.write.format("delta").mode("overwrite").save(delta_table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
ProjectCustomTabFields_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_ProjectCustomTabFields]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_ProjectCustomTabFields_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
ProjectCustomTabFields_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
CFGMainData_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_CFGMainData]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_CFGMainData_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
CFGMainData_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
CFGTimeAnalysis_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_CFGTimeAnalysis]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_CFGTimeAnalysis_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
CFGTimeAnalysis_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
CFGTimeAnalysisHeadingsData_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_CFGTimeAnalysisHeadingsData]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_CFGTimeAnalysisHeadingsData_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
CFGTimeAnalysisHeadingsData_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
EMAllCompany_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_EMAllCompany]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_EMAllCompany_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
EMAllCompany_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# Load data from Azure SQL Database into a Spark DataFrame
LedgerAR_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_LedgerAR]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_LedgerAR_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
LedgerAR_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
CFGEmployeeTypeDescriptions_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_CFGEmployeeTypeDescriptions]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_CFGEmployeeTypeDescriptions_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
CFGEmployeeTypeDescriptions_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
CFGClientTypeDescriptions_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_CFGClientTypeDescriptions]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_CFGClientTypeDescriptions_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
CFGClientTypeDescriptions_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
AR_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_AR]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_AR_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
AR_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
PR_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_PR]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_PR_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
PR_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
EMCompany_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_EMCompany]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_EMCompany_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
EMCompany_df.write.format("delta").mode("overwrite").save(delta_table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
EMPayroll_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_EMPayroll]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_EMPayroll_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
EMPayroll_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
V_UnbilledDetailData_BI_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_V_UnbilledDetailData_BI]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_V_UnbilledDetailData_BI_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
V_UnbilledDetailData_BI_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
V_RevRecDetailData_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_V_RevRecDetailData]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_V_RevRecDetailData_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
V_RevRecDetailData_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
ADPCode_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.ADPCode]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_ADPCode_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
ADPCode_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
Clendor_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.Clendor]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_Clendor_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
Clendor_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
ClendorProjectAssoc_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_ClendorProjectAssoc]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_ClendorProjectAssoc_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
ClendorProjectAssoc_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
CLAddress_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_CLAddress]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_CLAddress_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
CLAddress_df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from Azure SQL Database into a Spark DataFrame
Clients_ClientFolderList_df = spark.read.jdbc(url=jdbcUrl, table="[VPC-Vantagepoint3].[dbo].[VPC_DEV_CORPORATE_INFO.vw_Clients_ClientFolderList]", properties=connectionProperties)

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_Clients_ClientFolderList_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
Clients_ClientFolderList_df.write.format("delta").mode("overwrite").save(delta_table_path)

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
