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

from pyspark.sql.functions import col, to_timestamp, dense_rank
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import necessary libraries
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark Table Join Example") \
    .getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ADP

# CELL ********************

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "workers_data": f"{lakehouse_path}/br_adp_workers_data_v1",
    "adp_filenumber_data": f"{lakehouse_path}/br_adp_filenumber_data_v1"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load workers_data into a DataFrame
workers_data = spark.read.format("delta").load(table_paths["workers_data"])

# Load adp_filenumber_data into a DataFrame
adp_filenumber_data = spark.read.format("delta").load(table_paths["adp_filenumber_data"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select the required columns
adp_filenumber_data = adp_filenumber_data.select('associateOID', 'payrollFileNumber')

# Rename the columns
adp_filenumber_data = adp_filenumber_data.withColumnRenamed('payrollFileNumber', 'EmployeeCode')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select the required columns
workers_data = workers_data.select(
    'associateOID', 'personName', 'workerStatus',
    'workAssignments_actualStartDate', 
    'workAssignments_jobCode_title',
    'positionID', 'seniorityDate', 'workerType', 'jobTitle', 'workLocation',
    'reportingManager', 'BusinessUnit', 'Department',
    'DistributionListReport', 'OriginCompany', 'ExperienceDate',
    'PracticeGroup'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rename the columns
workers_data = workers_data.withColumnRenamed("associateOID", "associateOID") \
    .withColumnRenamed("personName", "EmployeeName") \
    .withColumnRenamed("workerStatus", "EmploymentStatus") \
    .withColumnRenamed("workAssignments_actualStartDate", "PositionStartDate") \
    .withColumnRenamed("workAssignments_jobCode_title", "Position") \
    .withColumnRenamed("positionID", "EmployeeNumber") \
    .withColumnRenamed("seniorityDate", "VerdantasStartDate") \
    .withColumnRenamed("workerType", "EmployeeBillType") \
    .withColumnRenamed("jobTitle", "Title") \
    .withColumnRenamed("workLocation", "OfficeLocation") \
    .withColumnRenamed("reportingManager", "ReportingManager") \
    .withColumnRenamed("DistributionListReport", "HomeDepartmentEmailList") \
    .withColumnRenamed("OriginCompany", "LegacyCompany") \
    .withColumnRenamed("certificationNameCode_codeValue", "CertificationCode") \
    .withColumnRenamed("certificationName", "NameOfCertification") \
    .withColumnRenamed("payrollFileNumber", "EmployeeCode")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workers_data_cleaned = workers_data.dropna(subset=['Title'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert 'PositionStartDate' to timestamp
workers_data_cleaned = workers_data_cleaned.withColumn('PositionStartDate', to_timestamp(col('PositionStartDate')))

# Define the window specification
window_spec = Window.partitionBy('associateOID').orderBy(col('PositionStartDate').desc())

# Add dense rank based on 'associateOID' and 'PositionStartDate'
workers_data_cleaned = workers_data_cleaned.withColumn('Rank', dense_rank().over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter rows where 'Rank' is 1
workers_final_data = workers_data_cleaned.filter(col('Rank') == 1)

# Drop the 'Rank' column
workers_final_data = workers_final_data.drop('Rank')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_merged = workers_final_data.join(adp_filenumber_data,on='associateOID',how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(table_merged)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_data = workers_final_data.select('EmployeeName','EmployeeNumber','EmployeeBillType','PracticeGroup','Title','BusinessUnit','Department','EmploymentStatus','OfficeLocation')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table_path = 'Tables/mvp_adp_data'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## License Certificate Data

# CELL ********************

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "certi_data": f"{lakehouse_path}/br_deltek_license_certification"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load licensedata into a DataFrame
certi_data = spark.read.format("delta").load(table_paths["certi_data"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

certi_data = certi_data.select('EmployeeID','BusinessUnitDescription','LicenseCertificationDescription',
 'LicenseCertificationState',
 'LicenseCertificationID')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

delta_table_path = 'Tables/mvp_certlice_data'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

certi_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)

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
