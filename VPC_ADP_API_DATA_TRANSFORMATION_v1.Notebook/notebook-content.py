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
# META       "default_lakehouse_workspace_id": "297572de-b7d7-4285-a88e-1388e2598d4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "24113e54-6f3f-4157-8c30-a3c5e66de623"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
import ast
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime, timedelta


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Lakehouse Update and Append") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dates
today = datetime.today()
today_str = today.strftime("%Y-%m-%d")
year_str = today.year
month_str = today.strftime("%m")
day_str = today.strftime("%d")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

day_str = '09'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Paths to your Delta table and input file
br_worker_delta_table_path = "Tables/br_adp_workers_data"
br_worker_input_file_path = f"Files/bronze/adp/v1/{year_str}/{month_str}/{day_str}/workers_data.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the new data from the file
worker_data_df = spark.read.csv(br_worker_input_file_path, header=True)
workers_data = worker_data_df.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workers_data['workAssignments'] = workers_data['workAssignments'].apply(ast.literal_eval)
workers_data_dic =  workers_data.to_dict(orient='records')
worker_final_results = []
for wdd in workers_data_dic:
    for workAssignment in wdd.get('workAssignments', []):
        update_dict= dict()
        update_dict = {
            'associateOID': wdd.get('associateOID'),
            'personName': wdd.get('person.legalName.formattedName'),
            'workerId': wdd.get('workerID.idValue', {}),
            'workerStatus': wdd.get('workerStatus.statusCode.codeValue', {}),
            'workAssignments_actualStartDate': workAssignment.get('actualStartDate'),
            'workAssignments_jobCode_CodeValue': workAssignment.get('jobCode', {}).get('codeValue'),
            'workAssignments_jobCode_title': workAssignment.get('jobCode', {}).get('longName', workAssignment.get('jobCode', {}).get('shortName')),
            'workAssignments_managementPositionIndicator': workAssignment.get('managementPositionIndicator')
        }
        worker_final_results.append(update_dict)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transformed_workers_data = pd.DataFrame(worker_final_results)
transformed_workers_data.drop_duplicates(inplace=True)
# Convert date column to datetime
transformed_workers_data['workAssignments_actualStartDate'] = pd.to_datetime(transformed_workers_data['workAssignments_actualStartDate']).dt.date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_df = spark.createDataFrame(transformed_workers_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# merge_condition =  "target.associateOID = source.associateOID"
# br_workers_table_df.alias("target").merge(
#     source_df.alias("source"),
#     merge_condition
# ).whenMatchedUpdateAll(
# ).whenNotMatchedInsertAll(
# ).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable('br_adp_workers_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Paths to your Delta table and input file
br_certification_delta_table_path = "Tables/br_adp_certification_data"
br_certification_input_file_path = f"Files/bronze/adp/v1/{year_str}/{month_str}/{day_str}/certification_data.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data into pandas DataFrame from "/lakehouse/default/" + "Files/bronze/adp/2024/07/03/certification_data.csv"
certification_data = pd.read_csv("/lakehouse/default/" + br_certification_input_file_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

certification_data['associateCertifications'] = certification_data['associateCertifications'].apply(ast.literal_eval)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

certification_data_dic =  certification_data.to_dict(orient='records')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

certification_final_results = []

for cdd in certification_data_dic:
    for associateCertifications in cdd.get('associateCertifications', []):
        update_dict = dict()
        update_dict = {
            'associateOID': associateCertifications.get('links', [{}])[0].get('href', '').split('/')[4] if associateCertifications.get('links', []) else None,
            'categoryCode_codeValue': associateCertifications.get('categoryCode', {}).get('codeValue'),
            'categoryCode_longName': associateCertifications.get('categoryCode', {}).get('longName'),
            'categoryCode_shortName': associateCertifications.get('categoryCode', {}).get('shortName'),
            'certificationID': associateCertifications.get('certificationID', {}).get('idValue'),
            'certificationNameCode_codeValue': associateCertifications.get('certificationNameCode', {}).get('codeValue'),
            'certificationNameCode_longName': associateCertifications.get('certificationNameCode', {}).get('longName'),
            'firstIssueDate': associateCertifications.get('firstIssueDate'),
            'issuingParty_nameCode_codeValue': associateCertifications.get('issuingParty', {}).get('nameCode', {}).get('codeValue'),
            'issuingParty_nameCode_longName': associateCertifications.get('issuingParty', {}).get('nameCode', {}).get('longName'),
            'issuingParty_nameCode_shortName': associateCertifications.get('issuingParty', {}).get('nameCode', {}).get('shortName'),
            'itemID': associateCertifications.get('itemID')
        }
        certification_final_results.append(update_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transformed_certification_data = pd.DataFrame(certification_final_results)
transformed_certification_data.drop_duplicates(inplace=True)
transformed_certification_data = transformed_certification_data[['associateOID' ,'certificationNameCode_codeValue', 'certificationNameCode_longName']]
transformed_certification_data.rename(columns = {'certificationNameCode_longName':'certificationName'},inplace=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
cert_source_df = spark.createDataFrame(transformed_certification_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cert_source_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable('br_adp_certification_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# merge_condition =  "target.associateOID = source.associateOID and target.certificationName = source.certificationName"
# br_certification_table_df.alias("target").merge(
#     cert_source_df.alias("source"),
#     merge_condition
# ).whenMatchedUpdateAll(
# ).whenNotMatchedInsertAll(
# ).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Paths to your Delta table and input file
br_filenumber_delta_table_path = "Tables/br_adp_payrollFileNumber"
br_filenumber_input_file_path = f"Files/bronze/adp/v1/{year_str}/{month_str}/{day_str}/payrollFileNumber_data.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filenumber_data = pd.read_csv("/lakehouse/default/" + br_filenumber_input_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filenumber_data = filenumber_data[['associateOID','payrollFileNumber']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
payroll_source_df = spark.createDataFrame(filenumber_data)
# Load the existing Delta table
#br_payroll_table_df = DeltaTable.forPath(spark, br_filenumber_delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payroll_source_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable('br_adp_filenumber_data')

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

# CELL ********************

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "worker": f"{lakehouse_path}/br_adp_workers_data",
    "certification": f"{lakehouse_path}/br_adp_certification_data",
    "payroll":  f"{lakehouse_path}/br_adp_filenumber_data",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workers_data = spark.read.format("delta").load(table_paths["worker"])
certificate_data = spark.read.format("delta").load(table_paths["certification"])
payroll_data = spark.read.format("delta").load(table_paths["payroll"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workers_data = workers_data.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workers_data = workers_data.rename(
    columns = {
    "associateOID": "associateOID",
    "personName": "EmployeeName",
    "workerId": "WorkerID",
    "workerStatus": "EmploymentStatus",
    "workAssignments_actualStartDate": "EmployeeDesignationStartDate",
    "workAssignments_jobCode_CodeValue": "EmployeeDesignationCode",
    "workAssignments_jobCode_title": "EmployeeDesignation",
    "workAssignments_managementPositionIndicator": "EmployeeManagementsPositionIndicator",
    "certificationNameCode_codeValue": "CertificationCode",
    "certificationName": "NameOfCertification",
    "payrollFileNumber": "EmployeeCode"
}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workers_data_cleaned = workers_data.dropna(subset=['EmployeeDesignation'])
workers_data_cleaned = workers_data_cleaned[workers_data_cleaned['EmploymentStatus'] == 'Active']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert 'EmployeeDesignationStartDate' to datetime
workers_data_cleaned['EmployeeDesignationStartDate'] = pd.to_datetime(workers_data_cleaned['EmployeeDesignationStartDate'])

# Sort DataFrame by 'associateOID' and 'EmployeeDesignationStartDate' in descending order
df_sorted = workers_data_cleaned.sort_values(by=['associateOID', 'EmployeeDesignationStartDate'], ascending=[True, False])

# Add dense rank based on 'associateOID' and 'EmployeeDesignationStartDate'
df_sorted['Rank'] = df_sorted.groupby('associateOID')['EmployeeDesignationStartDate'] \
                             .rank(method='dense', ascending=False).astype(int)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workers_final_data = df_sorted[df_sorted['Rank']==1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(workers_final_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load table1 into a DataFrame
table1 = spark.read.table('br_adp_workers_cleaned_data')

# Load table2 into a DataFrame
table2 = spark.table('br_adp_certification_data')

# Load table3 into a DataFrame
table3 = spark.table('br_adp_filenumber_data')


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

table_merged = table1.join(table2,on='associateOID',how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_merged_final = table_merged.join(table3,on='associateOID',how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_merged_final.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the existing Delta table
# table_df = DeltaTable.forPath(spark, 'Tables/sv_adp_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# merge_condition =  "target.associateOID = source.associateOID and target.certificationName = source.certificationName"
# table_df.alias("target").merge(
#     table_merged.alias("source"),
#     merge_condition
# ).whenMatchedUpdateAll(
# ).whenNotMatchedInsertAll(
# ).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_merged_final.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable('sv_adp_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.sql("SELECT * FROM VPC_Dev_Fablh_data.sv_adp_data limit 100")
# display(df)

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
