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

import pandas as pd
import ast
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException


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

# Function to create the Delta table if it doesn't exist
def create_delta_table(df, path):
    df.write.format("delta").mode("overwrite").save(path)

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

# MARKDOWN ********************

# ## **WORKERS DATA**

# CELL ********************

# Paths to your Delta table and input file
#br_worker_delta_table_path = "Tables/br_adp_workers_data"
br_worker_delta_table_path1 = "Tables/br_adp_workers_data_v1"
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

associateOIDs  = list(set(workers_data['associateOID']))
workers_data['workAssignments'] = workers_data['workAssignments'].apply(ast.literal_eval)
workers_data_dic =  workers_data.to_dict(orient='records')
worker_final_results = []

for wdd in workers_data_dic:
    for workAssignment in wdd.get('workAssignments', []):
        update_dict= dict()
        result = {
            item['typeCode']['shortName']: item['nameCode'].get('shortName', item['nameCode'].get('longName'))
            for item in workAssignment.get('assignedOrganizationalUnits',{})
        }
        
        # Convert to dictionary with nameCode['codeValue'] as keys and codeValue as values
        codeFields_dict = {}
        for item in  ast.literal_eval(wdd['customFieldGroup.codeFields']):
            # Check if nameCode and codeValue exist and are dictionaries
            if isinstance(item, dict) and 'nameCode' in item and isinstance(item['nameCode'], dict):
                key = item['nameCode'].get('codeValue')
                value = item.get('codeValue')
                if key and value:  # Ensure both key and value are present
                    codeFields_dict[key] = value
        # Convert to dictionary with nameCode['codeValue'] as keys and codeValue as values
        dateFields_dict = {}
        for item in  ast.literal_eval(wdd['customFieldGroup.dateFields']):
            # Check if nameCode and codeValue exist and are dictionaries
            if isinstance(item, dict) and 'nameCode' in item and isinstance(item['nameCode'], dict):
                key = item['nameCode'].get('codeValue')
                value = item.get('dateValue')
                if key and value:  # Ensure both key and value are present
                    dateFields_dict[key] = value
        
        # Convert to dictionary with nameCode['codeValue'] as keys and codeValue as values
        multiCodeFields_dict = {}
        for item in ast.literal_eval(wdd['customFieldGroup.multiCodeFields']):
            # Check if nameCode is a dictionary and contains codeValue, and codes exists
            if isinstance(item, dict) and 'nameCode' in item and isinstance(item['nameCode'], dict):
                key = item['nameCode'].get('codeValue')
                
                # Ensure codes is a list and has at least one element before accessing
                codes = item.get('codes')
                if isinstance(codes, list) and len(codes) > 0:
                    value = codes[0].get('codeValue')
                    if key and value:  # Ensure both key and value are present
                        multiCodeFields_dict[key] = value

            
        # Safely accessing the first address
        assigned_work_locations = workAssignment.get('assignedWorkLocations', [])
        
        if assigned_work_locations:
            address_data = assigned_work_locations[0].get('address', {})
        else:
            address_data = {}
        # Extracting values with default fallback
     #   line_one = address_data.get('lineOne', '')
        city_name = address_data.get('cityName', '')
        state_code = address_data.get('countrySubdivisionLevel1', {}).get('codeValue', '')
      #  postal_code = address_data.get('postalCode', '')
       # country_code = address_data.get('countryCode', '')
        
        reportingManger = workAssignment.get('reportsTo', [])
        if reportingManger:
            reportingManger = reportingManger[0].get('reportsToWorkerName','').get('formattedName','')
        else:
            reportingManger = ''
         # Creating the address string with conditionally included parts
        address_parts = [city_name, state_code]
        address = ', '.join(part for part in address_parts if part)
        update_dict = {
            'associateOID': wdd.get('associateOID'),
            'personName': wdd.get('person.legalName.formattedName'),
            'workerId': wdd.get('workerID.idValue', {}),
            'workerStatus': wdd.get('workerStatus.statusCode.codeValue', {}),
            'workAssignments_actualStartDate': workAssignment.get('actualStartDate'),
            'workAssignments_jobCode_CodeValue': workAssignment.get('jobCode', {}).get('codeValue'),
            'workAssignments_jobCode_title': workAssignment.get('jobCode', {}).get('longName', workAssignment.get('jobCode', {}).get('shortName')),
            'workAssignments_managementPositionIndicator': workAssignment.get('managementPositionIndicator',''),
            'postionID':workAssignment.get('positionID',''),
            'seniorityDate':workAssignment.get('seniorityDate', ''),
            'workerType': workAssignment.get('workerTypeCode', {}).get('shortName',''),
            'jobTitle': workAssignment.get('jobTitle', ''),
            'workLocation': address,
            'reportingManger': reportingManger
        }
        update_dict.update(result)
        update_dict.update(codeFields_dict)
        update_dict.update(dateFields_dict)
        update_dict.update(multiCodeFields_dict)

        worker_final_results.append(update_dict)
        
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

transformed_workers_data = transformed_workers_data[['associateOID', 'personName', 'workerId', 'workerStatus',
       'workAssignments_actualStartDate', 'workAssignments_jobCode_CodeValue',
       'workAssignments_jobCode_title',
       'workAssignments_managementPositionIndicator', 'postionID',
       'seniorityDate', 'workerType', 'jobTitle', 'workLocation',
       'reportingManger', 'Business Unit', 'Department', 
        'Distribution List Report', 'Origin Company',
        'Experience Date', 'Practice Group']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transformed_workers_data.columns = ['associateOID', 'personName', 'workerId', 'workerStatus',
       'workAssignments_actualStartDate', 'workAssignments_jobCode_CodeValue',
       'workAssignments_jobCode_title',
       'workAssignments_managementPositionIndicator', 'postionID',
       'seniorityDate', 'workerType', 'jobTitle', 'workLocation',
       'reportingManger', 'BusinessUnit', 'Department', 
        'DistributionListReport', 'OriginCompany',
        'ExperienceDate', 'PracticeGroup']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#transformed_workers_data = pd.DataFrame(worker_final_results)
transformed_workers_data.drop_duplicates(inplace=True)
# Convert date column to datetime
transformed_workers_data['workAssignments_actualStartDate'] = pd.to_datetime(transformed_workers_data['workAssignments_actualStartDate']).dt.date
transformed_workers_data['seniorityDate'] = pd.to_datetime(transformed_workers_data['seniorityDate']).dt.date
transformed_workers_data['ExperienceDate'] = pd.to_datetime(transformed_workers_data['ExperienceDate']).dt.date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transformed_workers_data['date_measured'] = datetime.now().date()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df = spark.createDataFrame(transformed_workers_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable('br_adp_workers_data_v1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Try to load the Delta table
# try:
#     delta_table = DeltaTable.forPath(spark, br_worker_delta_table_path1)
# except AnalysisException:
#     # If the Delta table does not exist, create it
#     create_delta_table(spark_df, br_worker_delta_table_path1)
#     delta_table = DeltaTable.forPath(spark, br_worker_delta_table_path1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **CERTIFICATION DATA**

# CELL ********************

# Paths to your Delta table and input file
br_certification_delta_table_path = "Tables/br_adp_certification_data_v1"
br_certification_input_file_path = f"Files/bronze/adp/v1/{year_str}/{month_str}/{day_str}/certification_data.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

certification_data = pd.read_csv("/lakehouse/default/" + br_certification_input_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

certification_data['certifications'] = certification_data['certifications'].apply(ast.literal_eval)

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
    for associateCertifications in cdd.get('certifications', []).get('associateCertifications', []):
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

transformed_certification_data['date_measured'] = datetime.now().date()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cert_source_df = spark.createDataFrame(transformed_certification_data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cert_source_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable('br_adp_certification_data_v1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **PAYROLL DATA**

# CELL ********************

# Paths to your Delta table and input file
br_filenumber_delta_table_path = "Tables/br_adp_payrollFileNumber"
br_filenumber_input_file_path = f"Files/bronze/adp/v1/{year_str}/{month_str}/{day_str}/payroll_data.csv"

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payroll_source_df.write.format("delta").mode("overwrite").saveAsTable('br_adp_filenumber_data_v1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **LICENSES DATA**

# CELL ********************

# Paths to your Delta table and input file
br_licenses_delta_table_path = "Tables/br_adp_licenses_data_v1"
br_licenses_input_file_path = f"Files/bronze/adp/v1/{year_str}/{month_str}/{day_str}/licenses_data.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

licenses_data = pd.read_csv("/lakehouse/default/" + br_licenses_input_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

licenses_data['licenses'] = licenses_data['licenses'].apply(ast.literal_eval)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

licenses_data_dic =  licenses_data.to_dict(orient='records')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

licenses_final_results = []

for ldd in licenses_data_dic:
    for associateLicenses in ldd.get('licenses', []).get('associateLicenses', []):
        update_dict = dict()
        update_dict = {
            'associateOID': associateLicenses.get('links', [{}])[0].get('href', '').split('/')[4] if associateLicenses.get('links', []) else None,
            'licenseID': associateLicenses.get('licenseID', {}).get('idValue'),
            'licenseName': associateLicenses.get('licenseNameCode', {}).get('longName'),
            'expirationDate': associateLicenses.get('expirationDate', '')
        }
        licenses_final_results.append(update_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transformed_license_data = pd.DataFrame(licenses_final_results)
transformed_license_data.drop_duplicates(inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

license_source_df = spark.createDataFrame(transformed_license_data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

license_source_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable('br_adp_license_data_v1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
