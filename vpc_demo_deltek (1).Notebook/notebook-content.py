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

# MARKDOWN ********************

# ## **Client**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when

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

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "Clendor": f"{lakehouse_path}/br_deltek_Clendor_v2",
    "CL": f"{lakehouse_path}/br_deltek_CL",
    "PR": f"{lakehouse_path}/br_deltek_PR"
}
# Read tables into DataFrames
df_CL = spark.read.format("delta").load(table_paths["CL"])
df_PR = spark.read.format("delta").load(table_paths["PR"])
df_Clendor = spark.read.format("delta").load(table_paths["Clendor"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_Clendor)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Clendor_trans = df_Clendor.select(['ClientID','Name']).withColumnRenamed('Name','ClientName')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Clendor_trans = df_Clendor_trans.withColumn("Worked", lit("YES")).withColumn("ClientAddressCity", lit("NA")).withColumn("ClientAddressState", lit("NA"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Clendor_trans.select("ClientID").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, trim

# Filter the DataFrame to check for null or empty strings in the specified column after stripping whitespace
df_filtered = df_Clendor_trans.filter(trim(col("ClientName")).eqNullSafe("") | col("ClientName").isNull())

# Show the filtered DataFrame
display(df_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_Client_demo"  # Example Delta Lake table path in Azure Data Lake Storage
 
# Write data to Delta Lake table
df_Clendor_trans.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Project**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, trim, count, concat

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoadTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "Clendor_trans": f"{lakehouse_path}/br_deltek_Client_demo",
    "PR": f"{lakehouse_path}/br_deltek_PR_v3"
}
# Read tables into DataFrames
df_Clendor_trans = spark.read.format("delta").load(table_paths["Clendor_trans"])
df_PR = spark.read.format("delta").load(table_paths["PR"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_PR.select("WBS1").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

wbs1_counts = df_PR.groupBy("WBS1").agg(count("WBS1").alias("count"))

duplicate_wbs1 = wbs1_counts.filter(col("count") > 1).select("WBS1")
unique_wbs1 = wbs1_counts.filter(col("count") == 1).select("WBS1")

df_PR_duplicates = df_PR.join(duplicate_wbs1, on="WBS1", how="inner")
df_PR_unique = df_PR.join(unique_wbs1, on="WBS1", how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_PR_duplicates.select("WBS1").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_PR_filtered = df_PR_duplicates.filter(
    (trim(col("WBS2")).isNull() | (trim(col("WBS2")) == "")) &
    (trim(col("WBS3")).isNull() | (trim(col("WBS3")) == ""))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_combined = df_PR_unique.unionByName(df_PR_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_combined.select("WBS1").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_combined.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_combined.withColumn("ProjectAddressStreet", trim(concat(col("Address1"), lit(" "), col("Address2"), lit(" "), col("Address3"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_PR_trans = df_joined.select("WBS1","Name","ClientID","Status","ProjectType","ProjectAddressStreet","City","State","ProjMgr","BillingClientID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rename_dict = {
    "WBS1": "ProjectID_PR",
    "Name": "ProjectName_PR",
    "ClientID": "ClientID_PR",
    "Status": "ProjectStatus_PR",
    "ProjectType": "ProjectType_PR",
    "ProjectAddressStreet": "ProjectAddressStreet_PR",
    "City": "ProjectAddressCity_PR",
    "State": "ProjectAddressState_PR",
    "ProjMgr": "ProjectManager_PR",
    "BillingClientID": "BillingClientID_PR"
}

df_PR_trans = df_PR_trans.select(
    [col(old_name).alias(new_name) for old_name, new_name in rename_dict.items()]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_PR_trans)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_Project_demo"  # Example Delta Lake table path in Azure Data Lake Storage
 
# Write data to Delta Lake table
df_PR_trans.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Project Revenue**

# CELL ********************

# from pyspark.sql import SparkSession
# import pandas as pd
# from pyspark.sql.types import DoubleType

# # Initialize Spark session (if not already initialized)
# spark = SparkSession.builder.appName("DeltaLakeWrite").getOrCreate()

# # Define the path to the built-in resource
# builtin_file_path = "file:///synfs/nb_resource/builtin/Project Earnings & PR table.xlsx"

# # Define the sheet name to read
# sheet_name = "Revenue Table"  # Replace with your actual sheet name

# # Read the Excel file into a Pandas DataFrame
# df_pandas = pd.read_excel(builtin_file_path, sheet_name=sheet_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df1 = df_pandas.drop(columns = ['WBS3'])
# df_spark = spark.createDataFrame(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import SparkSession
# import re

# # Initialize Spark session (if not already initialized)
# spark = SparkSession.builder.appName("DeltaLakeWrite").getOrCreate()

# # Function to sanitize column names
# def sanitize_column_name(col_name):
#     # Replace spaces and special characters with underscores
#     col_name = re.sub(r'[^\w]', '_', col_name)
#     # Ensure no double underscores or trailing underscores
#     col_name = re.sub(r'__+', '_', col_name)
#     return col_name

# # Sanitize all column names in df_spark
# sanitized_column_names = [sanitize_column_name(col) for col in df_spark.columns]

# # Rename columns in the DataFrame
# df_sanitized = df_spark.toDF(*sanitized_column_names)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# # Define the path for the Delta Lake table in the lakehouse
# delta_table_path = "Tables/br_deltek_Revenue"

# # Write the DataFrame to the Delta Lake table
# df_sanitized.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, trim, count, concat

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoadTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "Rev": f"{lakehouse_path}/br_deltek_Revenue"
}
# Read tables into DataFrames
df_Rev = spark.read.format("delta").load(table_paths["Rev"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Rev_req = df_Rev[[
 'WBS1',
 'Area',
 'ContractAmt',
 'Rev_JTD',
 'Billed_JTD',
 'UnBilled_JTD',
 'BACKLOG_JTD_',
 'TotalRevenueBudget',
 'ProjectType',
 'PracticService',
 'PS_Practice',
 'PS_Service',
 'PR_Project_Manager',
 'PR_PIMID']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Define the columns to group by
group_by_columns = [
    'WBS1',
    'PR_Project_Manager',
    'PR_PIMID'
]

# Define the columns to aggregate with the sum
agg_columns = [
    'ContractAmt',
    'Rev_JTD',
    'Billed_JTD',
    'UnBilled_JTD',
    'BACKLOG_JTD_',
    'TotalRevenueBudget'
]

# Define the columns with varying values that need to be concatenated
concat_columns = [
    'Area',
    'ProjectType',
    'PracticService',
    'PS_Practice',
    'PS_Service',
]

# Perform the group by and aggregation
df_grouped = df_Rev_req.groupBy(group_by_columns).agg(
    # Aggregating the sum for the numerical columns
    *[F.sum(col).alias(col) for col in agg_columns],
    
    # Concatenate values with semicolon, ignoring NaN/None values
    *[
        F.concat_ws(';', 
            F.collect_set(F.when(~F.isnan(F.col(col)) & F.col(col).isNotNull(), F.col(col)))
        ).alias(col)
        for col in concat_columns
    ]
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a dictionary of old column names to new column names
rename_columns = {
    'WBS1': 'ProjectID_Rev',
    'Area': 'ProjectArea_Rev',
    'PR_Project_Manager': 'ProjectManager_Rev',
    'PR_PIMID': 'ProjectManagerID_Rev',
    'ContractAmt': 'ContractAmount_Rev',
    'Rev_JTD': 'RevenueToDate_Rev',
    'Billed_JTD': 'BilledToDate_Rev',
    'UnBilled_JTD': 'UnbilledToDate_Rev',
    'BACKLOG_JTD_': 'BacklogToDate_Rev',
    'TotalRevenueBudget': 'TotalRevenueBudget_Rev',
    'ProjectType': 'ProjectType_Rev',
    'PracticService': 'ProjectPracticeServiceGroup_Rev',
    'PS_Practice': 'ProjectPracticeGroup_Rev',
    'PS_Service': 'ProjectServiceGroup_Rev'
}

# Loop over the dictionary and rename the columns
for old_name, new_name in rename_columns.items():
    df_grouped = df_grouped.withColumnRenamed(old_name, new_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_grouped.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_grouped.select("ProjectID_Rev").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

wbs1_counts = df_grouped.groupBy("ProjectID_Rev").agg(count("ProjectID_Rev").alias("count"))
duplicate_wbs1 = wbs1_counts.filter(col("count") > 1).select("ProjectID_Rev")
df_duplicates = df_grouped.join(duplicate_wbs1, on="ProjectID_Rev", how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_duplicates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_grouped)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_Revenue_demo"  # Example Delta Lake table path in Azure Data Lake Storage
 
# Write data to Delta Lake table
df_grouped.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Employee**

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
    "EMCompany": f"{lakehouse_path}/br_deltek_trans_EMCompany_v4",
    "ADP": f"{lakehouse_path}/sv_adp_data_v1",
    "EmployeeCustomTabFields": f"{lakehouse_path}/br_deltek_EmployeeCustomTabFields",
    "LD": f"{lakehouse_path}/br_deltek_trans_LD_alltime_v4",
    "UTE": f"{lakehouse_path}/br_deltek_utilisation_last7days",
    "skills": f"{lakehouse_path}/br_m365_profile_skills_data",
    "emall": f"{lakehouse_path}/br_deltek_EMAllCompany"
}

df_EMCompany = spark.read.format("delta").load(table_paths["EMCompany"]).select(['Employee_EMCompany','EmployeeJobCostRate','BillingCategory']).distinct()
df_ADP = spark.read.format("delta").load(table_paths["ADP"]).select(['EmployeeName','EmployeeNumber','Title','PracticeGroup','CertificationCode','NameOfCertification','BusinessUnit']).distinct()
df_EMCust = spark.read.format("delta").load(table_paths["EmployeeCustomTabFields"]).select(['Employee','Select3']).distinct()
df_LD = spark.read.format("delta").load(table_paths["LD"])
df_UTE = spark.read.format("delta").load(table_paths["UTE"]).select("Employee_LD","UTE","Target_UTE").withColumnRenamed("Employee_LD","Employee_UTE")
df_skills = spark.read.format("delta").load(table_paths["UTE"])
df_emall = spark.read.format("delta").load(table_paths["emall"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ADP = df_ADP.withColumn("EmployeeID", col("EmployeeNumber"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ADP = df_ADP.withColumn("EmployeeNumber", regexp_replace("EmployeeNumber", "^V6A0*", ""))
df_EMCompany = df_EMCompany.withColumn("Employee_EMCompany", regexp_replace("Employee_EMCompany", "^'?0*", ""))
df_EMCust = df_EMCust.withColumn("Employee", regexp_replace("Employee", "^'?0*", ""))
df_LD = df_LD.withColumn("Employee_LD", regexp_replace("Employee_LD", "^'?0*", ""))
df_UTE = df_UTE.withColumn("Employee_UTE", regexp_replace("Employee_UTE", "^'?0*", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_emcompany_join = df_ADP.alias("a") \
    .join(df_EMCompany.alias("b"), df_ADP["EmployeeNumber"] == df_EMCompany["Employee_EMCompany"], how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_emcompany_join2 = adp_emcompany_join.alias("a") \
    .join(df_EMCust.alias("b"), adp_emcompany_join["EmployeeNumber"] == df_EMCust["Employee"], how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_emcompany_join3 = adp_emcompany_join2.alias("a") \
    .join(df_LD.alias("b"), adp_emcompany_join2["EmployeeNumber"] == df_LD["Employee_LD"], how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_emcompany_join4 = adp_emcompany_join3.alias("a") \
    .join(df_UTE.alias("b"), adp_emcompany_join3["Employee_LD"] == df_UTE["Employee_UTE"], how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_emcompany_join4.select("Employee_LD").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_Employees_demo"  # Example Delta Lake table path in Azure Data Lake Storage
 
# Write data to Delta Lake table
adp_emcompany_join4.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Joins**

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
    "Employee": f"{lakehouse_path}/br_deltek_Employees_demo",
    "Project": f"{lakehouse_path}/br_deltek_Project_demo",
    "Revenue": f"{lakehouse_path}/br_deltek_Revenue_demo",
    "Client": f"{lakehouse_path}/br_deltek_Client_demo",
    "test": f"{lakehouse_path}/br_deltek_DemoData_v1",
    "emall": f"{lakehouse_path}/br_deltek_EMAllCompany"
}

df_Employee = spark.read.format("delta").load(table_paths["Employee"])
df_Project = spark.read.format("delta").load(table_paths["Project"])
df_Revenue = spark.read.format("delta").load(table_paths["Revenue"])
df_Client = spark.read.format("delta").load(table_paths["Client"])
df_test = spark.read.format("delta").load(table_paths["test"])
df_emall = spark.read.format("delta").load(table_paths["emall"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_pr_join = df_Employee.alias("a") \
    .join(df_Project.alias("b"), df_Employee["WBS1"] == df_Project["ProjectID_PR"], how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_pr_rev_join = emp_pr_join.alias("a") \
    .join(df_Revenue.alias("b"), emp_pr_join["ProjectID_PR"] == df_Revenue["ProjectID_Rev"], how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = emp_pr_rev_join.alias("a") \
    .join(df_Client.alias("b"), emp_pr_rev_join["ClientID_PR"] == df_Client["ClientID"], how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.select("ClientID").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Assuming you have two columns 'ProjectManager_PR' and 'ProjectManager_Rev' to compare
df_differing_rows = df_final.filter(
    F.col('ProjectManager_PR').cast('int') != F.col('ProjectManager_Rev').cast('int')
)

# Show the rows where the two columns differ
display(df_differing_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rename_columns = {
    'EmployeeName': 'EmployeeName',
    'Title': 'Title',
    'PracticeGroup': 'PracticeGroupADP',
    'CertificationCode': 'CertificationCode',
    'NameOfCertification': 'NameOfCertification',
    'BusinessUnit': 'BusinessUnit',
    'EmployeeID': 'EmployeeID',
    'EmployeeJobCostRate': 'EmployeeJobCostRate',
    'Select3': 'PracticeGroupDeltek',
    'ProjectID_PR': 'ProjectID',
    'ProjectName_PR': 'ProjectName',
    'ProjectStatus_PR': 'ProjectStatus',
    'ProjectAddressStreet_PR': 'ProjectAddressStreet',
    'ProjectAddressCity_PR': 'ProjectAddressCity',
    'ProjectAddressState_PR': 'ProjectAddressState',
    'ProjectManager_Rev': 'ProjectManager',
    'ProjectManagerID_Rev': 'ProjectManagerID',
    'ContractAmount_Rev': 'Budget',
    'RevenueToDate_Rev': 'RevenueToDate',
    'BilledToDate_Rev': 'BilledToDate',
    'UnbilledToDate_Rev': 'UnbilledToDate',
    'BacklogToDate_Rev': 'BacklogToDate',
    'TotalRevenueBudget_Rev': 'TotalRevenueBudget',
    'ProjectArea_Rev': 'ProjectArea',
    'ProjectType_Rev': 'ProjectType',
    'ProjectPracticeServiceGroup_Rev': 'ProjectPracticeServiceGroup',
    'ProjectPracticeGroup_Rev': 'ProjectPracticeGroup',
    'ProjectServiceGroup_Rev': 'ProjectServiceGroup',
    'ClientID': 'ClientID',
    'ClientName': 'ClientName',
    'Worked': 'Worked',
    'ClientAddressCity': 'ClientAddressCity',
    'ClientAddressState': 'ClientAddressState',
    'UTE': 'EmployeeUtilization',
    'Target_UTE': 'TargetUtilization'
}

for old_name, new_name in rename_columns.items():
    df_final = df_final.withColumnRenamed(old_name, new_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final2 = df_final[['ClientID','ClientName','Worked','ClientAddressCity',
'ClientAddressState','ProjectID','ProjectName','ProjectStatus',
'ProjectAddressStreet','ProjectAddressCity','ProjectAddressState',
'ProjectManager','ProjectManagerID','Budget','RevenueToDate','BilledToDate',
'UnbilledToDate','BacklogToDate','TotalRevenueBudget','ProjectArea',
'ProjectType','ProjectPracticeServiceGroup','ProjectPracticeGroup',
'ProjectServiceGroup','EmployeeID','EmployeeName','Title','PracticeGroupADP',
'PracticeGroupDeltek',
'BusinessUnit','EmployeeJobCostRate','EmployeeUtilization','TargetUtilization',
'NameOfCertification','CertificationCode']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final2_sorted = df_final2.orderBy(F.col('ClientID'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final2_sorted = df_final2_sorted.fillna('NA')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final2_sorted)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_trans = df_final2_sorted.withColumn('ProjectStatus',
    when(col('ProjectStatus') == 'D', 'Dormant').when(col('ProjectStatus') == 'I', 'In Active').when(col('ProjectStatus') == 'A', 'Active')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install us

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import us

state_mapping = {state.abbr: state.name for state in us.states.STATES}

# Broadcast the dictionary to all worker nodes
state_mapping_broadcast = spark.sparkContext.broadcast(state_mapping)

# Define a UDF that uses the broadcasted dictionary
def abbrev_to_full_name(abbrev):
    return state_mapping_broadcast.value.get(abbrev)

# Register the UDF in Spark
abbrev_to_full_name_udf = F.udf(abbrev_to_full_name, StringType())

# Apply the UDF to the 'State_Abbreviation' column
df_with_full_names = df_trans.withColumn("ProjectAddressState", abbrev_to_full_name_udf(F.col("ProjectAddressState")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

code_mapping = {
    "009": "Geological and Geotechnical Engineering",
    "012": "Process Engineering",
    "013": "Site and Roadway Engineering",
    "015": "Shared Services",
    "006": "Natural Resources and Environmental Planning",
    "011": "Mechanical, Electrical, and Automation Engineering",
    "003": "Environmental Assessment & Remediation",
    "008": "Environmental Health and Safety",
    "014": "Structural Engineering and Architecture",
    "010": "Hydrology, Hydraulics, and Fluids",
    "007": "Sustainability",
    "002": "Precise Visual Technologies / Applied Data and Technology"
}

code_mapping_broadcast = spark.sparkContext.broadcast(code_mapping)

# Define a UDF that uses the broadcasted dictionary
def map_code_to_name(code):
    return code_mapping_broadcast.value.get(code, "Unknown Code")

# Register the UDF in Spark
map_code_to_name_udf = F.udf(map_code_to_name, StringType())

# Apply the UDF to the 'Code' column
df_with_names = df_with_full_names.withColumn("PracticeGroupDeltek", map_code_to_name_udf(F.col("PracticeGroupDeltek")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_na = df_with_names.select(
    [when(col(c) == "", "NA").otherwise(col(c)).alias(c) for c in df_with_names.columns]
)
df_filled = df_with_na.fillna("NA")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_filled)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_filled.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_DemoData_v1"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
df_filled.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total_rows = df_filled.count()

# Count the number of 'NA' values per column
na_counts = {col_name: df_filled.filter(col(col_name) == 'NA').count() for col_name in df_filled.columns}

# Calculate the percentage of 'NA' values per column
percentage_na = {col_name: (count / total_rows) * 100 for col_name, count in na_counts.items()}

# Convert the result to a DataFrame
percentage_na_df = spark.createDataFrame(
    [(col_name, percentage) for col_name, percentage in percentage_na.items()],
    ['Column', 'Percentage_NA']
)

display(percentage_na_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
    "Employee": f"{lakehouse_path}/br_deltek_Employees_demo",
    "Project": f"{lakehouse_path}/br_deltek_Project_demo",
    "Revenue": f"{lakehouse_path}/br_deltek_Revenue_demo",
    "Client": f"{lakehouse_path}/br_deltek_Client_demo",
    "test": f"{lakehouse_path}/br_deltek_DemoData_v3",
    "emall": f"{lakehouse_path}/br_deltek_EMAllCompany"
}

df_Employee = spark.read.format("delta").load(table_paths["Employee"])
df_Project = spark.read.format("delta").load(table_paths["Project"])
df_Revenue = spark.read.format("delta").load(table_paths["Revenue"])
df_Client = spark.read.format("delta").load(table_paths["Client"])
df_test = spark.read.format("delta").load(table_paths["test"])
df_emall = spark.read.format("delta").load(table_paths["emall"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = df_EMCompany.select("Employee_EMCompany","BillingCategory").drop_duplicates()
print(df1.count())
df1.select("Employee_EMCompany").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_test.join(df1, df_test.EmployeeID == df1.Employee_EMCompany, how='left').drop("Employee_EMCompany")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_deltek_DemoData_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Sharepoint Deltek Join**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when

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

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "Clendor": f"{lakehouse_path}/br_deltek_Clendor_v2",
    "PR": f"{lakehouse_path}/br_deltek_Project_demo",
    "spo": f"{lakehouse_path}/br_m365_sharepoint_download_file_status_v2"
}
# Read tables into DataFrames
df_spo = spark.read.format("delta").load(table_paths["spo"])
df_PR = spark.read.format("delta").load(table_paths["PR"])
df_Clendor = spark.read.format("delta").load(table_paths["Clendor"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_PR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_spo)

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
