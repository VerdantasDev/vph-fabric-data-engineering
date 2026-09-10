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

# ## **CLILENT**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

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
    "PR": f"{lakehouse_path}/br_deltek_PR",
    "Clendor": f"{lakehouse_path}/br_deltek_Clendor"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read tables into DataFrames
df_PR = spark.read.format("delta").load(table_paths["PR"]).select('ClientID').distinct()
df_Clendor = spark.read.format("delta").load(table_paths["Clendor"]).select(['ClientID','Name']).distinct().withColumnRenamed('Name','ClientName')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_PR.join(df_Clendor, on="ClientID", how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_new_column = df_joined.withColumn("Worked", lit("YES")).withColumn("ClientAddressCity", lit("")).withColumn("ClientAddressState", lit(""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_new_column.select("ClientID").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/sv_deltek_Client_v2"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
df_with_new_column.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **PROJECT**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
    "PR": f"{lakehouse_path}/br_deltek_trans_PR_v3",
    "Clendor": f"{lakehouse_path}/sv_deltek_Client_v2"
    
}

# Read tables into DataFrames
df_PR = spark.read.format("delta").load(table_paths["PR"])
df_Clendor = spark.read.format("delta").load(table_paths["Clendor"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_PR.join(df_Clendor, on="ClientID", how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import concat, col, lit, trim

# Assume df is your DataFrame, and you want to join 'column1', 'column2', and 'column3'
df_joined = df_joined.withColumn("ProjectAddressStreet", trim(concat(col("Address1"), lit(" "), col("Address2"), lit(" "), col("Address3"))))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_joined.select(['ClientID','ClientName','Worked', 'ClientAddressCity', 'ClientAddressState','WBS1','Name', 'Status','ProjectAddressStreet','City','State'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_joined.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_rename = [
    ("WBS1", "ProjectID"),
    ("Name", "ProjectName"),
    ("Status", "ProjectStatus"),
    ("City", "ProjectAddressCity"),
    ("State", "ProjectAddressState")
]

# Apply the renaming using withColumnRenamed in a loop
for original_name, new_name in columns_to_rename:
    df_joined = df_joined.withColumnRenamed(original_name, new_name)

# Select all columns after renaming
df_renamed = df_joined.select("*")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_renamed.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_new_column = df_renamed.withColumn("RevenueTotal", lit("")).withColumn("ProjectPracticeGroup", lit("")).withColumn("ProjectBusinessUnit", lit("")).withColumn("Multiplier", lit(""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_new_column = df_with_new_column[['ClientID',
 'ClientName',
 'Worked',
 'ClientAddressCity',
 'ClientAddressState',
 'ProjectID',
 'ProjectName',
 'RevenueTotal',
 'ProjectStatus',
 'ProjectAddressStreet',
 'ProjectAddressCity',
 'ProjectAddressState',
 'ProjectPracticeGroup',
 'ProjectBusinessUnit',
 'Multiplier']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_new_column.select("ClientID").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/sv_deltek_Projects_v2"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
df_with_new_column.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **EMPLOYEE**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

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
    "Project": f"{lakehouse_path}/sv_deltek_Projects_v2",
    "EMCompany": f"{lakehouse_path}/br_deltek_trans_EMCompany_v3",
    "ADP": f"{lakehouse_path}/sv_adp_data",
    "EmployeeCustomTabFields": f"{lakehouse_path}/br_deltek_EmployeeCustomTabFields",
    "LD": f"{lakehouse_path}/br_deltek_trans_LD_alltime_v3",
    "EMCompany_ori": f"{lakehouse_path}/br_deltek_EMCompany",
    "Clendor": f"{lakehouse_path}/br_deltek_Clendor"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Projects  = spark.read.format("delta").load(table_paths["Project"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ADP = spark.read.format("delta").load(table_paths["ADP"]).select(['EmployeeName','EmployeeNumber','Title','PracticeGroup','CertificationCode','NameOfCertification','BusinessUnit']).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_EMCompany = spark.read.format("delta").load(table_paths["EMCompany"]).select(['Employee_EMCompany','ProvisionalBillingRate']).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_EmployeeCustomTabFields = spark.read.format("delta").load(table_paths["EmployeeCustomTabFields"]).select(['Employee','Select3']).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_LD =  spark.read.format("delta").load(table_paths["LD"])

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
df_EmployeeCustomTabFields = df_EmployeeCustomTabFields.withColumn("Employee", regexp_replace("Employee", "^'?0*", ""))
df_LD = df_LD.withColumn("Employee_LD", regexp_replace("Employee_LD", "^'?0*", ""))

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
    .join(df_EmployeeCustomTabFields.alias("b"), adp_emcompany_join["EmployeeNumber"] == df_EmployeeCustomTabFields["Employee"], how="inner")

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
    .join(df_Projects.alias("b"), adp_emcompany_join3["WBS1"] == df_Projects["ProjectID"], how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_rename = [
    ("Title", "EmployeePosition"),
    ("PracticeGroup", "EmployeePracticeGroupADP"),
    ("ProvisionalBillingRate", "BillingRate"),
    ("City", "ProjectAddressCity"),
    ("Select3", "EmployeePracticeGroupDeltek"),
    ("BusinessUnit", "EmployeeBusinessUnit")      
]

# Apply the renaming using withColumnRenamed in a loop
for original_name, new_name in columns_to_rename:
    adp_emcompany_join4 = adp_emcompany_join4.withColumnRenamed(original_name, new_name)

# Select all columns after renaming
df_renamed = adp_emcompany_join4.select("*")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_renamed = df_renamed[[
 'ClientID',
 'ClientName',
 'Worked',
 'ClientAddressCity',
 'ClientAddressState',
 'ProjectID',
 'ProjectName',
 'RevenueTotal',
 'ProjectStatus',
 'ProjectAddressStreet',
 'ProjectAddressCity',
 'ProjectAddressState',
 'ProjectPracticeGroup',
 'ProjectBusinessUnit',
 'EmployeeID',
 'EmployeeName',
 'EmployeePosition',
 'EmployeePracticeGroupADP',
 'EmployeePracticeGroupDeltek',
 'EmployeeBusinessUnit',
 'BillingRate',
 'CertificationCode',
 'NameOfCertification',
 ]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_renamed.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_renamed = df_renamed.orderBy("ClientID", "ProjectID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_renamed.select("EmployeeID").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_renamed.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/sv_deltek_Employees_v3"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
df_renamed.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **New deltek requirement 16/08/2024**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoadTables") \
    .getOrCreate()

    # Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "Project": f"{lakehouse_path}/sv_deltek_Projects_v1",
    "EMCompany": f"{lakehouse_path}/br_deltek_trans_EMCompany_v3",
    "ADP": f"{lakehouse_path}/sv_adp_data",
    "EmployeeCustomTabFields": f"{lakehouse_path}/br_deltek_EmployeeCustomTabFields",
    "LD": f"{lakehouse_path}/br_deltek_trans_LD_alltime_v3",
    "employee_new_join" : f"{lakehouse_path}/sv_deltek_Employees_v1",
    "Clendor" : f"{lakehouse_path}/br_deltek_Clendor",
    "FW" : f"{lakehouse_path}/br_deltek_FWCustomColumnValuesData"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fw  = spark.read.format("delta").load(table_paths["FW"])
display(df_fw.filter(col("Code").contains("002")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp_join  = spark.read.format("delta").load(table_paths["employee_new_join"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Clendor  = spark.read.format("delta").load(table_paths["Clendor"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ADP  = spark.read.format("delta").load(table_paths["ADP"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_ADP.select("PracticeGroup").distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp_join.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower

# List of specific ClientID values
client_ids = ["V6A080707","V6A080365","V6A001416","V6A080707",
"V6A001416","V6A030651","V6A001392","V6A001378","V6A001374","V6A001373",
"V6A001381","V6A001386","V6A200001","V6A001486","V6A020343"
]

# Filter the DataFrame based on the ClientID column
filtered_df = df_ADP.filter(lower(col("EmployeeNumber")).isin([client_id.lower() for client_id in client_ids]))

display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_ADP.select("CertificationCode","NameOfCertification").distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower
from functools import reduce
from pyspark.sql import DataFrame

# List of columns in df_Clendor
columns_to_check = df_Clendor.columns

# List to store all conditions
conditions = [
    lower(col(col_name)).isin([client_id.lower() for client_id in client_ids])
    for col_name in columns_to_check
]

# Combine all conditions with OR
combined_condition = reduce(lambda x, y: x | y, conditions)

# Apply the OR filter across all columns
filtered_df = df_Clendor.filter(combined_condition)

display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower
from functools import reduce
from pyspark.sql import DataFrame

# List of columns in df_Clendor
columns_to_check = df_emp_join.columns

# List to store all conditions
conditions = [
    lower(col(col_name)).isin([client_id.lower() for client_id in client_ids])
    for col_name in columns_to_check
]

# Combine all conditions with OR
combined_condition = reduce(lambda x, y: x | y, conditions)

# Apply the OR filter across all columns
filtered_df = df_emp_join.filter(combined_condition)

display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
