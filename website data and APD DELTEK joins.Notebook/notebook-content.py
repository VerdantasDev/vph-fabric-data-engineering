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

# ## Joining Tables


# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.functions import to_date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff, max
from datetime import timedelta
from pyspark.sql import functions as F




# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "adp_deltek": f"{lakehouse_path}/sv_deltek_adp_merged_v4",
    "website_data" : f"{lakehouse_path}/sv_verdantas_website_data"
}

adp_deltek = spark.read.format("delta").load(table_paths["adp_deltek"])
website_data = spark.read.format("delta").load(table_paths["website_data"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame

def dataframe_shape(df: DataFrame) -> tuple:
    num_rows = df.count()
    num_columns = len(df.columns)
    return num_rows, num_columns


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek = adp_deltek.toPandas()
website_data = website_data.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# adp_deltek = adp_deltek[['EmployeeName','EmploymentStatus','EmployeeBillType','Title','PracticeGroup']]
# website_data = website_data[['expertise_name','expertise_contact_person', 'service_name', 'market_name', 'market_contact_person','market_contact_person_designation', 'solution_name','project_name']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Assuming 'adp_deltek' is already defined
# Filter the rows to be exploded for 'Mechanical, Electrical & Automation Engineering'
rows_to_explode_mechanical = adp_deltek[adp_deltek['PracticeGroup'] == 'Mechanical, Electrical & Automation Engineering']

# Repeat each row 3 times for 'Mechanical, Electrical & Automation Engineering'
repeated_rows_mechanical = rows_to_explode_mechanical.loc[rows_to_explode_mechanical.index.repeat(3)].copy()

# Replace the 'PracticeGroup' values with the specific types for 'Mechanical, Electrical & Automation Engineering'
repeated_rows_mechanical['PracticeGroup'] = ['Mechanical Engineering', 'Electrical Engineering', 'Automation Engineering'] * len(rows_to_explode_mechanical)

# Filter the rows to be exploded for 'Structural Engineering & Architecture'
rows_to_explode_structural = adp_deltek[adp_deltek['PracticeGroup'] == 'Structural Engineering & Architecture']

# Repeat each row 2 times for 'Structural Engineering & Architecture'
repeated_rows_structural = rows_to_explode_structural.loc[rows_to_explode_structural.index.repeat(2)].copy()

# Replace the 'PracticeGroup' values with the specific types for 'Structural Engineering & Architecture'
repeated_rows_structural['PracticeGroup'] = ['Structural Engineering', 'Architecture'] * len(rows_to_explode_structural)

# Concatenate the modified DataFrames with the remaining original DataFrame
adp_deltek = pd.concat([
    adp_deltek[~adp_deltek['PracticeGroup'].isin(['Mechanical, Electrical & Automation Engineering', 'Structural Engineering & Architecture'])],
    repeated_rows_mechanical,
    repeated_rows_structural
])

# Reset index if needed
adp_deltek.reset_index(drop=True, inplace=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PL = ['Water & Wastewater Systems Engineering','Structural Engineering','Environmental Health & Safety']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# adp_filter = adp_deltek[adp_deltek['PracticeGroup'].isin(PL)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# website_filter = website_data[website_data['expertise_name'].isin(PL)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek['PracticeGroup'].value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data['expertise_name'].value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

matching_values = adp_deltek[adp_deltek['PracticeGroup'].isin(website_data['expertise_name'])]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matching_values_list = matching_values['PracticeGroup'].unique().tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# adp_deltek = adp_deltek.isin(matching_values_list)
# website_data = website_data.isin(matching_values_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pd.set_option('display.max_columns', None)
adp_deltek.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate the percentage of missing values in each column
missing_percentage = (adp_deltek.isnull().sum() / adp_deltek.shape[0]) * 100

# Filter columns with missing values greater than 1%
missing_greater_than_1 = missing_percentage[missing_percentage > 0]

# Display the result
print(missing_greater_than_1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate the percentage of missing values in each column
missing_percentage = (website_data.isnull().sum() / website_data.shape[0]) * 100

# Filter columns with missing values greater than 1%
# missing_greater_than_1 = missing_percentage[missing_percentage > 0]

# Display the result
# print(missing_greater_than_1)
missing_percentage

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek.rename(columns={'PracticeGroup':'Key'}, inplace=True)
website_data.rename(columns={'expertise_name':'Key'},inplace=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek[adp_deltek['Key'].isin(matching_values_list)]['Key'].value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data[website_data['Key'].isin(matching_values_list)]['Key'].value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_data = pd.merge(website_data, adp_deltek, on='Key', how='left')
merged_data.shape
merged_data.rename(columns={'Key':'ExperticeName_PracticeGroup'},inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_data.head(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_data.isnull().sum()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate the percentage of missing values in each column
missing_percentage = (merged_data.isnull().sum() / merged_data.shape[0]) * 100

# Filter columns with missing values greater than 1%
missing_greater_than_0 = missing_percentage[missing_percentage > 0]

# Display the result
print(missing_greater_than_0)
# missing_percentage

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(merged_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_data = spark.createDataFrame(merged_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(merged_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/br_website_adp_deltek_V1"
 
# Write data to Delta Lake table
merged_data.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
 
# # Create a Spark session
# spark = SparkSession.builder \
#     .appName("Sharepoint") \
#     .getOrCreate()
# spark_df = spark.createDataFrame(merged_data)
 
# Write DataFrame to Delta Lake table
delta_table_path = "lakehouse/Tables/br_website_adp_deltek_V1"  # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
mer.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(delta_table_path)

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

import pandas as pd
rows_to_explode = adp_deltek[adp_deltek['PracticeGroup'] == 'Mechanical, Electrical & Automation Engineering']
exploded_rows = pd.DataFrame({
    'adp_deltek': ['Mechanical Engineering', 'Electrical Engineering', 'Automation Engineering'],
    'other_column': rows_to_explode['other_column'].values.repeat(3)
})
adp_deltek = pd.concat([adp_deltek[adp_deltek['PracticeGroup'] != 'Mechanical, Electrical & Automation Engineering'], exploded_rows])
adp_deltek.reset_index(drop=True, inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(adp_deltek)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_data = merged_data.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Old Data QC


# MARKDOWN ********************


# CELL ********************

adp_deltek.select("PracticeGroup").distinct().rdd.flatMap(lambda x: x).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.select('expertise_name').distinct().rdd.flatMap(lambda x: x).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ['Water & Wastewater Systems Engineering', 'Site & Roadway Civil Engineering', 'Digital Technologies','Environmental Health & Safety','Sustainability Advisory','Hydrology, Hydraulics & Fluids', 'Natural Resources & Environmental Planning',
# 'Geotechnical & Geological Engineering','Environmental Assessment & Remediation']
# [ 'Mechanical, Electrical & Automation Engineering','Shared Services','Structural Engineering']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.select('Service_name').distinct().rdd.flatMap(lambda x: x).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.select('market_name').distinct().rdd.flatMap(lambda x: x).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek.select('ProjectDetails').distinct().rdd.flatMap(lambda x: x).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(adp_deltek.select('ProjectDetails').distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.select('expertise_contact_person').distinct().rdd.flatMap(lambda x: x).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek.select('Supervisor_Name').distinct().rdd.flatMap(lambda x: x).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_AD_website = adp_deltek.join(website_data,adp_deltek.PracticeGroup == website_data.expertise_name, "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_AD_website.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def find_duplicated_rows(df):
    duplicated_rows = df.groupBy(df.columns).count().filter(col("count") > 1).drop("count")
    return df.join(duplicated_rows, on=df.columns, how="inner").count()

def drop_duplicated_rows(df):
    return df.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

find_duplicated_rows(adp_deltek)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

find_duplicated_rows(website_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek.groupBy("PracticeGroup").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

website_data.groupBy("expertise_name").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_AD_website = adp_deltek.join(website_data,adp_deltek.PracticeGroup == website_data.expertise_name, "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_AD_website.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

project_details_list = adp_deltek.select('ProjectDetails').rdd.flatMap(lambda x: x).collect()

display(project_details_list[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_deltek.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(joined_AD_website)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

find_duplicated_rows(joined_AD_website)

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
