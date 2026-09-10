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

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "LD_df": f"{lakehouse_path}/br_deltek_LD",
    "PR_df": f"{lakehouse_path}/br_deltek_PR",
    "LD_df_transform": f"{lakehouse_path}/br_deltek_trans_LD",
    "PR_df_transform": f"{lakehouse_path}/br_deltek_trans_PR",
    "Clendor_df" : f"{lakehouse_path}/br_deltek_Clendor",
    "deltek_adp" : f"{lakehouse_path}/sv_deltek_adp_merged"
}
# Read tables into DataFrames
ld_df = spark.read.format("delta").load(table_paths["LD_df"])
pr_df = spark.read.format("delta").load(table_paths["PR_df"])
# Read tables into DataFrames
ld_transform_df = spark.read.format("delta").load(table_paths["LD_df_transform"])
pr_transform_df = spark.read.format("delta").load(table_paths["PR_df_transform"])
clendor_df =  spark.read.format("delta").load(table_paths["Clendor_df"])
deltek_adp =  spark.read.format("delta").load(table_paths["deltek_adp"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(ld_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ld_df.select("Employee").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(ld_df.filter(ld_df['Employee'] == '000466'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(deltek_adp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(pr_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "project": f"{lakehouse_path}/sv_deltek_Projects_v2",
    "employee": f"{lakehouse_path}/sv_deltek_Employees_v2"
}
# Read tables into DataFrames
project = spark.read.format("delta").load(table_paths["project"])
employee = spark.read.format("delta").load(table_paths["employee"])

project = project.toPandas()
employee= employee.toPandas()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import pandas as pd
from tabulate import tabulate

from tabulate import tabulate

def print_dataframe_shape(df):
    shape_data = [('Rows', df.shape[0]), ('Columns', df.shape[1])]
    print("DataFrame Shape:")
    print(tabulate(shape_data, headers=['Description', 'Value'], tablefmt='pretty'))


def print_dataframe_columns(df):
    columns_data = [(index + 1, column) for index, column in enumerate(df.columns)]
    print("\nDataFrame Columns:")
    print(tabulate(columns_data, headers=['Index', 'Column Name'], tablefmt='pretty'))

from tabulate import tabulate

def print_missing_percentage(df, threshold=1):
    missing_percentage = df.isnull().mean() * 100
    filtered_data = {column: percentage for column, percentage in missing_percentage.items() if percentage > threshold}
    
    if filtered_data:
        table_data = [(column, f'{percentage:.2f}%') for column, percentage in filtered_data.items()]
        print(tabulate(table_data, headers=['Column Name', 'Missing Percentage'], tablefmt='pretty'))
    else:
        print(f"No columns have missing percentages greater than {threshold}%.")

# Example usage:
# print_missing_percentage(df, threshold=1)


def print_nunique(df):
    # Get the number of unique values in each column
    nunique_data = [(column, df[column].nunique()) for column in df.columns]
    
    # Print the number of unique values table
    print("\nNumber of Unique Values per Column:")
    print(tabulate(nunique_data, headers=['Column Name', 'Unique Count'], tablefmt='pretty'))

def print_unique(df):
    for column in df.columns:
        unique_values = df[column].unique()
        print(f"\nUnique Values in Column '{column}':")
        print(tabulate(enumerate(unique_values, 1), headers=['Index', 'Unique Value'], tablefmt='pretty'))

def print_value_counts(df):
    for column in df.columns:
        value_counts = df[column].value_counts()
        value_counts_data = [(index, count) for index, count in value_counts.items()]
        print(f"\nValue Counts for Column '{column}':")
        print(tabulate(value_counts_data, headers=['Value', 'Count'], tablefmt='pretty'))

def print_duplicated_rows(df):
    # Get the count of duplicated rows
    duplicated_count = df.duplicated().sum()
    
    # Print the duplicated rows count
    print("\nCount of Duplicated Rows:")
    print(tabulate([['Duplicated Rows', duplicated_count]], headers=['Description', 'Count'], tablefmt='pretty'))

def print_data_types(df):
    # Get the data types of each column
    dtype_data = [(column, str(df[column].dtype)) for column in df.columns]
    
    # Print the data types table
    print("\nData Types of Columns:")
    print(tabulate(dtype_data, headers=['Column Name', 'Data Type'], tablefmt='pretty'))


def print_dataframe(df):
    return display(df)

def print_value_counts_column(df,column):
    value_counts = df[column].value_counts()
    value_counts_data = [(index, count) for index, count in value_counts.items()]
    print(f"\nValue Counts for Column '{column}':")
    print(tabulate(value_counts_data, headers=['Value', 'Count'], tablefmt='pretty'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_dataframe(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_dataframe_columns(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_dataframe_shape(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_duplicated_rows(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_data_types(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_missing_percentage(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_value_counts_column(employee, 'EmployeePosition')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_value_counts_column(employee,'EmployeePracticeGroupADP')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_nunique(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd


# Step 1: Group by ClientID and count occurrences
client_id_counts = employee.groupby('ClientID').size().reset_index(name='Count')

# Step 2: Check for repeating ClientIDs
repeating_ids = client_id_counts[client_id_counts['Count'] > 1]['ClientID']

# Step 3: Filter the original DataFrame to include only repeating ClientIDs
repeating_clients = employee[employee['ClientID'].isin(repeating_ids)]

# Step 4: Check if ClientNames are the same for repeating ClientIDs
client_name_check = repeating_clients.groupby('ClientID')['ClientName'].nunique().reset_index()
client_name_check.columns = ['ClientID', 'UniqueNameCount']

# Merge the counts with the unique name count
result = pd.merge(repeating_clients, client_name_check, on='ClientID')

# # Display the results
# print("Repeating Client IDs and their details:")
# print(result)

# # Check for ClientIDs where ClientNames are different
# different_names = result[result['UniqueNameCount'] > 1]

# if not different_names.empty:
#     print("\nClient IDs with different names:")
#     print(different_names)
# else:
#     print("\nAll repeating Client IDs have consistent names.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find ClientIDs that repeat more than once
repeating_client_ids = employee['ClientID'].value_counts()[employee['ClientID'].value_counts() > 1].index

# Filter the dataframe for these repeating ClientIDs
repeating_employees = employee[employee['ClientID'].isin(repeating_client_ids)]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

repeating_employees[['ClientID','ClientName']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_dataframe_columns(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_nunique(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_value_counts_column(employee,'Worked')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee['Worked'].value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee['ClientAddressCity'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee['RevenueTotal'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee['ClientAddressCity'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee['RevenueTotal'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee['ProjectPracticeGroup'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee['ProjectBusinessUnit'].value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee.isnull().sum()/employee.shape[0]* 100

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_unique(employee)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Projects


# CELL ********************

print_dataframe(project)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_dataframe_columns(project)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_dataframe_shape(project)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_missing_percentage(project)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_data_types(project)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_duplicated_rows(project)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_nunique(project)

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
