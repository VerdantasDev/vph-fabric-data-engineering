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
import json


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
    "adp_deltek": f"{lakehouse_path}/sv_deltek_Employees_v4"
}
# Read tables into DataFrames
adp_deltek = spark.read.format("delta").load(table_paths["adp_deltek"])
adp_deltek = adp_deltek.toPandas()

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

adp_deltek.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define a function to process the data and structure it into JSON format
def excel_to_json(df):
    clients = {}

    for _, row in df.iterrows():
        client_id = row['ClientID']

        # If the client is not already in the dictionary, add it
        if client_id not in clients:
            clients[client_id] = {
                'ClientID': row['ClientID'],
                'Client name': row['ClientName'],
                'Worked': row['Worked'],
                'Client Address (City)': row['ClientAddressCity'],
                'Client Address (State)': row['ClientAddressState'],
                'Projects': []
            }

        # Extract project information
        project_number = row['ProjectID']
        existing_project = None

        # Check if the project already exists
        for project in clients[client_id]['Projects']:
            if project['Project Number'] == project_number:
                existing_project = project
                break

        # If the project does not exist, create a new one
        if not existing_project:
            project = {
                'Project Number': row['ProjectID'],
                'Project Name': row['ProjectName'],
                'Status (Active/Dormant)': row['ProjectStatus'],
                'Total Revenue': row['RevenueTotal'],
                'Project Address (Street #) Deltek --> if NA Proposal or Report': row['ProjectAddressStreet'],
                'Project Address (City) Deltek --> if NA Proposal or Report': row['ProjectAddressCity'],
                'Project Address (State) Deltek --> if NA Proposal or Report': row['ProjectAddressState'],
                'Practice Group': row['ProjectPracticeGroup'],
                'Business Unit (Area)': row['ProjectBusinessUnit'],
                'Employees': []
            }
            clients[client_id]['Projects'].append(project)
            existing_project = project

        # Add employee information if available
        if pd.notna(row['EmployeeID']):
            employee = {
                'Employee ID': row['EmployeeID'],
                'Employee Name': row['EmployeeName'],
                'Employee Position': row['EmployeePosition'],
                'Employee practicegroup': row['EmployeePracticeGroupADP'],
                'Employee Business unit': row['EmployeeBusinessUnit'],
                'Certification Code': row['CertificationCode'],
                'Certification Name (ADP)': row['NameOfCertification'],
            }
            existing_project['Employees'].append(employee)

    return json.dumps(clients, indent=4)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display the first 1000 characters of the JSON
print(df_json[:1000002])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import collect_list, struct, when, col, lit
# import json

# # # Initialize a Spark session
# # spark = SparkSession.builder.appName("ExcelToJson").getOrCreate()

# def excel_to_json(df):
#     # Define the structure for employees
#     employees_df = df.where(col("EmployeeID").isNotNull()).select(
#         col("ProjectID"),
#         struct(
#             col("EmployeeID").alias("Employee ID"),
#             col("EmployeeName").alias("Employee Name"),
#             col("EmployeePosition").alias("Employee Position"),
#             col("EmployeePracticeGroupADP").alias("Employee practicegroup"),
#             col("EmployeeBusinessUnit").alias("Employee Business unit"),
#             col("CertificationCode").alias("Certification Code"),
#             col("NameOfCertification").alias("Certification Name (ADP)")
#         ).alias("EmployeeInfo")
#     )

#     # Group employees by project
#     employees_grouped = employees_df.groupBy("ProjectID").agg(
#         collect_list("EmployeeInfo").alias("Employees")
#     )

#     # Define the structure for projects
#     projects_df = df.join(employees_grouped, on="ProjectID", how="left").select(
#         col("ClientID"),
#         struct(
#             col("ProjectID").alias("Project Number"),
#             col("ProjectName").alias("Project Name"),
#             col("ProjectStatus").alias("Status (Active/Dormant)"),
#             col("RevenueTotal").alias("Total Revenue"),
#             col("ProjectAddressStreet").alias("Project Address (Street #) Deltek --> if NA Proposal or Report"),
#             col("ProjectAddressCity").alias("Project Address (City) Deltek --> if NA Proposal or Report"),
#             col("ProjectAddressState").alias("Project Address (State) Deltek --> if NA Proposal or Report"),
#             col("ProjectPracticeGroup").alias("Practice Group"),
#             col("ProjectBusinessUnit").alias("Business Unit (Area)"),
#             when(col("Employees").isNotNull(), col("Employees")).otherwise(lit([])).alias("Employees")
#         ).alias("ProjectInfo")
#     )

#     # Group projects by client
#     projects_grouped = projects_df.groupBy("ClientID").agg(
#         collect_list("ProjectInfo").alias("Projects")
#     )

#     # Define the structure for clients
#     clients_df = df.join(projects_grouped, on="ClientID", how="left").select(
#         col("ClientID"),
#         col("ClientName").alias("Client name"),
#         col("Worked"),
#         col("ClientAddressCity").alias("Client Address (City)"),
#         col("ClientAddressState").alias("Client Address (State)"),
#         col("Projects")
#     ).distinct()

#     # Convert the final dataframe to a JSON string
#     clients_json = clients_df.toJSON().collect()
    
#     # Combine all JSON strings into one JSON object
#     final_json = json.dumps([json.loads(client) for client in clients_json], indent=4)
    
#     return final_json


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
