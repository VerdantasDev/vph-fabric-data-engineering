# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# Path details
lakehouse_path = "abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Client Tables
Clendor = 'br_raw_deltek_Clendor'
CLAddress = 'br_raw_deltek_CLAddress'
ClendorProjectAssoc = 'br_raw_deltek_ClendorProjectAssoc'
CFGClientTypeDescriptions = 'br_raw_deltek_CFGClientTypeDescriptions'
Clients_ClientFolderList = 'br_raw_deltek_Clients_ClientFolderList'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transformed Client Tables
s_Clendor = 'br_deltek_s_Clendor'
s_CLAddress = 'br_deltek_s_CLAddress'
s_ClendorProjectAssoc = 'br_deltek_s_ClendorProjectAssoc'
s_CFGClientTypeDescriptions = 'br_deltek_s_CFGClientTypeDescriptions'
s_Clients_ClientFolderList = 'br_deltek_s_Clients_ClientFolderList'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Project Tables
PR = 'br_raw_deltek_PR'
ProjectCustomTabFields = 'br_raw_deltek_ProjectCustomTabFields'
LedgerAR = 'br_raw_deltek_LedgerAR'
AR = 'br_raw_deltek_Ar'
V_UnbilledDetailData_BI = 'br_raw_deltek_V_UnbilledDetailData_BI'
V_RevRecDetailData = 'br_raw_deltek_V_RevRecDetailData'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Employee Tables
EMAllCompany = 'br_raw_deltek_EMAllCompany'
EMCompany = 'br_raw_deltek_EMCompany'
LD = 'br_raw_deltek_LD'
CFGEmployeeTypeDescriptions = 'br_raw_deltek_CFGEmployeeTypeDescriptions'
EmployeeCustomTabFields = 'br_raw_deltek_EmployeeCustomTabFields'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transformed Project Tables
s_PR = 'br_deltek_s_PR'
s_ProjectCustomTabFields = 'br_deltek_s_ProjectCustomTabFields'
s_LedgerAR = 'br_deltek_s_LedgerAR'
s_AR = 'br_deltek_s_AR'
s_V_UnbilledDetailData_BI = 'br_deltek_s_V_UnbilledDetailData_BI'
s_V_RevRecDetailData = 'br_deltek_s_V_RevRecDetailData'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transformed Employee Tables
s_EMAllCompany = 'br_deltek_s_EMAllCompany'
s_EMCompany = 'br_deltek_s_EMCompany'
s_LD = 'br_deltek_s_LD'
s_CFGEmployeeTypeDescriptions = 'br_deltek_s_CFGEmployeeTypeDescriptions'
s_EmployeeCustomTabFields = 'br_deltek_s_EmployeeCustomTabFields'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

project_type_mapping = {
    'NECS01': 'General building',
    'NECS02': 'Industrial process',
    'NECS03': 'Manufacturing',
    'NECS04': 'Water Supply',
    'NECS05': 'Sewage / Solid Waste Disposal',
    'NECS06': 'Transportation',
    'NECS08': 'Power',
    'NECS09': 'Petroleum',
    'NECS10': 'Telecommunication',
    'NECS11': 'Other',
    'NECS12': 'Hazardous waste',
    'Envi0024': 'Environment'
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Joined layers
br_Clients = 'br_deltek_ClientsData'
br_Projects = 'br_deltek_ProjectsData'
br_Employees = 'br_deltek_EmployeesData'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final tables
sv_Clients = 'sv_deltek_Clients_merged'
sv_Projects = 'sv_deltek_Projects_merged'
sv_Employees = 'sv_deltek_Employees_merged'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
