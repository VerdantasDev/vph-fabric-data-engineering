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

%run ntbk_deltek_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ntbk_deltek_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, row_number, when

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Loading the tables**

# CELL ********************

df_PR = load_table(lakehouse_path, s_PR)
df_ProjectCustomTabFields = load_table(lakehouse_path, s_ProjectCustomTabFields).withColumnRenamed("WBS1","WBS1_Cust").withColumnRenamed("CreateDate","CreateDate_Cust").withColumnRenamed("ModDate","ModDate_Cust")
df_V_RevRecDetailData = load_table(lakehouse_path, s_V_RevRecDetailData).withColumnRenamed("WBS1","WBS1_Rev").withColumnRenamed("CreateDate","CreateDate_Rev").withColumnRenamed("ModDate","ModDate_Rev")
# df_AR = load_table(lakehouse_path, s_AR)
# df_LedgerAR = load_table(lakehouse_path, s_LedgerAR)
# df_V_UnbilledDetailData_BI = load_table(lakehouse_path, s_V_UnbilledDetailData_BI)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp = load_table(lakehouse_path, s_EMAllCompany).select("Employee","EmployeenameDeltek").withColumnRenamed("Employee","EmployeeID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Joins**

# CELL ********************

df_joined1 = df_PR.join(df_ProjectCustomTabFields, df_PR['WBS1'] == df_ProjectCustomTabFields['WBS1_Cust'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined2 = df_joined1.join(df_V_RevRecDetailData, df_joined1['WBS1'] == df_V_RevRecDetailData['WBS1_Rev'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined3 = df_joined2 \
    .join(df_emp, df_joined2.Principal == df_emp.EmployeeID, "left") \
    .withColumnRenamed("EmployeeNameDeltek", "ProjectPrincipalName") \
    .drop("EmployeeID") \
    .join(df_emp, df_joined2.ProjMgr == df_emp.EmployeeID, "left") \
    .withColumnRenamed("EmployeeNameDeltek", "ProjectManagerName") \
    .drop("EmployeeID") \
    .join(df_emp, df_joined2.Supervisor == df_emp.EmployeeID, "left") \
    .withColumnRenamed("EmployeeNameDeltek", "ProjectSupervisorName") \
    .drop("EmployeeID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined3 = df_joined3.withColumn('Status',
    when(col('Status') == 'A', 'Active').when(col('Status') == 'I', 'Inactive').when(col('Status') == 'T', 'Terminated')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_joined3[['WBS1',
 'Name',
 'ChargeType',
 'SubLevel',
 'Principal',
 'ProjectPrincipalName',
 'ProjMgr',
 'ProjectManagerName',
 'Supervisor',
 'ProjectSupervisorName',
 'ClientID',
 'CLAddress',
 'Fee',
 'ConsultFee',
 'Status',
 'RevType',
 'MultAmt',
 'Org',
 'StartDate',
 'EndDate',
 'BillWBS1',
 'BillWBS2',
 'BillWBS3',
 'Description',
 'ContactID',
 'CLBillingAddr',
 'LongName',
 'Address1',
 'Address2',
 'Address3',
 'City',
 'State',
 'Zip',
 'County',
 'Country',
 'ProjectType',
 'EstCompletionDate',
 'ActCompletionDate',
 'ContractDate',
 'TotalProjectCost',
 'OpportunityID',
 'BillingClientID',
 'BillingContactID',
 'EMail',
 'ProposalWBS1',
 'ProfServicesComplDate',
 'ConstComplDate',
 'CreateDate',
 'ModDate',
 'PIMID',
 'ProjectAddressStreet',
 'WBS1_Cust',
 'Select1',
 'Select6',
 'CustLatitude',
 'CustLongitude',
 'CustDivision',
 'CustTypeofOwner',
 'CustTypeSite',
 'CustMunicipality',
 'CustRegionalOffice',
 'CustOfficeWorkLocation',
 'CustBillingType',
 'CustLegacy',
 'CustPRPracticeService',
 'CustArea',
 'CustBusinessDeveloperLead',
 'CustMarketingCoordinator',
 'CustPracticeService_PR',
 'CustPractice',
 'CustService',
 'CustClientFolderID',
 'CustClientFolderCreated',
 'Cust_PWCounty',
 'CustDept_Reporting',
 'Cust2023Org',
 'Cust2024Org',
 'CreateDate_Cust',
 'ModDate_Cust',
 'WBS1_Rev',
 'ContractAmt_sum',
 'Rev_JTD_sum',
 'Billed_JTD_sum',
 'UnBilled_JTD_sum',
 'Backlog_sum',
 'JTDBillRate_avg',
 'AR_sum',
 'Billed_YTD_sum',
 'Rev_CUR_sum',
 'Billed_CUR_sum',
 'CURBillRate_avg',
 'MaxLaborDate_max',
 'TotalRevenueBudget_sum',
 'Unbilled_CUR_Revenue_sum',
 'LaborCost_YTD_sum',
 'Subtotal_SubsandExpense_sum',
 'NetRev_JTD_sum',
 'NETRev_YTD_sum',
 'LaborCost_JTD_sum',
 'LaborCost_CUR_sum',
 'MarkuponSubsandExpenses_sum',
 'NetRevenueBudget_sum',
 'NetRev_CUR_sum',
 'CURCost_sum',
 'CurProfitPercent_avg']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Ingestion**

# CELL ********************

# summary_df = analyze_dataframe(df_joined3)
# display(summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_final, lakehouse_path, br_Projects, overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
