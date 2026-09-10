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

from pyspark.sql.functions import regexp_replace, col, when

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Loading the tables**

# CELL ********************

df_EMAllCompany = load_table(lakehouse_path, s_EMAllCompany).withColumn("EmployeeIDDeltek", col("Employee")).withColumnRenamed("Type","EmployeeTypeCode")
df_EMCompany = load_table(lakehouse_path, s_EMCompany).withColumnRenamed("Employee","Employee_EMCompany")
df_EmployeeCustomTabFields = load_table(lakehouse_path, s_EmployeeCustomTabFields).withColumnRenamed("Employee","Employee_Cust")
df_CFGEmployeeTypeDescriptions = load_table(lakehouse_path, s_CFGEmployeeTypeDescriptions).select("Type","Label")
df_ADP = load_table(lakehouse_path, "mvp_adp_data").withColumn("EmployeeIDADP", col("EmployeeNumber"))
df_week_utilization = load_table(lakehouse_path, "br_deltek_employee_weekly_utilization").withColumnRenamed("Employee_LD","Employee_UTE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ADP = df_ADP.withColumn("EmployeeNumber", regexp_replace("EmployeeNumber", "^V6A0*", ""))
df_EMCompany = df_EMCompany.withColumn("Employee_EMCompany", regexp_replace("Employee_EMCompany", "^'?0*", ""))
df_EmployeeCustomTabFields = df_EmployeeCustomTabFields.withColumn("Employee_Cust", regexp_replace("Employee_Cust", "^'?0*", ""))
df_EMAllCompany = df_EMAllCompany.withColumn("Employee", regexp_replace("Employee", "^'?0*", ""))
df_week_utilization = df_week_utilization.withColumn("Employee_UTE", regexp_replace("Employee_UTE", "^'?0*", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_rename_dict = {
    'EmployeeName': 'EmployeeNameADP',
    'EmploymentStatus': 'EmploymentStatusADP',
    'Position': 'EmployeePositionADP',
    'Title': 'EmployeeTitleADP',
    'BusinessUnit': 'EmployeeBusinessUnitADP',
    'Department': 'EmployeeDepartmentADP',
    'PracticeGroup': 'EmployeePracticeGroupADP'
}

for old_col, new_col in adp_rename_dict.items():
    df_ADP = df_ADP.withColumnRenamed(old_col, new_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined1 = df_EMAllCompany.join(df_EMCompany, df_EMAllCompany['Employee'] == df_EMCompany['Employee_EMCompany'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined2 = df_joined1.join(df_EmployeeCustomTabFields, df_joined1['Employee'] == df_EmployeeCustomTabFields['Employee_Cust'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined3 = df_joined2.join(df_ADP, df_joined2['Employee'] == df_ADP['EmployeeNumber'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined4 = df_joined3.join(df_CFGEmployeeTypeDescriptions, df_joined3['EmployeeTypeCode'] == df_CFGEmployeeTypeDescriptions['Type'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined5 = df_joined4.join(df_week_utilization, df_joined4['Employee'] == df_week_utilization['Employee_UTE'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined5 = df_joined5.withColumn("UtilizationStatus", when(col("Gap") < 0, "Underutilized")
                              .when(col("Gap") > 0, "Overutilized")
                              .otherwise("Shared Services"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltek_rename_dict = {
    'Employee': 'EmployeeID_modified',
    'EmployeeNameDeltek': 'EmployeeNameDeltek',
    'HomeCompany': 'EmployeeHomeCompany',
    'HomeCompanyName': 'EmployeeHomeCompanyName',
    'EmployeeCompany': 'EmployeeCompany',
    'EmployeeCompanyName': 'EmployeeCompanyName',
    'EmployeeAddress': 'EmployeeAddressStreet',
    'City': 'EmployeeAddressCity',
    'State': 'EmployeeAddressState',
    'ZIP': 'EmployeeAddressZIP',
    'Country': 'EmployeeCountry',
    'EMail': 'EmployeeEmail',
    'Title': 'EmployeeTitleDeltek',
    'PreferredName': 'EmployeePreferredName',
    'TargetRatio': 'EmployeeTargetRatio',
    'UtilizationRatio': 'EmployeeUtilizationRatio',
    'PIMID': 'PIMID',
    'JobCostRate': 'EmployeeJobCostRate',
    'JobCostType': 'EmployeeJobCostType',
    'HoursPerDay': 'HoursPerDay',
    'HireDate': 'EmployeeHireDate',
    'RaiseDate': 'EmployeeRaiseDate',
    'Status': 'EmploymentStatusDeltek',
    'EmployeeTypeCode': 'EmployeeTypeCode',
    'BillingCategory': 'EmployeeBillingCategory',
    'PayType': 'EmployeePayType',
    'ADPFileNumber': 'ADPFileNumber',
    'ADPCompanyCode': 'ADPCompanyCode',
    'ProvCostRate': 'EmployeeProvCostRate',
    'ProvBillRate': 'EmployeeProvBillRate',
    'TerminationDate': 'EmployeeTerminationDate',
    'UseTotalHrsAsStd': 'UseTotalHrsAsStd',
    'YearsOtherFirms': 'YearsOtherFirms',
    'Supervisor': 'EmployeeSupervisor',
    'SupervisorNameDeltek': 'EmployeeSupervisorNameDeltek',
    'PriorYearsFirm': 'PriorYearsFirm',
    'BillingPool': 'EmployeeBillingPool',
    'Suffix': 'Suffix',
    'CreateDate': 'CreateDate',
    'Org': 'Org',
    'Region': 'Region',
    'TKGroup': 'TKGroup',
    'EKGroup': 'EKGroup',
    'Employee_EMCompany': 'Employee_EMCompany',
    'PayRate': 'EmployeePayRate',
    'PayRateMeth': 'EmployeePayRateMeth',
    'PayRateTableNo': 'EmployeePayRateTableNo',
    'Employee_Cust': 'Employee_Cust',
    'Select3': 'EmployeePracticeGroupDeltek',
    'associateOID': 'associateOID',
    'EmployeeNameADP': 'EmployeeNameADP',
    'EmploymentStatusADP': 'EmploymentStatusADP',
    'PositionStartDate': 'PositionStartDate',
    'EmployeePositionADP': 'EmployeePositionADP',
    'EmployeeNumber': 'EmployeeNumber',
    'VerdantasStartDate': 'VerdantasStartDate',
    'EmployeeBillType': 'EmployeeTypeADP',
    'EmployeeTitleADP': 'EmployeeTitleADP',
    'OfficeLocation': 'OfficeLocation',
    'ReportingMangaer': 'ReportingManager',
    'EmployeeBusinessUnitADP': 'EmployeeBusinessUnitADP',
    'EmployeeDepartmentADP': 'EmployeeDepartmentADP',
    'HomeDepartmentEmailList': 'HomeDepartmentEmailList',
    'LegacyCompany': 'LegacyCompany',
    'ExperienceDate': 'ExperienceDate',
    'EmployeePracticeGroupADP': 'EmployeePracticeGroupADP',
    'CertificationCode': 'CertificationCode',
    'NameOfCertification': 'NameOfCertification',
    'EmployeeCode': 'EmployeeCode',
    'licenseID': 'licenseID',
    'licenseName': 'licenseName',
    'licenseExpirationDate': 'licenseExpirationDate',
    'EmployeeIDADP': 'EmployeeIDADP',
    'Label': 'EmployeeTypeDeltek',
    'CustArea': 'EmployeeBusinessUnitDeltek',
    'CustDept_Reporting': 'EmployeeDepartmentDeltek',
    'Target_UTE': 'EmployeeTargetUtilization',
    'Standard_Hours': 'EmployeeStandardHours',
    'Direct_Hours': 'EmployeeDirectHours',
    'Proposal_Hours':'EmployeeProposalHours',
    'Overhead_Hours': 'EmployeeOverheadHours',
    'Total_Benefit_Hours': 'EmployeeTotalBenefitHours',
    'UTE': 'EmployeeUtilization',
    'Gap': 'Gap',
    'Billability': 'EmployeeBillability',
    'Use': 'EmployeeUse'
}

for old_col, new_col in deltek_rename_dict.items():
    df_joined5 = df_joined5.withColumnRenamed(old_col, new_col)

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

write_data(df_joined5.select("EmployeeIDADP",
 'EmployeeBillingCategory',
 'EmployeeUtilization',
 'UtilizationStatus').drop_duplicates(), lakehouse_path, "mvp_utilization_data", overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_joined5, lakehouse_path, br_Employees, overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
