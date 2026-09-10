# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

# ## **Data Loading**

# CELL ********************

summary = dict()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Clients = load_table(lakehouse_path, sv_Clients)
df_Projects = load_table(lakehouse_path, sv_Projects).withColumnRenamed("ClientID","ClientID_PR")
df_Employees = load_table(lakehouse_path, sv_Employees)
# df_ADP = load_table(lakehouse_path, "mvp_adp_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Employees.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Total Distinct Clients'] = df_Clients.select("ClientID").distinct().count()
summary['Total Distinct Projects'] = df_Projects.select("ProjectID").distinct().count()
summary['Total Distinct Employees'] = df_Employees.select("EmployeeIDDeltek").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_LD = load_table(lakehouse_path, LD).select("Employee","WBS1").drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Total Distinct Projects in LD Table'] = df_LD.select("WBS1").distinct().count()
summary['Total Distinct Employees in LD Table'] = df_LD.select("Employee").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Joins**

# CELL ********************

df_joined1 = df_Clients.join(df_Projects, df_Clients['ClientID'] == df_Projects['ClientID_PR'], how = 'inner')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Total Distinct Projects after Client-Project Inner Join'] = df_joined1.select("ProjectID").distinct().count()
summary['Total Distinct Clients after Client-Project Inner Join'] = df_joined1.select("ClientID").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined2 = df_joined1.join(df_LD, df_joined1['ProjectID'] == df_LD['WBS1'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined2.select('WBS1').distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined2.filter(df_joined2[])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql.functions import regexp_replace, col, when

# df_ADP = df_ADP.withColumn("EmployeeNumber2", regexp_replace("EmployeeNumber", "^V6A0*", ""))
# df_joined2 = df_joined2.withColumn("Employee_mod", regexp_replace("Employee", "^'?0*", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined3 = df_joined2.join(df_Employees, df_joined2['Employee'] == df_Employees['EmployeeIDDeltek'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_joined3 = df_joined2.join(df_ADP, df_joined2['Employee_mod'] == df_ADP['EmployeeNumber2'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Transformations**

# CELL ********************

from pyspark.sql import functions as F

# Add a 'Source' column that flags whether the data came from ADP or Deltek
df_tagged = df_joined3.withColumn(
    'Source', 
    F.when(F.col('EmployeeIDADP').isNotNull(), 'ADP').otherwise('Deltek')
)

# Fill columns with ADP first, then fallback to Deltek
df_filled = df_tagged.withColumn(
    'EmployeeID', F.coalesce(F.col('EmployeeIDADP'), F.col('EmployeeIDDeltek'))
).withColumn(
    'EmployeeName', F.coalesce(F.col('EmployeeNameADP'), F.col('EmployeeNameDeltek'))
).withColumn(
    'EmployeeType', F.coalesce(F.col('EmployeeTypeADP'), F.col('EmployeeTypeDeltek'))
).withColumn(
    'EmployeePracticeGroup', F.coalesce(F.col('EmployeePracticeGroupADP'), F.col('EmployeePracticeGroupDeltek'))
).withColumn(
    'EmployeeTitle', F.coalesce(F.col('EmployeeTitleADP'), F.col('EmployeeTitleDeltek'))
).withColumn(
    'EmployeeBusinessUnit', F.coalesce(F.col('EmployeeBusinessUnitADP'), F.col('EmployeeBusinessUnitDeltek'))
).withColumn(
    'EmployeeDepartment', F.coalesce(F.col('EmployeeDepartmentADP'), F.col('EmployeeDepartmentDeltek'))
).withColumn(
    'EmploymentStatus', F.coalesce(F.col('EmploymentStatusADP'), F.col('EmploymentStatusDeltek'))
)

# Drop any unnecessary columns if required
columns_to_drop = [
    'EmployeeIDADP', 'EmployeeIDDeltek', 'EmployeeNameADP', 'EmployeeNameDeltek', 
    'EmployeeTypeADP', 'EmployeeTypeDeltek', 'EmployeePracticeGroupADP', 'EmployeePracticeGroupDeltek', 
    'EmployeeTitleADP', 'EmployeeTitleDeltek', 'EmployeeBusinessUnitADP', 'EmployeeBusinessUnitDeltek', 
    'EmployeeDepartmentADP', 'EmployeeDepartmentDeltek', 'EmploymentStatusADP', 'EmploymentStatusDeltek'
]
df_final = df_filled.drop(*columns_to_drop)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_final.drop("Employee","ClientID_PR","WBS1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rename_dict = {
    "OfficeLocationADP": "EmployeeOfficeLocation",
    "EmployeeBusinessUnit": "EmployeeArea",
    "ProjectBusinessUnit": "ProjectArea"
}

df_final = df_final.select([col(col_name).alias(rename_dict[col_name]) if col_name in rename_dict else col_name for col_name in df_final.columns])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# filtered_df = df_joined3.filter((col('EmployeeNumber2').isNull()) & col('Employee_mod').isNotNull())
# display(filtered_df.select("Employee_mod").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_joined3.select("Employee_mod").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_joined3.select("EmployeeNumber2").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_final.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_final.select(
    [when(col(c) == "", "NA").otherwise(col(c)).alias(c) for c in df_final.columns]
)
df_final = df_final.fillna("NA")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_final, lakehouse_path, "mvp_project_index_data", overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Testing**

# CELL ********************

test_df = load_table(lakehouse_path, "mvp_project_index_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adf = analyze_dataframe(test_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(adf)

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
