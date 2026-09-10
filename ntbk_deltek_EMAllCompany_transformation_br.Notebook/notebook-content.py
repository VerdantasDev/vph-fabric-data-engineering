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

from pyspark.sql.functions import lit, col, trim, count, concat, StringType, when
import pyspark.sql.functions as F
from pyspark.sql import Window
!pip install us
import us

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load tables**

# CELL ********************

df_EMAllCompany = load_table(lakehouse_path, EMAllCompany)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_CFGMainData = load_table(lakehouse_path, "br_deltek_CFGMainData_v3").select("Company","FirmName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df_EMAllCompany.withColumn("is_duplicate", F.count("Employee").over(Window.partitionBy("Employee")) > 1)

df_duplicates = df.filter(F.col("is_duplicate"))
df_non_duplicates = df.filter(~F.col("is_duplicate"))

window_spec = Window.partitionBy("Employee")

# Step 3: Add a column to mark if `EmployeeCompany` has "001" for any of the rows
df_with_has_001 = df_duplicates.withColumn(
    "has_001", F.max(F.when(F.col("EmployeeCompany") == "001", 1).otherwise(0)).over(window_spec)
)

# Step 4: Propagate the `has_001` flag as `True` for all rows if any row has "001"
df_with_has_001 = df_with_has_001.withColumn(
    "has_001", F.col("has_001") == 1
)

# Step 5: Separate DataFrames based on `has_001` column
df_has_001 = df_with_has_001.filter(F.col("has_001"))
df_no_001 = df_with_has_001.filter(~F.col("has_001"))
df_001_filtered = df_has_001.filter(F.col("EmployeeCompany") == "001")

df_final = df_non_duplicates.unionByName(df_001_filtered.drop('has_001'))
df_final = df_final.drop('is_duplicate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_final.withColumn(
    "EmployeeNameDeltek",
    F.concat_ws(
        " ",
        F.when(F.col("LastName").isNotNull() & (F.col("LastName") != ""), F.concat(F.col("LastName"), F.lit(",")))
         .otherwise(F.lit("")),
        F.when(F.col("FirstName").isNotNull() & (F.col("FirstName") != ""), F.col("FirstName"))
         .otherwise(F.lit("")),
        F.when(F.col("MiddleName").isNotNull() & (F.col("MiddleName") != ""), F.col("MiddleName"))
         .otherwise(F.lit(""))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_final.withColumn(
    "EmployeeAddress",
    F.concat_ws(
        " ",
        F.when(F.col("Address1").isNotNull() & (F.col("Address1") != ""), F.col("Address1"))
         .otherwise(F.lit("")),
        F.when(F.col("Address2").isNotNull() & (F.col("Address2") != ""), F.col("Address2"))
         .otherwise(F.lit("")),
        F.when(F.col("Address3").isNotNull() & (F.col("Address3") != ""), F.col("Address3"))
         .otherwise(F.lit(""))
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Map State codes to their full names
state_mapping = {state.abbr: state.name for state in us.states.STATES}

# Broadcast the dictionary to all worker nodes
state_mapping_broadcast = spark.sparkContext.broadcast(state_mapping)

# Define a UDF that uses the broadcasted dictionary
def abbrev_to_full_name(abbrev):
    return state_mapping_broadcast.value.get(abbrev)

# Register the UDF in Spark
abbrev_to_full_name_udf = F.udf(abbrev_to_full_name, StringType())

# Apply the UDF to the 'State_Abbreviation' column
df_mapped = df_final.withColumn("State", abbrev_to_full_name_udf(F.col("State")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mapped = df_mapped.join(df_CFGMainData.withColumnRenamed('Company', 'EmployeeCompany')
                                      .withColumnRenamed('FirmName', 'EmployeeCompanyName'),
                           on='EmployeeCompany', how='left')

df_mapped = df_mapped.join(df_CFGMainData.withColumnRenamed('Company', 'HomeCompany')
                                      .withColumnRenamed('FirmName', 'HomeCompanyName'),
                           on='HomeCompany', how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_to_map = df_mapped.select("Employee","EmployeeNameDeltek").withColumnRenamed("Employee","Employee_ref").withColumnRenamed("EmployeeNameDeltek","SupervisorNameDeltek")
df_mapped2 = df_mapped.join(df_to_map, df_mapped['Supervisor'] == df_to_map['Employee_ref'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mapped2 = df_mapped2.withColumn('Status',
    when(col('Status') == 'A', 'Active').when(col('Status') == 'I', 'Inactive').when(col('Status') == 'T', 'Terminated')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mapped2 = df_mapped2.withColumn('PayType',
    when(col('PayType') == 'H', 'Hourly').when(col('PayType') == 'S', 'Salaried')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mapped_final = df_mapped2[[
 'Employee',
 'EmployeeNameDeltek',
 'HomeCompany',
 'HomeCompanyName',
 'EmployeeCompany',
 'EmployeeCompanyName',
 'EmployeeAddress',
 'City',
 'State',
 'ZIP',
 'Country',
 'EMail',
 'Title',
 'PreferredName',
 'TargetRatio',
 'UtilizationRatio',
 'PIMID',
 'JobCostRate',
 'JobCostType',
 'HoursPerDay',
 'HireDate',
 'RaiseDate',
 'Status',
 'Type',
 'BillingCategory',
 'PayType',
 'ADPFileNumber',
 'ADPCompanyCode',
 'ProvCostRate',
 'ProvBillRate',
 'TerminationDate',
 'UseTotalHrsAsStd',
 'YearsOtherFirms',
 'Supervisor',
 'SupervisorNameDeltek',
 'PriorYearsFirm',
 'BillingPool',
 'Suffix',
 'CreateDate',
 'Org',
 'Region',
 'TKGroup',
 'EKGroup']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mapped_final.select("Employee").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_mapped_final, lakehouse_path, s_EMAllCompany, overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
