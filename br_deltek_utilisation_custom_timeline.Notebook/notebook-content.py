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

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, isnan, when, count, to_date, current_date, datediff, max
from datetime import timedelta
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Reading the tables from lakehouse**

# CELL ********************

spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {
    "EmployeeCustomTabFields": f"{lakehouse_path}/br_deltek_EmployeeCustomTabFields",
    "LD" : f"{lakehouse_path}/br_deltek_LD",
    "CFGTimeAnalysis" : f"{lakehouse_path}/br_deltek_CFGTimeAnalysis",
    "CFGTimeAnalysisHeadings" : f"{lakehouse_path}/br_deltek_CFGTimeAnalysisHeadings",
    "EMAllCompany":f"{lakehouse_path}/br_deltek_EMAllCompany"
}

df_EmployeeCustomTabFields = spark.read.format("delta").load(table_paths["EmployeeCustomTabFields"]).withColumnRenamed("Employee","Employee_Cust")
df_LD = spark.read.format("delta").load(table_paths["LD"]).withColumnRenamed("Employee", "Employee_LD")
df_CFGTimeAnalysis = spark.read.format("delta").load(table_paths["CFGTimeAnalysis"])
df_CFGTimeAnalysisHeadings = spark.read.format("delta").load(table_paths["CFGTimeAnalysisHeadings"]).withColumnRenamed('ReportColumn','ReportColumn_Head')
df_EMAllCompany = spark.read.format("delta").load(table_paths["EMAllCompany"]).withColumnRenamed("Employee","Employee_All")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Limiting data to required columns
df_EmployeeCustomTabFields = df_EmployeeCustomTabFields[['Employee_Cust', 'CustDept_Reporting','CustStraightTimeOT','CustHourlyorSalary']]
df_LD = df_LD[['Employee_LD', 'WBS1','TransDate','ChargeType','RegHrs','OvtHrs','SpecialOvtHrs']]
df_EMAllCompany = df_EMAllCompany[['Employee_All','FirstName','MiddleName','LastName','TargetRatio','EmployeeCompany','Supervisor']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Timeline Filtering**

# CELL ********************

latest_date = df_LD.select(max("TransDate")).first()[0]
cutoff_date = latest_date - timedelta(days = utilization_timeline)
df_LD_filtered = df_LD.filter((col("TransDate") >= cutoff_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Identify missing Employee_LD values
missing_employees = df_LD.select("Employee_LD").subtract(df_LD_filtered.select("Employee_LD")).distinct()

# Create placeholder DataFrame with missing Employee_LD values
placeholders = missing_employees.withColumn("WBS1", F.lit(None).cast("string")) \
                                .withColumn("TransDate", F.lit(None).cast("date")) \
                                .withColumn("ChargeType", F.lit(None).cast("string")) \
                                .withColumn("RegHrs", F.lit(0).cast("double")) \
                                .withColumn("OvtHrs", F.lit(0).cast("double")) \
                                .withColumn("SpecialOvtHrs", F.lit(0).cast("double"))

# Step 4: Union the placeholder DataFrame with df_final_lastyear
df_LD_final = df_LD_filtered.unionByName(placeholders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Joining the tables**

# CELL ********************

joined_analysis_heading = df_CFGTimeAnalysis.join(df_CFGTimeAnalysisHeadings, df_CFGTimeAnalysis.ReportColumn == df_CFGTimeAnalysisHeadings.ReportColumn_Head, "left")
joined_analysis_heading = joined_analysis_heading.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_LD_EmployeeCust = df_LD_final.join(df_EmployeeCustomTabFields, df_LD_final.Employee_LD == df_EmployeeCustomTabFields.Employee_Cust, "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_LD_EmployeeCust_EMallCompany = joined_LD_EmployeeCust.join(df_EMAllCompany, joined_LD_EmployeeCust.Employee_LD == df_EMAllCompany.Employee_All, "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_join = joined_LD_EmployeeCust_EMallCompany.join(joined_analysis_heading, joined_LD_EmployeeCust_EMallCompany.WBS1 == joined_analysis_heading.StartWBS1, "left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Adding Columns**

# CELL ********************

df_final_join.select("Employee_LD").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# total hours
df_final_join = df_final_join.withColumn(
    "Total_Hours",
    df_final_join["RegHrs"] + 
    df_final_join["OvtHrs"] + 
    df_final_join["SpecialOvtHrs"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# standard hours
def calculate_standard_hours(row):
    if row['CustHourlyorSalary'] == 'H':
        return row['Total_Hours']
    elif row['CustHourlyorSalary'] == 'S' and row['CustStraightTimeOT'] == "Y":
        return row['Total_Hours']
    elif row['CustHourlyorSalary'] == 'S' and row['CustStraightTimeOT'] == "N":
        return 40
    else:
        return 0

df_final_join = df_final_join.withColumn(
    'Standard_Hours',
    F.udf(calculate_standard_hours)(F.struct('CustHourlyorSalary', 'CustStraightTimeOT', 'Total_Hours'))  
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def standard_hours_flag(row):
    if row['CustHourlyorSalary'] == 'S' and row['CustStraightTimeOT'] == "N":
        return 'NO'
    else:
        return 'YES'
 
df_final_join = df_final_join.withColumn(
    'Standard_Hours_Flag',
    F.udf(standard_hours_flag)(F.struct('CustHourlyorSalary', 'CustStraightTimeOT'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the function
def calculate_direct_hours(row):
    if row['ChargeType'] == 'R':
        return row['Total_hours']
    else:
        return 0

df_final_join = df_final_join.withColumn(
    'Direct_Hours',
    F.udf(calculate_direct_hours)(F.struct('ChargeType', 'Total_hours'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculate_proposal_hours(row):
    if row['ChargeType'] == 'P':
        return row['Total_hours']
    else:
        return 0

df_final_join = df_final_join.withColumn(
    'Proposal_Hours',
    F.udf(calculate_proposal_hours)(F.struct('ChargeType', 'Total_hours'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculate_overhead_hours(row):
    if row['ChargeType'] == 'H':
        return row['Total_hours']
    else:
        return 0

df_final_join = df_final_join.withColumn(
    'Overhead_Hours',
    F.udf(calculate_overhead_hours)(F.struct('ChargeType', 'Total_hours'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#PTO Paid time out
def calculate_PTO(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'PTO':
        return row['Total_hours']
    else:
        return 0
df_final_join = df_final_join.withColumn(
    'PTO',
    F.udf(calculate_PTO)(F.struct('ChargeType','Label','Total_hours'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Vacation 
def calculate_Vacation(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'Vacation':
        return row['Total_hours']
    else:
        return 0
df_final_join = df_final_join.withColumn(
    'Vacation',
    F.udf(calculate_Vacation)(F.struct('ChargeType','Label','Total_hours'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Sick
def calculate_Sick(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'Sick Oth LV':
        return row['Total_hours']
    else:
        return 0
df_final_join = df_final_join.withColumn(
    'Sick',
    F.udf(calculate_Sick)(F.struct('ChargeType','Label','Total_hours'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Holiday
def calculate_Holiday(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'Holiday':
        return row['Total_hours']
    else:
        return 0
df_final_join = df_final_join.withColumn(
    'Holiday',
    F.udf(calculate_Holiday)(F.struct('ChargeType','Label','Total_hours'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Floating Holiday
def calculate_Float_Holiday(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'Float Holday':
        return row['Total_hours']
    else:
        return 0
df_final_join = df_final_join.withColumn(
    'Floating Holiday',
    F.udf(calculate_Float_Holiday)(F.struct('ChargeType','Label','Total_hours'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# total hours
df_final_join = df_final_join.withColumn(
    "Total_Benefit_Hours",
    df_final_join["PTO"] + 
    df_final_join["Vacation"] + 
    df_final_join["Sick"] +
    df_final_join["Holiday"] +
    df_final_join["Floating Holiday"] 
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Rolling up data at employee level**

# CELL ********************

df_fix_stdhrs = df_final_join.filter(F.col('Standard_Hours_Flag') == 'YES')
df_vary_stdhrs = df_final_join.filter(F.col('Standard_Hours_Flag') == 'NO')

df_fix_stdhrs_rollup = df_fix_stdhrs.groupBy('Employee_LD').agg(
    F.first('TargetRatio').alias('TargetRatio'),
    F.sum('Standard_Hours').alias('Standard_Hours'),
    F.sum('Direct_Hours').alias('Direct_Hours'),
    F.sum('Proposal_Hours').alias('Proposal_Hours'),
    F.sum('Overhead_Hours').alias('Overhead_Hours'),
    F.sum('Total_Benefit_Hours').alias('Total_Benefit_Hours')
)

# Aggregate the df_no DataFrame
df_vary_stdhrs_rollup = df_vary_stdhrs.groupBy('Employee_LD').agg(
    F.first('TargetRatio').alias('TargetRatio'),
    F.first('Standard_Hours').alias('Standard_Hours'),
    F.sum('Direct_Hours').alias('Direct_Hours'),
    F.sum('Proposal_Hours').alias('Proposal_Hours'),
    F.sum('Overhead_Hours').alias('Overhead_Hours'),
    F.sum('Total_Benefit_Hours').alias('Total_Benefit_Hours')
)

# Merge the two aggregated DataFrames
df_final_rollup = df_fix_stdhrs_rollup.unionByName(df_vary_stdhrs_rollup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_rollup = df_final_rollup.withColumnRenamed("TargetRatio","Target_UTE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculate_ute(df):
    df = df.withColumn("UTE", (F.col("Direct_Hours") / (F.col("Standard_Hours") - F.col("Total_Benefit_Hours"))) * 100)
    return df
 
def calculate_Gap(df):
    df = df.withColumn("Gap", F.col("UTE") - F.col("Target_UTE"))
    return df
 
def calculate_Billability(df):
    df = df.withColumn("Billability", (F.col("Direct_Hours") / F.col("Standard_Hours")) * 100)
    return df
 
def calculate_Use(df):
    df = df.withColumn("Use", ((F.col("Direct_Hours") + F.col("Proposal_Hours")) / (F.col("Standard_Hours") - F.col("Total_Benefit_Hours"))) * 100)
    return df

df_final_rollup = calculate_ute(df_final_rollup)
df_final_rollup = calculate_Gap(df_final_rollup)
df_final_rollup = calculate_Billability(df_final_rollup)
df_final_rollup = calculate_Use(df_final_rollup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final_rollup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_table_name = 'br_deltek_utilisation_last' + str(utilization_timeline) + 'days'

# Write DataFrame to Delta Lake table
delta_table_path = "Tables/" + final_table_name # Example Delta Lake table path in Azure Data Lake Storage

# Write data to Delta Lake table
df_final_rollup.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
