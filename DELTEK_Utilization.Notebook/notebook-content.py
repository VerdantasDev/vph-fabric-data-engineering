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
    "EmployeeCustomTabFields": f"{lakehouse_path}/br_deltek_EmployeeCustomTabFields",
    "LD" : f"{lakehouse_path}/br_deltek_LD",
    "CFGTimeAnalysis" : f"{lakehouse_path}/br_deltek_CFGTimeAnalysis",
    "CFGTimeAnalysisHeadings" : f"{lakehouse_path}/br_deltek_CFGTimeAnalysisHeadings",
    "EMAllCompany":f"{lakehouse_path}/br_deltek_EMAllCompany"
}

EmployeeCustomTabFields = spark.read.format("delta").load(table_paths["EmployeeCustomTabFields"])
LD = spark.read.format("delta").load(table_paths["LD"])
CFGTimeAnalysis = spark.read.format("delta").load(table_paths["CFGTimeAnalysis"])
CFGTimeAnalysisHeadings = spark.read.format("delta").load(table_paths["CFGTimeAnalysisHeadings"])
EMAllCompany = spark.read.format("delta").load(table_paths["EMAllCompany"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

EmployeeCustomTabFields = EmployeeCustomTabFields[['Employee', 'CustDept_Reporting','CustStraightTimeOT','CustHourlyorSalary']]
LD = LD[['Employee', 'WBS1','TransDate','ChargeType','RegHrs','OvtHrs','SpecialOvtHrs']]
EMAllCompany = EMAllCompany[['Employee','FirstName','MiddleName','LastName','TargetRatio','EmployeeCompany','Supervisor']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def print_shapes(*dfs):
    for df in dfs:
        num_rows = df.count()
        num_columns = len(df.schema.names)
        print(f"Shape of {df}: {num_rows} rows, {num_columns} columns")

def drop_column(df, col_name):
    return df.drop(col_name)

def rename_column(df, old_col_name, new_col_name):
    return df.withColumnRenamed(old_col_name, new_col_name)

def missing_values_percentage_column(df, column_name):
    total_count = df.count()
    missing_count = df.filter(col(column_name).isNull() | isnan(col(column_name))).count()
    missing_percentage = (missing_count / total_count) * 100
    return f"{missing_percentage:.2f}%"

def unique(df, column_name):
    return df.select(column_name).distinct().show()

def nunique(df, column_name):
    return df.select(column_name).distinct().count()

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

LD = LD.withColumnRenamed("Employee", "Employee_1")
EmployeeCustomTabFields = EmployeeCustomTabFields.withColumnRenamed("Employee","Employee_2")
EMAllCompany = EMAllCompany.withColumnRenamed("Employee","Employee_3")

joined_LD_EmployeeCust = LD.join(EmployeeCustomTabFields, LD.Employee_1 == EmployeeCustomTabFields.Employee_2, "left")
# print_shapes(joined_LD_EmployeeCust)

joined_LD_EmployeeCust_EMallCompany = joined_LD_EmployeeCust.join(EMAllCompany, joined_LD_EmployeeCust.Employee_2 == EMAllCompany.Employee_3, "left")
joined_LD_EmployeeCust_EMallCompany = joined_LD_EmployeeCust_EMallCompany.drop('Employee_2','Employee_3')
# print_shapes(joined_LD_EmployeeCust_EMallCompany)

CFGTimeAnalysisHeadings = rename_column(CFGTimeAnalysisHeadings,'ReportColumn','Report_column_1')
joined_analysis_heading = CFGTimeAnalysis.join(CFGTimeAnalysisHeadings, CFGTimeAnalysis.ReportColumn == CFGTimeAnalysisHeadings.Report_column_1, "left")
joined_analysis_heading = drop_column(joined_analysis_heading,'Report_column_1')
joined_analysis_heading = drop_duplicated_rows(joined_analysis_heading)

joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany.join(joined_analysis_heading, joined_LD_EmployeeCust_EMallCompany.WBS1 == joined_analysis_heading.StartWBS1, "left")
joined_LD_EmployeeCust_EMallCompany_analysis = drop_column(joined_LD_EmployeeCust_EMallCompany_analysis,'StartWBS1')


joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn('TransDate', to_date(LD['TransDate'], 'yyyy-MM-dd'))
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.sort("TransDate", ascending=False)


latest_date = joined_LD_EmployeeCust_EMallCompany_analysis.select(max("TransDate")).first()[0]
one_year_ago = latest_date - timedelta(days=365)
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.filter((col("TransDate") >= one_year_ago) & (col("TransDate") <= latest_date))

joined_LD_EmployeeCust_EMallCompany_analysis = drop_duplicated_rows(joined_LD_EmployeeCust_EMallCompany_analysis)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import year, month
# from pyspark.sql.functions import weekofyear


# # Initialize Spark session
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn("Year", year(joined_LD_EmployeeCust_EMallCompany_analysis["TransDate"]))
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn("Month", month(joined_LD_EmployeeCust_EMallCompany_analysis["TransDate"]))
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn("Week", weekofyear(joined_LD_EmployeeCust_EMallCompany_analysis["TransDate"]))

# # Now 'df' has the 'Year', 'Month', and 'Week' columns added


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.filter(col("Employee_1") == "030569")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(joined_LD_EmployeeCust_EMallCompany_analysis)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Joinning Tables

# CELL ********************


# # total hours
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     "Total_Hours",
#     joined_LD_EmployeeCust_EMallCompany_analysis["RegHrs"] + 
#     joined_LD_EmployeeCust_EMallCompany_analysis["OvtHrs"] + 
#     joined_LD_EmployeeCust_EMallCompany_analysis["SpecialOvtHrs"]
# )


# # standard hours
# def calculate_standard_hours(row):
#     if row['CustHourlyorSalary'] == 'H':
#         return row['Total_Hours']
#     elif row['CustHourlyorSalary'] == 'S' and row['CustStraightTimeOT'] == "Y":
#         return row['Total_Hours']
#     elif row['CustHourlyorSalary'] == 'S' and row['CustStraightTimeOT'] == "N":
#         return 40
#     else:
#         return None

# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     'Standard_Hours',
#     F.udf(calculate_standard_hours)(F.struct('CustHourlyorSalary', 'CustStraightTimeOT', 'Total_Hours'))
# )

# #direct hours

# from pyspark.sql import functions as F

# # Define the function
# def calculate_direct_hours(row):
#     if row['ChargeType'] == 'R':
#         return row['Total_hours']
#     else:
#         return None

# # Use the UDF to create the new column
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     'Direct_Hours',
#     F.udf(calculate_direct_hours)(F.struct('ChargeType', 'Total_hours'))
# )


# #proposal hours
# from pyspark.sql import functions as F

# # Define the function
# def calculate_proposal_hours(row):
#     if row['ChargeType'] == 'P':
#         return row['Total_hours']
#     else:
#         return None

# # Use the UDF to create the new column
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     'Proposal_Hours',
#     F.udf(calculate_direct_hours)(F.struct('ChargeType', 'Total_hours'))
# )

# #overhead hours

# # Define the function
# def calculate_overhead_hours(row):
#     if row['ChargeType'] == 'H':
#         return row['Total_hours']
#     else:
#         return None

# # Use the UDF to create the new column
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     'Overhead_Hours',
#     F.udf(calculate_direct_hours)(F.struct('ChargeType', 'Total_hours'))
# )

# #PTO Paid time out
# def calculate_PTO(row):
#     if row['ChargeType'] == 'H'and row['Label'] == 'PTO':
#         return row['Total_hours']
#     else:
#         return None
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     'PTO',
#     F.udf(calculate_PTO)(F.struct('ChargeType','Label','Total_hours'))
# )

# #Vacation 
# def calculate_Vacation(row):
#     if row['ChargeType'] == 'H'and row['Label'] == 'Vacation':
#         return row['Total_hours']
#     else:
#         return None
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     'Vacation',
#     F.udf(calculate_Vacation)(F.struct('ChargeType','Label','Total_hours'))
# )

# #Sick
# def calculate_Sick(row):
#     if row['ChargeType'] == 'H'and row['Label'] == 'Sick Oth LV':
#         return row['Total_hours']
#     else:
#         return None
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     'Sick',
#     F.udf(calculate_Sick)(F.struct('ChargeType','Label','Total_hours'))
# )

# #Holiday
# def calculate_Holiday(row):
#     if row['ChargeType'] == 'H'and row['Label'] == 'Holiday':
#         return row['Total_hours']
#     else:
#         return None
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     'Holiday',
#     F.udf(calculate_Holiday)(F.struct('ChargeType','Label','Total_hours'))
# )

# #Floating Holiday
# def calculate_Float_Holiday(row):
#     if row['ChargeType'] == 'H'and row['Label'] == 'Float Holday':
#         return row['Total_hours']
#     else:
#         return None
# joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
#     'Floating Holiday',
#     F.udf(calculate_Float_Holiday)(F.struct('ChargeType','Label','Total_hours'))
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(joined_LD_EmployeeCust_EMallCompany_analysis)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# total hours
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    "Total_Hours",
    joined_LD_EmployeeCust_EMallCompany_analysis["RegHrs"] + 
    joined_LD_EmployeeCust_EMallCompany_analysis["OvtHrs"] + 
    joined_LD_EmployeeCust_EMallCompany_analysis["SpecialOvtHrs"]
)


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

joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    'Standard_Hours',
    F.udf(calculate_standard_hours)(F.struct('CustHourlyorSalary', 'CustStraightTimeOT', 'Total_Hours'))
)

#direct hours

from pyspark.sql import functions as F

# Define the function
def calculate_direct_hours(row):
    if row['ChargeType'] == 'R':
        return row['Total_hours']
    else:
        return 0

# Use the UDF to create the new column
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    'Direct_Hours',
    F.udf(calculate_direct_hours)(F.struct('ChargeType', 'Total_hours'))
)


#proposal hours
from pyspark.sql import functions as F

# Define the function
def calculate_proposal_hours(row):
    if row['ChargeType'] == 'P':
        return row['Total_hours']
    else:
        return 0

# Use the UDF to create the new column
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    'Proposal_Hours',
    F.udf(calculate_proposal_hours)(F.struct('ChargeType', 'Total_hours'))
)

#overhead hours

# Define the function
def calculate_overhead_hours(row):
    if row['ChargeType'] == 'H':
        return row['Total_hours']
    else:
        return 0

# Use the UDF to create the new column
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    'Overhead_Hours',
    F.udf(calculate_overhead_hours)(F.struct('ChargeType', 'Total_hours'))
)

#PTO Paid time out
def calculate_PTO(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'PTO':
        return row['Total_hours']
    else:
        return 0
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    'PTO',
    F.udf(calculate_PTO)(F.struct('ChargeType','Label','Total_hours'))
)

#Vacation 
def calculate_Vacation(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'Vacation':
        return row['Total_hours']
    else:
        return 0
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    'Vacation',
    F.udf(calculate_Vacation)(F.struct('ChargeType','Label','Total_hours'))
)

#Sick
def calculate_Sick(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'Sick Oth LV':
        return row['Total_hours']
    else:
        return 0
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    'Sick',
    F.udf(calculate_Sick)(F.struct('ChargeType','Label','Total_hours'))
)

#Holiday
def calculate_Holiday(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'Holiday':
        return row['Total_hours']
    else:
        return 0
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    'Holiday',
    F.udf(calculate_Holiday)(F.struct('ChargeType','Label','Total_hours'))
)

#Floating Holiday
def calculate_Float_Holiday(row):
    if row['ChargeType'] == 'H'and row['Label'] == 'Float Holday':
        return row['Total_hours']
    else:
        return 0
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
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
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn(
    "Total_Benifit_Hours",
    joined_LD_EmployeeCust_EMallCompany_analysis["PTO"] + 
    joined_LD_EmployeeCust_EMallCompany_analysis["Vacation"] + 
    joined_LD_EmployeeCust_EMallCompany_analysis["Sick"] +
    joined_LD_EmployeeCust_EMallCompany_analysis["Holiday"] +
    joined_LD_EmployeeCust_EMallCompany_analysis["Floating Holiday"] 
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(joined_LD_EmployeeCust_EMallCompany_analysis)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# #calculate UTE
# def calculate_ute(joined_LD_EmployeeCust_EMallCompany_analysis):
#     joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn("UTE", (col("Direct_Hours") / (col("Standard_Hours") - col("Total_Benifit_Hours"))) * 100)
#     return joined_LD_EmployeeCust_EMallCompany_analysis
# joined_LD_EmployeeCust_EMallCompany_analysis = calculate_ute(joined_LD_EmployeeCust_EMallCompany_analysis)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col, year, month, weekofyear
 
# Assuming 'df' is your PySpark DataFrame with columns 'TransDate', 'Employee', 'RegHrs', and 'OvtHrs'
joined_LD_EmployeeCust_EMallCompany_analysis = joined_LD_EmployeeCust_EMallCompany_analysis.withColumn("Year", year(col("TransDate"))) \
       .withColumn("Month", month(col("TransDate"))) \
       .withColumn("Week", weekofyear(col("TransDate")))
 
# Group by 'Employee_1', 'Year', 'Month', and 'Week' and aggregate
filtered_df = joined_LD_EmployeeCust_EMallCompany_analysis.groupBy("Employee_1", "Year", "Month", "Week") \
                .agg(
                    F.sum("Total_Hours").alias("Total_Hours"),
                    F.sum("Direct_Hours").alias("Direct_Hours"),
                    F.sum("Proposal_Hours").alias("Proposal_Hours"),
                    F.sum("Overhead_Hours").alias("Overhead_Hours"),
                    F.sum("Total_Benifit_Hours").alias("Total_Benifit_Hours")
                )
 
# Show the resulting DataFrame
filtered_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df = filtered_df.orderBy("Year", "Month", "Week")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F

def calculate_ute(df):
    df = df.withColumn("Standard_Hours", F.lit(40)) \
           .withColumn("Target_UTE", F.lit(75)) \
           .withColumn("UTE", (F.col("Direct_Hours") / (F.col("Standard_Hours") - F.col("Total_Benifit_Hours"))) * 100)
    return df
 
def calculate_Gap(df):
    df = df.withColumn("Gap", F.col("UTE") - F.col("Target_UTE"))
    return df
 
def calculate_Billability(df):
    df = df.withColumn("Billability", (F.col("Direct_Hours") / F.col("Standard_Hours")) * 100)
    return df
 
def calculate_Use(df):
    df = df.withColumn("Use", ((F.col("Direct_Hours") + F.col("Proposal_Hours")) / (F.col("Standard_Hours") - F.col("Total_Benifit_Hours"))) * 100)
    return df

filtered_df = calculate_ute(filtered_df)
filtered_df = calculate_Gap(filtered_df)
filtered_df = calculate_Billability(filtered_df)
filtered_df = calculate_Use(filtered_df)

filtered_df.show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(joined_LD_EmployeeCust_EMallCompany_analysis)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(filtered_df)

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
