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

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.load('Tables/mvp_adp_data')
df2 = spark.read.load('Tables/mvp_aquia_data')
df3 = spark.read.load('Tables/mvp_practice_group_data')
df4 = spark.read.load('Tables/mvp_utilization_data')
df5 = spark.read.load('Tables/mvp_certlice_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_cols_list = df1.columns

df1 = df1.withColumn("EmployeeName1", regexp_replace(col("EmployeeName"), r"\.", ""))

df_adp = df1.withColumn('final', split(trim(lower(col('EmployeeName1'))), r"[ ,]+"))
df_adp = df_adp.select(*adp_cols_list, 'final')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

acquia_cols_list = df2.columns

df_acquia = df2.withColumn('template', trim(regexp_replace(translate(regexp_replace(col("filename"), r"\d+", ""), '_', ' '), r"\s+", " ")))\
                     .withColumn('test', trim(regexp_replace(col('template'), "Resume", "")))\
                     .withColumn('name', split(trim(lower(col("test"))), r"[ ,]+"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_adp.join(
                df_acquia,
                size(array_intersect(col("name"), col("final"))) >= size(col("name")),
                "left"
    )

df_acquia_adp = df_joined.select(*acquia_cols_list, *adp_cols_list)
merged_cols_list = df_acquia_adp.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mapping_dict = {
    "Digital Technologies": "Precise Visual Technologies / Applied Data & Technology",
    "Water & Wastewater Systems Engineering": "Process Engineering",
    "Sustainability Advisory": "Sustainability"
}

df_acquia_adp = df_acquia_adp.replace(mapping_dict, subset=['PracticeGroup'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_acquia_adp = df_acquia_adp.withColumn('PracticeGroup_1', lower(trim(regexp_replace(regexp_replace("PracticeGroup", "[,]", ""), "[&]", "and"))))
df_acquia_adp = df_acquia_adp.withColumn('PracticeGroup_1', regexp_replace(col('PracticeGroup_1'), r'~~', ''))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

practice_grp_cols_list = df3.columns
df_practice_grp = df3.withColumn('Practice1', lower(trim(regexp_replace(regexp_replace("Practice", "[,]", ""), "[&]", "and"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_acquia_adp.join(
    df_practice_grp, 
    on = [df_acquia_adp.PracticeGroup_1 == df_practice_grp.Practice1], 
    how = 'left'
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_final.select(*merged_cols_list, *practice_grp_cols_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_deltk_joined = df_final.join(
            df4,
            on = [df_final.EmployeeNumber == df4.EmployeeIDADP],
            how = 'left'
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_deltk_joined.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df_deltk_joined.join(
    df5,
    on = [df_deltk_joined.EmployeeNumber == df5.EmployeeID],
    how = 'left'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# l = {
# 'file_url':	"ResumeUrl",
# 'tags':	"EmployeeTaxonomySkills",
# 'extractedskills':	"EmployeeSkills",
# 'EmployeeName':	"EmployeeName",
# 'EmployeeNumber':	"EmployeeId",
# 'EmployeeBillType':	"EmployeeType",
# 'PracticeGroup':	"EmployeePracticeGroup",
# 'Title':	"EmployeeRole",
# 'BusinessUnit':	"EmployeeBusinessUnit",
# 'Department':	"EmployeeDepartment",
# 'EmploymentStatus':	"EmployeeStatus",
# 'OfficeLocation':	"EmployeeWorkLocation",
# 'skills_list':	"EmployeePracticeGroupSkills",
# 'EmployeeBillingCategory':	"EmployeeBillingLaborCategory",
# 'BusinessUnitDescription':	"EmployeeBusinessUnitDescription",
# 'LicenseCertificationDescription':	"Licensure",
# 'LicenseCertificationState':	"LicenseState",
# 'LicenseCertificationID':	"LicenseCode"
# }

# for i, j in l.items():
#     s = f"df.withColumnRenamed('{i}', '{j}')"
#     print(s)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumnRenamed('file_url', 'ResumeUrl')
df = df.withColumnRenamed('tags', 'EmployeeTaxonomySkills')
df = df.withColumnRenamed('extractedskills', 'EmployeeSkills')
df = df.withColumnRenamed('EmployeeName', 'EmployeeName')
df = df.withColumnRenamed('EmployeeNumber', 'EmployeeId1')
df = df.withColumnRenamed('EmployeeBillType', 'EmployeeType')
df = df.withColumnRenamed('PracticeGroup', 'EmployeePracticeGroup')
df = df.withColumnRenamed('Title', 'EmployeeRole')
df = df.withColumnRenamed('BusinessUnit', 'EmployeeBusinessUnit')
df = df.withColumnRenamed('Department', 'EmployeeDepartment')
df = df.withColumnRenamed('EmploymentStatus', 'EmployeeStatus')
df = df.withColumnRenamed('OfficeLocation', 'EmployeeWorkLocation')
df = df.withColumnRenamed('skills_list', 'EmployeePracticeGroupSkills')
df = df.withColumnRenamed('EmployeeBillingCategory', 'EmployeeBillingLaborCategory')
df = df.withColumnRenamed('BusinessUnitDescription', 'EmployeeBusinessUnitDescription')
df = df.withColumnRenamed('LicenseCertificationDescription', 'Licensure')
df = df.withColumnRenamed('LicenseCertificationState', 'LicenseState')
df = df.withColumnRenamed('LicenseCertificationID', 'LicenseCode')
df = df.withColumnRenamed('UtilizationStatus', 'EmployeeUtilizationStatus')
df = df.withColumnRenamed('EmployeeUtilization', 'EmployeeUtilization')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final = df.select('ResumeUrl', 'EmployeeTaxonomySkills', 'EmployeeSkills', 'EmployeeName', 'EmployeeId1', 'EmployeeType', 'EmployeePracticeGroup', 'EmployeeRole', 'EmployeeBusinessUnit', 'EmployeeDepartment', 'EmployeeStatus', 'EmployeeWorkLocation', 'EmployeePracticeGroupSkills', 'EmployeeBillingLaborCategory', 'EmployeeBusinessUnitDescription', 'Licensure', 'LicenseState', 'LicenseCode', 'EmployeeUtilizationStatus', 'EmployeeUtilization')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final = final.withColumnRenamed('EmployeeId1', 'EmployeeId')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final.write.format('delta').mode('overwrite').save('Tables/mvp_resource_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load('Tables/mvp_resource_data')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.select('ResumeUrl',
 'EmployeeTaxonomySkills',
 'EmployeeSkills',
 'EmployeeName',
 'EmployeeId',
 'EmployeePracticeGroupSkills').drop_duplicates().dropna(subset=["ResumeUrl"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

initial_df = df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the schema for the JSON structure
schema = StructType([
    StructField("Environmental", ArrayType(StringType()), True),
    StructField("Engineering", ArrayType(StringType()), True),
    StructField("Project Management", ArrayType(StringType()), True),
    StructField("Location", ArrayType(StringType()), True),
    StructField("ClientFolderID", ArrayType(StringType()), True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parse the JSON column into structured data
parsed_df = df.select(col("EmployeeId"), from_json(col("EmployeeTaxonomySkills"), schema).alias("data"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode the arrays to flatten the DataFrame
exploded_environmental = parsed_df.select(
    col("EmployeeId"),
    col("data.Environmental").alias("Environmental")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exploded_engineering = parsed_df.select(
    col("EmployeeId"),
    col("data.Engineering").alias("Engineering")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exploded_project_management = parsed_df.select(
    col("EmployeeId"),
    col("data.Project Management").alias("ProjectManagement")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_environmental = df.join(exploded_environmental, on="EmployeeId", how="left")
joined_engineering = joined_environmental.join(exploded_engineering, on="EmployeeId", how="left")
joined_project_management = joined_engineering.join(exploded_project_management, on="EmployeeId", how="left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, expr

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = joined_project_management.withColumn("EmployeeSkills", expr("transform(EmployeeSkills, x -> lower(x))")) \
       .withColumn("Environmental", expr("transform(Environmental, x -> lower(x))")) \
       .withColumn("Engineering", expr("transform(Engineering, x -> lower(x))")) \
       .withColumn("ProjectManagement", expr("transform(ProjectManagement, x -> lower(x))"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, size, array_intersect, array_except

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find common values
df = df.withColumn("common_values_ProjectManagement", array_intersect(col("EmployeeSkills"), col("ProjectManagement")))

# Find unique values in col1
df = df.withColumn("unique_to_EmployeeSkills_ProjectManagement", array_except(col("EmployeeSkills"), col("ProjectManagement")))

# Count the number of common values
df = df.withColumn("common_count_ProjectManagement", size(col("common_values_ProjectManagement")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find common values
df = df.withColumn("common_values_Engineering", array_intersect(col("EmployeeSkills"), col("Engineering")))

# Find unique values in col1
df = df.withColumn("unique_to_EmployeeSkills_Engineering", array_except(col("EmployeeSkills"), col("Engineering")))

# Count the number of common values
df = df.withColumn("common_count_Engineering", size(col("common_values_Engineering")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find common values
df = df.withColumn("common_values_Environmental", array_intersect(col("EmployeeSkills"), col("Environmental")))

# Find unique values in col1
df = df.withColumn("unique_to_EmployeeSkills_Environmental", array_except(col("EmployeeSkills"), col("Environmental")))

# Count the number of common values
df = df.withColumn("common_count_Environmental", size(col("common_values_Environmental")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.dropDuplicates(subset = ['EmployeeId'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = initial_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df1.withColumn("EmployeeSkills", expr("transform(EmployeeSkills, x -> lower(x))")) \
       .withColumn("EmployeePracticeGroupSkills", expr("transform(EmployeePracticeGroupSkills, x -> lower(x))"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find common values
df = df.withColumn("common_values_EmployeePracticeGroupSkills", array_intersect(col("EmployeeSkills"), col("EmployeePracticeGroupSkills")))

# Find unique values in col1
df = df.withColumn("unique_to_EmployeeSkills_EmployeePracticeGroupSkills", array_except(col("EmployeeSkills"), col("EmployeePracticeGroupSkills")))

# Count the number of common values
df = df.withColumn("common_count_EmployeePracticeGroupSkills", size(col("common_values_EmployeePracticeGroupSkills")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

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
