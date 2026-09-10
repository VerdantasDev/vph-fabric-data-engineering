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

df_acquia = spark.read.load('Tables/br_acquia_content_ADI_v2')
df_acquia = df_acquia.filter(df_acquia.collection_name == 'Verdantas Resumes')
acquia_cols_list = df_acquia.columns

df_acquia = df_acquia.withColumn('template', trim(regexp_replace(translate(regexp_replace(col("filename"), r"\d+", ""), '_', ' '), r"\s+", " ")))\
                     .withColumn('test', trim(regexp_replace(col('template'), "Resume", "")))\
                     .withColumn('name', split(trim(lower(col("test"))), r"[ ,]+"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_adp_deltek = spark.read.load('Tables/br_employee_deltek_adp_merged')
adp_deltek_cols_list = df_adp_deltek.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_adp_deltek = df_adp_deltek.withColumn("FirstName1", regexp_replace(col("FirstName"), r"\.", ""))\
                             .withColumn("MiddleName1", regexp_replace(col("MiddleName"), r"\.", ""))\
                             .withColumn("LastName1", regexp_replace(col("LastName"), r"\.", ""))\
                             .withColumn("EmployeeName1", regexp_replace(col("EmployeeName"), r"\.", ""))

df_adp_deltek = df_adp_deltek.withColumn('temp', split(trim(lower(col('EmployeeName1'))), r"[ ,]+"))
df_adp_deltek = df_adp_deltek.withColumn('temp1', split(trim(lower(concat_ws(', ', col('FirstName1'), col('MiddleName1'), col('LastName1')))), r"[ ,]+"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp = df_adp_deltek.withColumn('final', when(col('temp').isNull(), col('temp1')).otherwise(col('temp')))
df_adp_deltek_final = temp.select(*adp_deltek_cols_list, 'final')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_acquia.join(
                df_adp_deltek_final,
                size(array_intersect(col("name"), col("final"))) >= size(col("name")),
                "inner"
    )

df_acquia_adp_deltek = df_joined.select(*acquia_cols_list, *adp_deltek_cols_list)
merged_cols_list = df_acquia_adp_deltek.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_acquia_adp_deltek = df_acquia_adp_deltek.withColumn('PracticeGroup_1', lower(trim(regexp_replace(regexp_replace("DeltekPracticeGroup", "[,]", ""), "[&]", "and"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_practice_grp = spark.read.load('Tables/br_practicegroup_manual')
practice_grp_cols_list = df_practice_grp.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_practice_grp = df_practice_grp.withColumn('Practice1', lower(trim(regexp_replace(regexp_replace("Practice", "[,]", ""), "[&]", "and"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_acquia_adp_deltek.join(df_practice_grp, on = [df_acquia_adp_deltek.PracticeGroup_1 == df_practice_grp.Practice1], how = 'left')

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

display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
