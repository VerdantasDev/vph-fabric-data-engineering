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

from pyspark.sql.functions import col, trim, regexp_replace, translate, split, lower, array_intersect, size

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

acquia_content_table = 'Tables/br_acquia_content_ADI_v2'
adp_workers_data_table = 'Tables/br_adp_workers_data'
target_table_path = 'Tables/sv_acquia_resumes'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:

    merge_keys = ['id', 'filename', 'associateOID']

    df1 = spark.read.load(acquia_content_table)
    df1 = df1.filter(df1.collection_name == 'Verdantas Resumes')

    cols = df1.columns

    df1 = df1.withColumn('template', trim(regexp_replace(translate(regexp_replace(col("filename"), r"\d+", ""), '_', ' '), r"\s+", " ")))\
            .withColumn('test', trim(regexp_replace(col('template'), "Resume", "")))\
            .withColumn('name', split(trim(lower(col("test"))), r"[ ,]+"))

    df_1 = spark.read.load(adp_workers_data_table)

    df_1 = df_1.withColumn('temp', split(trim(lower(df_1.personName)), r"[ ,]+"))
    df_1 = df_1.select('associateOID', 'temp')

    df_joined = df1.join(
        df_1,
        size(array_intersect(col("name"), col("temp"))) >= size(col("name")),
        "inner"
    )

    df_final = df_joined.select('associateOID', *cols)

    total_resumes_from_acquia = df1.select(df1.filename).distinct().count()
    mapped_resumes_count = df_final.select(df1.filename).distinct().count()

except Exception as e:
    raise Exception(e)


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

display(temp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp = df_final.dropDuplicates(merge_keys)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
