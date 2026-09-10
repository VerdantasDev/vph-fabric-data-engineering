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

# %run utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %run ntbk_audit_frmwrk

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

InputParam = """['LD', ['Employee]]"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# logger = LoggerSetup('ntbk_deltek_bronze_ingestion')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder \
    .appName("DelekeIngestion") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# resource_mapper = resource_name_mapper()
# kv_uri = f'https://{resource_mapper["key_vault"]}.vault.azure.net/'

# serverName = mssparkutils.credentials.getSecret(kv_uri, )
# database = mssparkutils.credentials.getSecret(kv_uri, )
# dbPort = 1433
# dbUserName = mssparkutils.credentials.getSecret(kv_uri, )
# dbPassword = mssparkutils.credentials.getSecret(kv_uri, )
# schema = mssparkutils.credentials.getSecret(kv_uri, )

serverName = "10.100.10.21"
database = "VPC-Vantagepoint"
dbPort = 1433
dbUserName = "vpc"
dbPassword = "Vsolutions2025!"
schema = ""

jdbcUrl = f"jdbc:sqlserver://{serverName}:{dbPort};database={database};user={dbUserName};password={dbPassword}"
connection = {"user":dbUserName,"password":dbPassword,"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.jdbc(url=jdbcUrl, table = 'table_name', properties=connection)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    # logger.info("Ingesting deltek data")

    # input = json.loads(InputParam)
    # source_table_name = input[0]
    # merge_keys = input[1]

    source_table_name = ''
    
    table_name = f"{schema}.{source_table_name}"
    # logger.info(f"Reading table {table_name}")

    df = spark.read.jdbc(url=jdbcURL, table = table_name, properties=connection)
    # logger.info(f"Table {table_name} read into dataframe")

    target_table_path = f'Tables/{source_table_name}'

    df.write.format('delta').mode('overwrite').save(target_table_path)

    # logger.info(f"Writing data to Lakehouse using merge keys {merge_keys}")
    write_to_target(df, target_table_path, merge_keys)
    # logger.info(f"Table {target_table_path} written to lakehouse successfully")

    # save_audit_history(target_table_path)
    # logger.info("Saving Delta Table Audit History.")
    # logger.info("Process completed successfully.")
    # logger.save_logs()

except Exception as e:
    # logger.error(f"An error occurred during processing: {e}")
    # logger.save_logs()
    raise Exception(e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
