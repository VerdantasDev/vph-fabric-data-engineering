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

# Import libraries
import os
import requests
import pandas as pd
from datetime import datetime
from delta.tables import DeltaTable
from tqdm import tqdm

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("External") \
    .getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_df = spark.read.format("delta").load("Tables/br_external_delware_gov").filter("download_status = 0")

file_url_df = source_df.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

today = datetime.today()
today_str = today.strftime("%Y-%mm-%d")
year_str = today.year
month_str = today.strftime("%m")
day_str = today.strftime("%d") 
os.makedirs(f'/lakehouse/default/Files/bronze/external_source/delaware_gov_bid/{year_str}/{month_str}/{day_str}',exist_ok=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_urls = file_url_df.to_dict(orient='records')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fu = file_urls[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fu

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for fu in tqdm(file_urls, desc="Processing files"):
    timestamp = datetime.now()
    timestamp = int(timestamp.timestamp())
    file_name = fu['document_url'].split('/')[-1]
    file_suffix = '.'+file_name.split('.')[-1]
    save_file_name = file_name.replace(file_suffix,'').replace('.','').replace('|','') + '||' + str(timestamp) + file_suffix

    response = requests.get(fu['document_url'])
    if response.status_code == 200:
        data = response.content
        full_path = f'/lakehouse/default/Files/bronze/external_source/delaware_gov_bid/{year_str}/{month_str}/{day_str}/{save_file_name}'

        with open(full_path, 'wb') as file:
            file.write(response.content)

        file_url_df.loc[(file_url_df['contractNumber'] == fu['contractNumber']) & (file_url_df['document_name'] == fu['document_name']), 'download_status']  = 1
        file_url_df.loc[(file_url_df['contractNumber'] == fu['contractNumber']) & (file_url_df['document_name'] == fu['document_name']),'bronze_rfp_document_save_path'] = full_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(file_url_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df = spark.createDataFrame(file_url_df)
delta_table = DeltaTable.forPath(spark, 'br_external_delware_gov')
merge_condition =   "delta_table.contractNumber = spark_df.contractNumber AND delta_table.document_name = spark_df.document_name"
delta_table.alias("delta_table").merge(spark_df.alias("spark_df"),
        merge_condition
    ).whenMatchedUpdateAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import pandas as pd
# # Load data into pandas DataFrame from f"{notebookutils.nbResPath}/builtin/delware_gov.csv"
# df = pd.read_csv(f"{notebookutils.nbResPath}/builtin/delware_gov.csv")
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df['download_status'] = 0
# df['bronze_rfp_document_save_path'] = ''
# spark_df = spark.createDataFrame(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Write DataFrame to Delta Lake table
# delta_table_path = "Tables/br_external_delware_gov"  # Example Delta Lake table path in Azure Data Lake Storage
 
# # Write data to Delta Lake table
# spark_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(delta_table_path)

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
