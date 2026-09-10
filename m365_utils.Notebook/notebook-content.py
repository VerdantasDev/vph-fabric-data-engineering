# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import re
import requests

def get_access_token(tenant_id, client_id, client_secret_value):

    scope = 'https://graph.microsoft.com/.default'
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret_value,
        'scope': scope
    }

    response = requests.post(url, data=data)
    response.raise_for_status()

    if response.status_code == 200:
        access_token = response.json().get('access_token')
    else:
        access_token = None
    
    return access_token
        

def get_headers(access_token):
    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    return headers

def convert_pandasdf_to_sparkdf(pandas_df):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("Delve") \
        .getOrCreate()

    spark_df = spark.createDataFrame(pandas_df)

    return spark_df

def write_data(df, delta_table_path, append = False, overwriteSchema = False):
    if append:
        df.write.format("delta").mode("append").save(delta_table_path)
    elif overwriteSchema:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)
    else:
        df.write.format("delta").mode("overwrite").save(delta_table_path)


def get_secrets_from_KV(key_vault_uri, key):
    return mssparkutils.credentials.getSecret(key_vault_uri, key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
