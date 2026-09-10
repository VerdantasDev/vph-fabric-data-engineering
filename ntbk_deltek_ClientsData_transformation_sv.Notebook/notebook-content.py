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

from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# test_df = load_table(lakehouse_path, PR)
# display(test_df.filter(test_df['ClientID'] == 'LG2023_04892').select("WBS1").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Loading Data**

# CELL ********************

df_Clients = load_table(lakehouse_path, br_Clients)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df_Clients.filter(df_Clients['ClientID'] == 'maA1101292').select("ClientID","Name","ClientAddressLocation","City","State").drop_duplicates())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import functions as F
# df_req = df_Clients.select("ClientID","Name","ClientAddressLocation","City","State").drop_duplicates()
# df_grouped = df_req.groupBy("ClientID", "Name") \
#     .agg(F.count("*").alias("RowCount")) \
#     .orderBy(F.col("RowCount").desc())

# display(df_grouped)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Clients_worked = df_Clients.withColumn("Worked", lit("YES"))
# df_Clients_req = df_Clients_req.withColumn("ClientAddressCity", lit("NA"))
# df_Clients_req = df_Clients_req.withColumn("ClientAddressState", lit("NA"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Clients_req = df_Clients_worked[['ClientID',
 'CustClientFolderID',
 'Name',
 'Status',
 'Description',
 'Worked'
]].drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rename_dict = {
    "ClientID": "ClientID",
    "Name": "ClientName",
    "Status": "ClientStatus",
    "Description": "ClientType",
    "CustClientFolderID": "ClientFolderID"
}

for old_name, new_name in rename_dict.items():
    df_Clients_req = df_Clients_req.withColumnRenamed(old_name, new_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_Clients_req)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_Clients_req, lakehouse_path, sv_Clients, overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
