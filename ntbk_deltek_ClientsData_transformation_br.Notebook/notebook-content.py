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

# MARKDOWN ********************

# ## **Loading the tables**

# CELL ********************

df_Clendor = load_table(lakehouse_path, s_Clendor)
df_CLAddress = load_table(lakehouse_path, s_CLAddress).withColumnRenamed("ClientID","ClientID_CLAddress")
# df_ClendorProjectAssoc = load_table(lakehouse_path, ClendorProjectAssoc)
df_CFGClientTypeDescriptions = load_table(lakehouse_path, s_CFGClientTypeDescriptions)
df_Clients_ClientFolderList = load_table(lakehouse_path, s_Clients_ClientFolderList)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Joins**

# CELL ********************

# display(df_Clients_ClientFolderList.filter(df_Clients_ClientFolderList['CustClientID'] == 'Vision_ALDEN_250'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Clendor.select("ClientID").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined1 = df_Clendor.join(df_CFGClientTypeDescriptions, df_Clendor['Type'] == df_CFGClientTypeDescriptions['Code'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined2 = df_joined1.join(df_Clients_ClientFolderList, df_joined1['ClientID'] == df_Clients_ClientFolderList['CustClientID'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined3 = df_joined2.join(df_CLAddress, df_joined2['ClientID'] == df_CLAddress['ClientID_CLAddress'], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined3.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Ingestion**

# CELL ********************

# summary_df = analyze_dataframe(df_joined3)
# display(summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_joined3, lakehouse_path, br_Clients, overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
