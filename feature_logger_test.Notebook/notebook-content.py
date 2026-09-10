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

from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table_path = "Tables/br_m365_sharepoint_download_file_status"

try:
    
    tablename = delta_table_path.split("Tables/")[1]
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    history_df = delta_table.history()
    history_df.createOrReplaceTempView("temp")

    query = f"""
        SELECT
            '{tablename}' as TableName,
            version,
            operation,
            readVersion,
            case 
                when operation = 'MERGE' then concat(
                    operationMetrics['numTargetRowsCopied'], 
                    " - Rows copied, ", 
                    operationMetrics['numTargetRowsCopied'], 
                    " - Rows Inserted, ", 
                    operationMetrics['numTargetRowsCopied'], 
                    " - Rows Updated, "
                )
                else concat(
                    operationMetrics['numOutputRows'],
                    " - Rows copied"
                )
            end as record_count,
            timestamp as load_timestamp
        FROM 
            temp
    """

    df = spark.sql(query)

except Exception as e:
    print(e)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load('Tables/sv_acquia_resumes')
df = df.select('content', 'filename')
df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp = df.limit(1)
display(temp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

text = temp.collect()[0]['content']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install transformers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
