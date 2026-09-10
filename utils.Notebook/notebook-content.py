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

from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_onelake_path():
    """
        Generates the Onelakepath
        Returns:
            str: Generated onelake path
    """
    base_path = notebookutils.fs.ls('./')[0].path
    container = base_path.split('@')[0].split('//')[1]
    file_system = base_path.split('@')[1].split('/')[1]

    onelakepath = f"abfss://{container}@onelake.dfs.fabric.microsoft.com/{file_system}"

    return onelakepath


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def resource_name_mapper():
    """
        resource name mapper
        Returns:
            Dict: with resourcetype as key and name as value
    """
    
    resource_mapper = {
        "key_vault":"vpc-dev-keyvault",
        "document_intelligence":"vpc-dev-documentintelligence"
    }
    
    return resource_mapper


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_target(temp, delta_table_path, merge_keys, partition_keys = []):
    """
        writes the transformed data to delta tables, if table exists does an upsert else creates a new table
        Args:
            temp (df): Spark dataframe object with transformed data
            delta_table_path (str): Path to the table `Tables\<tablename>`
            merge_keys (list): Keys to use to upsert the data
    """

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        
        tgt_table = DeltaTable.forPath(spark, delta_table_path)

        merge_condition = " and ".join(
            [f" target.{col} = updates.{col} " for col in merge_keys]
        )

        tgt_table.alias('target').merge(
            source=temp.alias("updates"), condition=merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    else:
        if len(partition_keys) == 0:
            temp.write.format("delta").mode("append").save(delta_table_path)
        else:
            temp.write.partitionBy(*partition_keys).format("delta").mode("append").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
