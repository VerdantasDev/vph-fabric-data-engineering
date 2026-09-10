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

from datetime import datetime

from delta.tables import *
from pyspark.sql.functions import lit, col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_sink(temp, delta_table_path, merge_keys):

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        
        tgt_table = DeltaTable.forPath(spark, delta_table_path)

        merge_condition = " and ".join(
            [f" target.{col} = updates.{col} " for col in merge_keys]
        )

        tgt_table.alias('target').merge(
            source=temp.alias("updates"), condition=merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    else:
        temp.write.format("delta").mode("append").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

today = datetime.today().date()
source_tbl_path = "Tables/br_nds_drug_testing"

delta_table_path = "Tables/sv_nds_drug_testing"
merge_keys = ['state', 'last_updated_on']


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load(source_tbl_path)

df = df.withColumn('created_on', lit(today))\
       .withColumnRenamed('update_date', 'last_updated_on')\
       .select('state', 'url', 'last_updated_on', 'drug_testing_protocols', col('created_on'))

write_to_sink(df, delta_table_path, merge_keys)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
