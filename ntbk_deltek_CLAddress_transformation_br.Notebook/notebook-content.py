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

import pyspark.sql.functions as F
from pyspark.sql.functions import lit, col, trim, count, concat
from pyspark.sql.types import StringType
!pip install us
import us

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_CLAddress = load_table(lakehouse_path, CLAddress)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Map State codes to their full names
state_mapping = {state.abbr: state.name for state in us.states.STATES}

# Broadcast the dictionary to all worker nodes
state_mapping_broadcast = spark.sparkContext.broadcast(state_mapping)

# Define a UDF that uses the broadcasted dictionary
def abbrev_to_full_name(abbrev):
    return state_mapping_broadcast.value.get(abbrev)

# Register the UDF in Spark
abbrev_to_full_name_udf = F.udf(abbrev_to_full_name, StringType())

# Apply the UDF to the 'State_Abbreviation' column
df_CLAddress_trans = df_CLAddress.withColumn("State", abbrev_to_full_name_udf(F.col("State")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_CLAddress_trans = df_CLAddress_trans.withColumn("ClientAddressLocation", 
                   trim(concat(col("Address1"), lit(" "), col("Address2"), lit(" "), col("Address3"), lit(" "), col("Address4"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_CLAddress_trans.groupBy("ClientID").count().orderBy("count", ascending=False).withColumnRenamed('count','number of address for each client'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df_CLAddress_trans.filter(df_CLAddress_trans['ClientID'] == 'LG2023_06575'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_CLAddress_trans = df_CLAddress_trans[['ClientID',
 'ClientAddressLocation',
 'City',
 'State',
 'ZIP',
 'Country',
 'Phone',
 'EMail',
 'PrimaryInd',
 'CLAddressID',
 'Rowversion',
 'CreateUser',
 'CreateDate',
 'ModUser',
 'ModDate']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_CLAddress_trans, lakehouse_path, s_CLAddress, overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
