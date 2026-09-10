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

!pip install us
import us
from pyspark.sql.functions import lit, col, trim, count, concat, StringType
import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load tables**

# CELL ********************

df_PR = load_table(lakehouse_path, PR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Keeping one row per project
wbs1_counts = df_PR.groupBy("WBS1").agg(count("WBS1").alias("count"))

duplicate_wbs1 = wbs1_counts.filter(col("count") > 1).select("WBS1")
unique_wbs1 = wbs1_counts.filter(col("count") == 1).select("WBS1")

df_PR_duplicates = df_PR.join(duplicate_wbs1, on="WBS1", how="inner")
df_PR_unique = df_PR.join(unique_wbs1, on="WBS1", how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_PR_filtered = df_PR_duplicates.filter(
    (trim(col("WBS2")).isNull() | (trim(col("WBS2")) == "")) &
    (trim(col("WBS3")).isNull() | (trim(col("WBS3")) == ""))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_combined = df_PR_unique.unionByName(df_PR_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_combined = apply_mapping(project_type_mapping, df_combined, 'ProjectType')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_combined.withColumn("ProjectAddressStreet", trim(concat(col("Address1"), lit(" "), col("Address2"), lit(" "), col("Address3"))))

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
df_joined = df_joined.withColumn("State", abbrev_to_full_name_udf(F.col("State")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_joined, lakehouse_path, s_PR, overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
