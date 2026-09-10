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

df_ProjectCustomTabFields = load_table(lakehouse_path, ProjectCustomTabFields)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ProjectCustomTabFields.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ProjectCustomTabFields.select("WBS1").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Keeping one row per project
wbs1_counts = df_ProjectCustomTabFields.groupBy("WBS1").agg(count("WBS1").alias("count"))

duplicate_wbs1 = wbs1_counts.filter(col("count") > 1).select("WBS1")
unique_wbs1 = wbs1_counts.filter(col("count") == 1).select("WBS1")

df_ProjectCustomTabFields_duplicates = df_ProjectCustomTabFields.join(duplicate_wbs1, on="WBS1", how="inner")
df_ProjectCustomTabFields_unique = df_ProjectCustomTabFields.join(unique_wbs1, on="WBS1", how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ProjectCustomTabFields_filtered = df_ProjectCustomTabFields_duplicates.filter(
    (trim(col("WBS2")).isNull() | (trim(col("WBS2")) == "")) &
    (trim(col("WBS3")).isNull() | (trim(col("WBS3")) == ""))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_combined = df_ProjectCustomTabFields_unique.unionByName(df_ProjectCustomTabFields_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_combined)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_combined, lakehouse_path, s_ProjectCustomTabFields, overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
