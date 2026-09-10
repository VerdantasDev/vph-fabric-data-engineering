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

from pyspark.sql.functions import lit, col, trim, count, concat, StringType, when
import pyspark.sql.functions as F
from pyspark.sql import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load tables**

# CELL ********************

df_EMCompany = load_table(lakehouse_path, EMCompany)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df_EMCompany.withColumn("is_duplicate", F.count("Employee").over(Window.partitionBy("Employee")) > 1)

df_duplicates = df.filter(F.col("is_duplicate"))
df_non_duplicates = df.filter(~F.col("is_duplicate"))

window_spec = Window.partitionBy("Employee")

# Step 3: Add a column to mark if `EmployeeCompany` has "001" for any of the rows
df_with_has_001 = df_duplicates.withColumn(
    "has_001", F.max(F.when(F.col("EmployeeCompany") == "001", 1).otherwise(0)).over(window_spec)
)

# Step 4: Propagate the `has_001` flag as `True` for all rows if any row has "001"
df_with_has_001 = df_with_has_001.withColumn(
    "has_001", F.col("has_001") == 1
)

# Step 5: Separate DataFrames based on `has_001` column
df_has_001 = df_with_has_001.filter(F.col("has_001"))
df_no_001 = df_with_has_001.filter(~F.col("has_001"))
df_001_filtered = df_has_001.filter(F.col("EmployeeCompany") == "001")

df_final = df_non_duplicates.unionByName(df_001_filtered.drop('has_001'))
df_final = df_final.drop('is_duplicate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final2 = df_final.select("Employee","PayRate","PayRateMeth","PayRateTableNo")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_data(df_final2, lakehouse_path, s_EMCompany, overwriteSchema = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
