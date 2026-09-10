# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "37fdbe59-505a-4223-83b3-8386d17898e4",
# META       "default_lakehouse_name": "demo_lakehouse",
# META       "default_lakehouse_workspace_id": "a4338e88-8d35-40c8-9929-a5b820d084e3",
# META       "known_lakehouses": [
# META         {
# META           "id": "37fdbe59-505a-4223-83b3-8386d17898e4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_new.write.mode("overwrite").saveAsTable("employee")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_new = df.withColumn(
    "salary",
    col("salary") * 1.10
)

df_new.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
data = [
    (1, "John", 50000),
    (2, "Mike", 60000),
    (3, "David", 70000)
]

df = spark.createDataFrame(
    data,
    ["emp_id", "name", "salary"]
)

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
