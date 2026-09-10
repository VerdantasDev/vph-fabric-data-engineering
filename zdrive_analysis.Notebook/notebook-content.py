# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.getOrCreate()

# Define the path to the file in the lakehouse ABFS path
file_path = "./builtin/MVP_ProjectFiles_final.xlsx"

# Read the Excel file into a DataFrame
df = spark.read.format("com.crealytics.spark.excel") \
    .option("useHeader", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
