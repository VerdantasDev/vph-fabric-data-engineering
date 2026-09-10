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

from itertools import combinations
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = load_table(lakehouse_path, 'br_deltek_EMCompany_v3')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to check if a combination of columns is a primary key
def is_composite_primary_key(df, columns):
    return df.select(columns).distinct().count() == df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get unique values with condition
def get_unique_values(df, column):
    unique_values = df.select(column).distinct().collect()
    if len(unique_values) < 4:
        return [row[column] for row in unique_values]
    else:
        return "too many values"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get null and empty values count
def get_null_and_empty_values(df, column):
    return df.filter((df[column].isNull()) | (df[column] == '')).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get junk values count (only special characters)
def get_junk_values(df, column):
    return df.filter(df[column].rlike('^[^a-zA-Z0-9]*$') & (df[column] != '')).count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get duplicate rows count
def get_duplicate_rows(df):
    return df.count() - df.dropDuplicates().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize summary list
summary = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Iterate over all columns
for column in df.columns:
    summary.append({
        "Column": column,
        "Is Primary Key": is_composite_primary_key(df, [column]),
        "Total Count": df.count(),
        "Distinct Count": df.select(column).distinct().count(),
        "Unique Values": get_unique_values(df, column),
        "Null Values": get_null_and_empty_values(df, column),
        "Junk Values": get_junk_values(df, column)
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add duplicate rows count to summary
summary.append({
    "Column": "All Columns",
    "Is Primary Key": None,
    "Total Count": df.count(),
    "Distinct Count": df.distinct().count(),
    "Unique Values": None,
    "Null Values": None,
    "Junk Values": None,
    "Duplicate Rows": get_duplicate_rows(df)
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_df = pd.DataFrame(summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
