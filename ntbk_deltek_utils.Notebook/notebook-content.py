# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

def load_table(lakehouse_path, table_name):

    from pyspark.sql import SparkSession

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("LoadTables") \
        .getOrCreate()

    # Define paths to your Lakehouse tables
    table_paths = {
        "table_path": f"{lakehouse_path}/{table_name}",
    }

    df = spark.read.format("delta").load(table_paths["table_path"])

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_duplicates(df, columns_list):

    duplicates = df.groupBy(columns_list).count().filter("count > 1")
    df_duplicates = df.join(duplicates, on=columns_list, how='inner').drop("count")

    return df_duplicates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_data(df, lakehouse_path, table_name, append = False, overwriteSchema = False):
    if append:
        df.write.format("delta").mode("append").save(f"{lakehouse_path}/{table_name}")
    elif overwriteSchema:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{lakehouse_path}/{table_name}")
    else:
        df.write.format("delta").mode("overwrite").save(f"{lakehouse_path}/{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def analyze_dataframe(df):
    from itertools import combinations
    import pandas as pd
    # Function to check if a combination of columns is a primary key
    def is_composite_primary_key(df, columns):
        return df.select(columns).distinct().count() == df.count()

    # Function to get unique values with condition
    def get_unique_values(df, column):
        unique_values = df.select(column).distinct().collect()
        if len(unique_values) < 4:
            return [row[column] for row in unique_values]
        else:
            return "too many values"

    # Function to get null and empty values count
    def get_null_and_empty_values(df, column):
        return df.filter((df[column].isNull()) | (df[column] == '')).count()

    # Function to get junk values count (only special characters)
    def get_junk_values(df, column):
        return df.filter(df[column].rlike('^[^a-zA-Z0-9]*$') & (df[column] != '')).count()

    # Function to get duplicate rows count
    def get_duplicate_rows(df):
        return df.count() - df.dropDuplicates().count()

    # Initialize summary list
    summary = []

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

    summary_df = pd.DataFrame(summary)
    return summary_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def apply_mapping(mapping_dict, df, column):

    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import StringType

    # Normalize the keys in the mapping dictionary
    normalized_mapping_dict = {k.strip().lower(): v for k, v in mapping_dict.items()}
    
    # Define a UDF to apply the mapping
    def map_value(value):
        if value is None:
            return None
        return normalized_mapping_dict.get(value.strip().lower(), value)

    
    map_value_udf = udf(map_value, StringType())
    
    # Apply the UDF to the specified column
    df = df.withColumn(column, map_value_udf(col(column)))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def group_by_agg(df, group_by_columns, agg_spec):
    """
    Group a DataFrame by specified columns and apply multiple aggregations.
    
    Parameters:
    df : DataFrame - the input DataFrame
    group_by_columns : list - list of columns to group by
    agg_spec : dict - a dictionary specifying aggregation functions for each column.
                      Supported aggregation types: 'sum', 'avg', 'concat', 'first', 'last',
                      'min', 'max', 'count', 'countDistinct', 'stddev', 'variance', 'collect_list'
    
    Returns:
    DataFrame - the grouped and aggregated DataFrame
    """
    from pyspark.sql import functions as F
    
    # Define the aggregation expressions
    agg_exprs = []
    for col, agg_type in agg_spec.items():
        if agg_type == 'sum':
            agg_exprs.append(F.sum(col).alias(f'{col}_sum'))
        elif agg_type == 'avg':
            agg_exprs.append(F.avg(col).alias(f'{col}_avg'))
        elif agg_type == 'concat':
            agg_exprs.append(F.concat_ws(';', F.collect_set(F.when(~F.isnan(F.col(col)) & F.col(col).isNotNull(), F.col(col)))).alias(f'{col}_concat'))
        elif agg_type == 'first':
            agg_exprs.append(F.first(col, ignorenulls=True).alias(f'{col}_first'))
        elif agg_type == 'last':
            agg_exprs.append(F.last(col, ignorenulls=True).alias(f'{col}_last'))
        elif agg_type == 'min':
            agg_exprs.append(F.min(col).alias(f'{col}_min'))
        elif agg_type == 'max':
            agg_exprs.append(F.max(col).alias(f'{col}_max'))
        elif agg_type == 'count':
            agg_exprs.append(F.count(col).alias(f'{col}_count'))
        elif agg_type == 'countDistinct':
            agg_exprs.append(F.countDistinct(col).alias(f'{col}_countDistinct'))
        elif agg_type == 'stddev':
            agg_exprs.append(F.stddev(col).alias(f'{col}_stddev'))
        elif agg_type == 'variance':
            agg_exprs.append(F.variance(col).alias(f'{col}_variance'))
        elif agg_type == 'collect_list':
            agg_exprs.append(F.collect_list(F.col(col)).alias(f'{col}_list'))
        else:
            raise ValueError(f"Unsupported aggregation type: {agg_type}")
    
    # Perform the group by and aggregation
    df_grouped = df.groupBy(group_by_columns).agg(*agg_exprs)
    
    return df_grouped

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
