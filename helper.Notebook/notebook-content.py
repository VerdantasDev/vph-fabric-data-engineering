# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import split, col, when, lit, size, lower
from pyspark.sql.types import StructType, StructField, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_tgt_table_schema(source):
    """
        Generates target table schema and merge keys for the source type
        Args:
            source (str): Source type supported value: sharepoint, acquia
        Returns:
            spark schema: Target table schema
            list: Merge Keys
    """

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("file_extension", StringType(), True),
        StructField("file_url", StringType(), True),
        StructField("BronzeFilePath", StringType(), True),
        StructField("file_created_on", StringType(), True),
        StructField("file_modified_on", StringType(), True),
        StructField("content", StringType(), True),
        StructField("content_extractor", StringType(), True),
        StructField("source", StringType(), True)
    ])

    if source.lower() == 'sharepoint':    
        merge_keys = ['filename', 'id', 'file_extension']
    elif source.lower() == 'acquia':
        merge_keys = ['filename', 'id', 'file_extension']

    return schema, merge_keys


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def prepare_source_dataframe(source, df):
    """
        Prepares the source dataframe in the expected format for ADI integration
        Args:
            source (str): Source type supported value: sharepoint, acquia
            df (df) : source data in a spark dataframe
        Returns:
            df : Processed dataframe as per ADI rules
            list : Extra columns that are to be appended in target table
    """

    if source.lower() == 'sharepoint': 

        df = df.filter(df.download_status == 1)
        df = df.filter(df.data_tag != 'Other')

        df = df.withColumnRenamed('display_name', 'file_name')\
               .withColumnRenamed('sharepoint_url', 'file_url')\
               .withColumnRenamed('last_modified_datetime', 'file_modified_datetime')\
               .withColumnRenamed('created_datetime', 'file_created_datetime')\
               .withColumnRenamed('custom_tags', 'tags')

        df = df.select(
            "id",
            "data_tag",
            "file_name",
            "file_url",
            "bronze_save_path",
            "tags",
            "file_created_datetime",
            "file_modified_datetime",
        )

        relevant_cols = ['data_tag', 'tags']

    elif source.lower() == 'acquia':

        df = df.filter(df.download_status == 1)

        supported_extensions = ["docx", 'pdf']

        df = df.withColumn("extension", split(col("asset_name"), "\."))\
               .withColumn('arrsize', size(col('extension')))\
               .withColumn('temp', when(col('arrsize') >= 2, col('extension')[col('arrsize')-1]).otherwise(None))\
               .filter(col('temp').isNotNull())\
               .filter(lower(col('temp')).isin(supported_extensions))

        df = df.withColumnRenamed("asset_name", "file_name")\
               .withColumnRenamed("asset_url", "file_url")\
               .withColumnRenamed("asset_modified_date", "file_modified_datetime")\
               .withColumnRenamed("asset_created_date", "file_created_datetime")\
               .withColumnRenamed("asset_id", "id")

        df = df.select(
            "id",
            "file_name",
            "file_url",
            "bronze_save_path",
            "file_created_datetime",
            "file_modified_datetime",
            "collection_name",
            "assettype",
            "client",
            "companyLegacy",
            "expertise",
            "keywords",
            "projectName",
            "solutions",
            "state",
            "IRecognitionKeywords",
        )

        relevant_cols = [
            "collection_name",
            "assettype",
            "client",
            "companyLegacy",
            "expertise",
            "keywords",
            "projectName",
            "solutions",
            "state",
            "IRecognitionKeywords",
        ]


    return df, relevant_cols


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
