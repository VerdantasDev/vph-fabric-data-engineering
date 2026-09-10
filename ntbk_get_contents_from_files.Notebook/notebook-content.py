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
# META     },
# META     "environment": {
# META       "environmentId": "1a059686-dca3-8ca8-44f7-bb496b74f668",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

!pip install docx2txt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run helper

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import json
import time
import docx2txt
import pdfplumber
import pandas as pd
from datetime import date
from docx import Document
from PyPDF2 import PdfReader, PdfWriter

from delta.tables import *
from pyspark.sql.functions import lit, col, concat_ws
from pyspark.sql.types import StringType

from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import DocumentAnalysisFeature

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

source_table_path = "Tables/br_acquia_files_ingestion_status_v1"
target_table_path = "Tables/br_acquia_content_ADI_v2"
source = "acquia"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_docx_file(document_analysis_client, file_path):
    """
    Reads content from docx file using ADI or docx2txt
    Args:
        document_analysis_client (client): Document Intelligence Client
        file_path (str): FilePath
    Returns:
        str: content of the files
        str: mechanism used to extract the content
    """

    try:
        with open(file_path, "rb") as f:
            file_data = f.read()
            base64_data = base64.b64encode(file_data).decode("utf-8")

        poller = document_analysis_client.begin_analyze_document(
            model_id="prebuilt-layout",
            analyze_request={"base64Source": base64_data},
        )

        result = poller.result()["content"]
        return result, "ADI"
    except Exception as e:
        result = docx2txt.process(file_path)
        return result, "docx2txt"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_pdf_file(document_analysis_client, file_path):
    """
    Reads content from pdf file using ADI or pdfplumber
    Args:
        document_analysis_client (client): Document Intelligence Client
        file_path (str): FilePath
    Returns:
        str: content of the files
        str: mechanism used to extract the content
    """

    try:
        with open(file_path, "rb") as f:
            poller = document_analysis_client.begin_analyze_document(
                "prebuilt-layout",
                analyze_request=f,
                features=[
                    DocumentAnalysisFeature.KEY_VALUE_PAIRS,
                    DocumentAnalysisFeature.STYLE_FONT,
                    DocumentAnalysisFeature.OCR_HIGH_RESOLUTION,
                ],
                content_type="application/octet-stream",
            )

        result = poller.result()["content"]
        return result, "ADI"
    except Exception as e:
        text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text += page.extract_text()
        return text, "pdfPlumber"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_files(target_schema, files_list):
    """
    Processes files from the files list to generate content from the files
    Args:
        target_schema (spark schema): Schema for the target Table
        files_list (list): list of all files to be processed
    Returns:
        dataframe: target dataframe with content for each file
        list: unprocessed files list
    """

    logger = []
    onelakepath = get_onelake_path()
    temp = spark.createDataFrame([], target_schema)

    for file in files_list:
        filename = file["file_name"]
        file_path = file["bronze_save_path"]
        abs_onelake_path = file_path.split("/lakehouse/default/")[1]
        file_name, extension = os.path.splitext(filename)
        if len(extension) == 0:
            continue
        else:
            file_type = extension.split('.')[1]
        extraction_flag = None

        try:

            files = mssparkutils.fs.ls(f"{onelakepath}/{abs_onelake_path}")
            file_size = files[0].size / (1024 * 1000)

            if file_size < 100:
                if file_type.lower() == "docx":
                    schema = StringType()
                    doc_text, extraction_flag = read_docx_file(
                        document_analysis_client, file_path
                    )
                elif file_type.lower() == "pdf":
                    doc_text, extraction_flag = read_pdf_file(
                        document_analysis_client, file_path
                    )
                elif file_type.lower() == "doc":
                    print("File type not supported")
                    logger.append(filename)
                    continue
                else:
                    continue
            else:
                continue

            rdd = spark.sparkContext.parallelize([doc_text])
            df = spark.createDataFrame(rdd.map(lambda x: (x,)), schema=["text"])

            df_final = (
                df.withColumn("id", lit(file["id"]))
                .withColumn("filename", lit(file_name))
                .withColumn("file_extension", lit(file_type))
                .withColumn("file_url", lit(file["file_url"]))
                .withColumn("BronzeFilePath", lit(file["bronze_save_path"]))
                .withColumn("file_created_on", lit(file["file_created_datetime"]))
                .withColumn("file_modified_on", lit(file["file_modified_datetime"]))
                .withColumn("content_extractor", lit(extraction_flag))
                .withColumn("source", lit(source))
                .withColumnRenamed("text", "content")
            )

            df_final = df_final.select(
                "id",
                "filename",
                "file_extension",
                "file_url",
                "BronzeFilePath",
                "file_created_on",
                "file_modified_on",
                "content",
                "content_extractor",
                "source",
            )
            temp = temp.union(df_final)
            print(f"{file_name} load completed")

        except Exception as e:
            logger.append(filename)
            continue

    return temp, logger


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_files_list_to_process(source_df, target_table_path, source):
    """
    From the source dataframe filters only the unprocessed files
    Args:
        source_df (DataFrame): source dataframe with exact matching columns file_name, bronze_save_path, file_modified_datetime, download_status
        target_table_path (str): Target Table path i.e. Table with extracted contents
    Returns:
        Row type: List of unprocessed files
    """

    # Read ADI content table in df
    if DeltaTable.isDeltaTable(spark, target_table_path):
        df_content = spark.read.load(target_table_path)
        # df_content = df_content.filter(df_content.source == source)
        df_content = df_content.withColumn(
            "file_name", concat_ws(".", df_content.filename, df_content.file_extension)
        )
        df_content = df_content.select(col("file_name"), "file_modified_on")

        # Join status df with final table df to get the files whose content is not extracted
        df_final_files = source_df.join(
            df_content,
            on=[
                source_df.file_name == df_content.file_name,
                source_df.file_modified_datetime == df_content.file_modified_on,
            ],
            how="leftanti",
        )

        # collect list
        files_list = df_final_files.collect()
    else:
        files_list = source_df.collect()

    return files_list


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
    Extract contents from files
    Returns:
       str : A stringified dictionary with unprocessed files list
"""

try:
    resource_mapper = resource_name_mapper()

    # Extract contents from Input params
    # input_json = json.loads(InputParam)
    # src_table_path = input_json["source_table"]
    # target_table_path = input_json["tgt_table"]
    # source = input_json["source"]

    # Build endpoints and retrieve values from KV
    endpoint = f"https://{resource_mapper['document_intelligence']}.cognitiveservices.azure.com/"
    key = mssparkutils.credentials.getSecret(
        f'https://{resource_mapper["key_vault"]}.vault.azure.net/', "vpc-app-di-key"
    )

    # Fetch Target schema and merge keys
    target_schema, merge_keys = get_tgt_table_schema(source)
    target_columns = [field.name for field in list(target_schema)]

    # Document Client
    document_analysis_client = DocumentIntelligenceClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )

    # Read and prepare source data for content extraction
    df = spark.read.load(source_table_path)
    df, source_columns = prepare_source_dataframe(source, df)

    # Filter files to be processed
    files_list = get_files_list_to_process(df, target_table_path, source)

    # Dataframe with contents extracted
    df_final, logger = process_files(target_schema, files_list)

    # Prepare target dataframe with selecting all extra columns as per sourcetype
    final_df = df_final.join(df.select('id', *source_columns), on = ['id'], how='inner')

    # Write Data in target table
    write_to_target(final_df, target_table_path, merge_keys)

except Exception as e:
    print("error", e)
    raise Exception(e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(logger)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
