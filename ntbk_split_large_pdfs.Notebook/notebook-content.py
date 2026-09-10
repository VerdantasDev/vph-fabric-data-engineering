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
import traceback
import json
from PyPDF2 import PdfReader, PdfWriter

from delta.tables import *
from pyspark.sql.functions import lit, col, concat_ws

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

source_table_path = "Tables/br_acquia_files_ingestion_status"
target_table_path = "Tables/br_acquia_content_ADI"
source = "acquia"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def split_pdf_by_size(input_pdf_path, output_folder, max_size_mb=20):

    """
        Splits large pdfs into small files of 20Mb each
        Args:
            input_pdf_path (str): File path for the files to be processed
            output_folder (str): Output Dir to create the files
            max_size_mb (int): Default to 20Mb
        Returns:
            int: Number of generated files
    """

    # Convert MB to bytes
    max_size_bytes = max_size_mb * 1024 * 1024
    
    # Create a PdfReader object
    reader = PdfReader(input_pdf_path)
    total_pages = len(reader.pages)

    # Initialize variables
    writer = PdfWriter()
    output_file_number = 1
    current_output_size = 0

    for i in range(total_pages):
        # Add the page to the writer
        writer.add_page(reader.pages[i])

        # Estimate the size of the current output PDF
        temp_output = os.path.join(output_folder, f'temp_output.pdf')
        with open(temp_output, 'wb') as temp_pdf:
            writer.write(temp_pdf)
        
        current_output_size = os.path.getsize(temp_output)
        
        # Check if the current output size exceeds the maximum allowed size
        if current_output_size > max_size_bytes:
            # Save the current writer's content to a new PDF file
            output_pdf_path = os.path.join(output_folder, f'output_{output_file_number}.pdf')
            with open(output_pdf_path, 'wb') as output_pdf:
                writer.write(output_pdf)
            
            # Increment the file number and reset the writer and size
            output_file_number += 1
            writer = PdfWriter()
            current_output_size = 0
    
    # Save the last PDF file
    if len(writer.pages) > 0:
        output_pdf_path = os.path.join(output_folder, f'output_{output_file_number}.pdf')
        with open(output_pdf_path, 'wb') as output_pdf:
            writer.write(output_pdf)

    # Cleanup temporary file
    os.remove(temp_output)

    return output_file_number

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

    #Read ADI content table in df
    if DeltaTable.isDeltaTable(spark, target_table_path):
        df_content = spark.read.load(target_table_path)
        df_content = df_content.filter(df_content.source == source)
        df_content = df_content.withColumn('file_name', concat_ws('.', df_content.filename, df_content.file_extension))
        df_content = df_content.select(col('file_name'), 'file_modified_on')

        # Join status df with final table df to get the files whose content is not extracted
        df_final_files = source_df.join(
            df_content,
            on=[
                source_df.file_name == df_content.file_name,
                source_df.file_modified_datetime == df_content.file_modified_on,
            ],
            how="leftanti",
        )
        #collect list
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

def process_large_files(files_list, base_output_folder):

    """
        Processes large files to split them and create small files
        Args:
            files_list (List): List of files to process
            base_output_folder (str): Path where split files will be written
        Returns:
            list: A list of dictionary with metadata for the splitted files
            list: A list of unprocessed files which needs to be reviewed
    """

    error_logger = []
    output = []
    onelakepath = get_onelake_path()

    for file in files_list:
        filename = file['file_name']
        file_path = file['bronze_save_path']
        abs_onelake_path = file_path.split('/lakehouse/default/')[1]
        file_name, extension = os.path.splitext(filename)

        if len(extension) == 0:
            continue
        else:
            file_type = extension.replace('.','')

        try:

            files = mssparkutils.fs.ls(f'{onelakepath}/{abs_onelake_path}')
            file_size = files[0].size/(1024*1000)

            if file_size > 100 and file_type.lower() == 'pdf':
                print(f"{file_name} File size: {file_size}")

                input_pdf_path = file_path
                output_folder = f'{base_output_folder}/{file_name}'
                os.makedirs(output_folder, exist_ok=True)
                
                temp =  [files for files in os.listdir(output_folder)]
                if len(temp) > 1:
                    output.append({
                    'output_path' : output_folder,
                    'filecount' : len(temp),
                    'file_name' : filename
                })
                else:
                    output_file_number = split_pdf_by_size(input_pdf_path, output_folder)
                    output.append({
                        'output_path' : output_folder,
                        'filecount' : output_file_number,
                        'file_name' : filename
                    })
            else:
                continue
            
        except Exception as e:
            # print(f'Processing failed for the file: {filename}')
            # print(traceback.format_exc())
            error_logger.append(filename)
            continue
    
    return output, error_logger

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
    Splits lage files into small chunks (only supports PDF files)
    Returns:
        str : A stringified dictionary with unprocessed files list and process files list with metadata
        
"""

try:
    # Extract contents from Input params
    # input_json = json.loads(InputParam)
    # source_table_path = input_json['source_table']
    # target_table_path = input_json['tgt_table']
    # source = input_json['source']

    base_output_folder = '/lakehouse/default/Files/Archive/TESTING'

    #Read data from the table
    df = spark.read.load(source_table_path)
    df, source_columns = prepare_source_dataframe(source, df)

    # Filter files to be processed
    files_list = get_files_list_to_process(df, target_table_path, source)

    if len(files_list) != 0:
        output, error_logger = process_large_files(files_list, base_output_folder)
    else:
        error_logger = output = []

    exit_logger = {
        "output_files" : output,
        "error_logger" : error_logger
    }

    exitLog = json.dumps(exit_logger)

except Exception as e:
    print('Error', e)
    raise Exception(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(exitLog)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
