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

import os
import json
import time
import base64
import docx2txt
import pdfplumber
import pandas as pd
from datetime import date
from docx import Document
from PyPDF2 import PdfReader, PdfWriter

from delta.tables import *
from pyspark.sql.functions import lit, col, concat_ws, length
from pyspark.sql.types import StringType

from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import DocumentAnalysisFeature

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_docx_file(file_path):
    try:
        with open(file_path, "rb") as f:
            file_data = f.read()
            base64_data = base64.b64encode(file_data).decode("utf-8")

        poller = document_analysis_client.begin_analyze_document(
            model_id="prebuilt-layout",
            analyze_request={"base64Source": base64_data},
        )

        result = poller.result()["content"]
        return result, 'ADI'
    except Exception as e:
        result = docx2txt.process(file_path)
        return result, 'docx2txt'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stg_path = notebookutils.fs.ls('./')[0].path
container_name = stg_path.split('@')[0].split('//')[1]
file_system = stg_path.split('@')[1].split('/')[1]

print(container_name, stg_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

onelakepath = "abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623" 
delta_table_path = "Tables/br_m365_sharepoint_file_content_ADI"
merge_keys = ['filename', 'file_modified_on']
src_table_path = "Tables/br_m365_sharepoint_download_file_status"
processed_files_list = []

endpoint = "https://vpc-dev-documentintelligence.cognitiveservices.azure.com/"
key = "e7b23cd86e7c4b7181bbc8019e5d97f6"

document_analysis_client = DocumentIntelligenceClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
)

temp = spark.createDataFrame([], "id:string, filetype: string, filename: string, file_extension: string, sharepointURL: string, BronzeFilePath: string, file_created_on: string, file_modified_on: string, content: string, tags:string, content_extractor:string")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read data from the table
df = spark.read.load(src_table_path)

#Filter the downloaded data and only selecting the required columns
df = df.filter(df.download_status == 1)
df = df.filter(df.data_tag != 'Other')

df = df.withColumnRenamed('display_name', 'file_name')\
       .withColumnRenamed('sharepoint_url', 'file_url')\
       .withColumnRenamed('last_modified_datetime', 'file_modified_datetime')\
       .withColumnRenamed('created_datetime', 'file_created_datetime')\
       .withColumnRenamed('custom_tags', 'tags')

df = df.select('id', 'data_tag', 'file_name', 'file_url', 'bronze_save_path', 'tags', 'file_created_datetime', 'file_modified_datetime')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

new_row1 = Row(id='01LC474IUUCFOXL2P6XJF37CT5RCUWPB6G', data_tag='VPC_OLT DEMO data', file_name='Daniel_DiFranscesco_Manual_Resume.docx', file_url='https://hullinc.sharepoint.com/:w:/r/sites/VerdantasProjectsCopilot2024/Project%20documents/VPC_OLT%20DEMO%20data/Daniel_DiFranscesco_Manual_Resume.docx?d=wc19fced3adee449bb39c324614172636&csf=1&web=1&e=NorqH9', bronze_save_path='/lakehouse/default/Files/bronze/microsoft365/sharepoint/2024/09/12/Daniel_DiFranscesco_Manual_Resume.docx', tags={'Water': ['Arsenic'], 'Engineering': ['Ampere', 'Civil Engineering', 'Concrete', 'Cone', 'Current', 'Design for Manufacturing', 'Drilling', 'Electrical Resistance', 'Environmental Studies', 'Geotechnical Engineering', 'Health', 'Length', 'Load and Resistance Factor Design', 'Materials Testing', 'Pascal', 'Polyethylene', 'Professional Engineer', 'Resistance', 'Siemens', 'Size', 'Traffic Engineering', 'Velocity'], 'Project\\u\x01Management': ['Construction Costs', 'Contract Administration', 'Contracts and Agreements', 'Design', 'Design for Manufacturability', 'Flexible Manufacturing Systems', 'Methodology', 'Monitoring and Control', 'Planning', 'Procurement', 'Project Budget', 'Project Evaluation', 'Project Leader', 'Project Management', 'Project Manager', 'Project Team Members (Initiation)', 'Quality', 'Scheduling', 'Scope', 'Task'], 'O\\u\x01G': ['Drilling', 'Environmental Protection Agency', 'Helium', 'Monitoring Wells', 'Oil Depot', 'Organizations', 'Planning'], 'Environmental': ['Air', 'Air Quality', 'Asbestos', 'Asphalt', 'Boron', 'Department of Transportation', 'Environmental Consulting', 'Environmental Protection Agency', 'European Agreement Concerning the International Carriage of Dangerous Goods by Inland Waterways', 'Family', 'Groundwater', 'Helium', 'Indoor Air Quality', 'Lead', 'Phosphorus', 'Recycling', 'Soil', 'Sustainable Building', 'Transportation', 'U.S. Green Building Council Leadership in Energy and Environmental Design', 'Water Drainage', 'Wells', 'Wells']}, file_created_datetime='2024-09-12T21:00:28Z', file_modified_datetime='2024-09-12T21:00:28Z')
new_df1 = spark.createDataFrame([new_row1])

new_row2 = Row(id='01LC474IUUCFOXL2P6XJF37CT5RCUWPB6G', data_tag='VPC_OLT DEMO data', file_name='Angela_Boyd_Manual_Resume.docx', file_url='https://hullinc.sharepoint.com/:w:/r/sites/VerdantasProjectsCopilot2024/Project%20documents/VPC_OLT%20DEMO%20data/Angela_Boyd_Manual_Resume.docx?d=w13efb39840b046a28aa3594faeaea9ae&csf=1&web=1&e=oQAnWg', bronze_save_path='/lakehouse/default/Files/bronze/microsoft365/sharepoint/2024/09/12/Angela_Boyd_Manual_Resume.docx', tags={'Water': ['Arsenic'], 'Engineering': ['Ampere', 'Civil Engineering', 'Concrete', 'Cone', 'Current', 'Design for Manufacturing', 'Drilling', 'Electrical Resistance', 'Environmental Studies', 'Geotechnical Engineering', 'Health', 'Length', 'Load and Resistance Factor Design', 'Materials Testing', 'Pascal', 'Polyethylene', 'Professional Engineer', 'Resistance', 'Siemens', 'Size', 'Traffic Engineering', 'Velocity'], 'Project\\u\x01Management': ['Construction Costs', 'Contract Administration', 'Contracts and Agreements', 'Design', 'Design for Manufacturability', 'Flexible Manufacturing Systems', 'Methodology', 'Monitoring and Control', 'Planning', 'Procurement', 'Project Budget', 'Project Evaluation', 'Project Leader', 'Project Management', 'Project Manager', 'Project Team Members (Initiation)', 'Quality', 'Scheduling', 'Scope', 'Task'], 'O\\u\x01G': ['Drilling', 'Environmental Protection Agency', 'Helium', 'Monitoring Wells', 'Oil Depot', 'Organizations', 'Planning'], 'Environmental': ['Air', 'Air Quality', 'Asbestos', 'Asphalt', 'Boron', 'Department of Transportation', 'Environmental Consulting', 'Environmental Protection Agency', 'European Agreement Concerning the International Carriage of Dangerous Goods by Inland Waterways', 'Family', 'Groundwater', 'Helium', 'Indoor Air Quality', 'Lead', 'Phosphorus', 'Recycling', 'Soil', 'Sustainable Building', 'Transportation', 'U.S. Green Building Council Leadership in Energy and Environmental Design', 'Water Drainage', 'Wells', 'Wells']}, file_created_datetime='2024-09-12T21:00:28Z', file_modified_datetime='2024-09-12T21:00:28Z')
new_df2 = spark.createDataFrame([new_row2])

new_row3 = Row(id='01LC474IUUCFOXL2P6XJF37CT5RCUWPB6G', data_tag='VPC_OLT DEMO data', file_name='Jeffery Plante_Manual_Resume.docx', file_url='https://hullinc.sharepoint.com/:w:/r/sites/VerdantasProjectsCopilot2024/Project%20documents/VPC_OLT%20DEMO%20data/Jeffery%20Plante_Manual_Resume.docx?d=w96d54c90b97f46d081f18769320704f2&csf=1&web=1&e=qIR7YX', bronze_save_path='/lakehouse/default/Files/bronze/microsoft365/sharepoint/2024/09/12/Jeffery Plante_Manual_Resume.docx', tags={'Water': ['Arsenic'], 'Engineering': ['Ampere', 'Civil Engineering', 'Concrete', 'Cone', 'Current', 'Design for Manufacturing', 'Drilling', 'Electrical Resistance', 'Environmental Studies', 'Geotechnical Engineering', 'Health', 'Length', 'Load and Resistance Factor Design', 'Materials Testing', 'Pascal', 'Polyethylene', 'Professional Engineer', 'Resistance', 'Siemens', 'Size', 'Traffic Engineering', 'Velocity'], 'Project\\u\x01Management': ['Construction Costs', 'Contract Administration', 'Contracts and Agreements', 'Design', 'Design for Manufacturability', 'Flexible Manufacturing Systems', 'Methodology', 'Monitoring and Control', 'Planning', 'Procurement', 'Project Budget', 'Project Evaluation', 'Project Leader', 'Project Management', 'Project Manager', 'Project Team Members (Initiation)', 'Quality', 'Scheduling', 'Scope', 'Task'], 'O\\u\x01G': ['Drilling', 'Environmental Protection Agency', 'Helium', 'Monitoring Wells', 'Oil Depot', 'Organizations', 'Planning'], 'Environmental': ['Air', 'Air Quality', 'Asbestos', 'Asphalt', 'Boron', 'Department of Transportation', 'Environmental Consulting', 'Environmental Protection Agency', 'European Agreement Concerning the International Carriage of Dangerous Goods by Inland Waterways', 'Family', 'Groundwater', 'Helium', 'Indoor Air Quality', 'Lead', 'Phosphorus', 'Recycling', 'Soil', 'Sustainable Building', 'Transportation', 'U.S. Green Building Council Leadership in Energy and Environmental Design', 'Water Drainage', 'Wells', 'Wells']}, file_created_datetime='2024-09-12T21:00:28Z', file_modified_datetime='2024-09-12T21:00:28Z')
new_df3 = spark.createDataFrame([new_row3])

new_row4 = Row(id='01LC474IUUCFOXL2P6XJF37CT5RCUWPB6G', data_tag='VPC_OLT DEMO data', file_name='Shannon_Gonzalez_Manual_Resume.docx', file_url='https://hullinc.sharepoint.com/:w:/r/sites/VerdantasProjectsCopilot2024/Project%20documents/VPC_OLT%20DEMO%20data/Shannon_Gonzalez_Manual_Resume.docx?d=wcb8c24d0fdcb4d75a81f1c9baa013198&csf=1&web=1&e=VOdy0n', bronze_save_path='/lakehouse/default/Files/bronze/microsoft365/sharepoint/2024/09/12/Shannon_Gonzalez_Manual_Resume.docx', tags={'Water': ['Arsenic'], 'Engineering': ['Ampere', 'Civil Engineering', 'Concrete', 'Cone', 'Current', 'Design for Manufacturing', 'Drilling', 'Electrical Resistance', 'Environmental Studies', 'Geotechnical Engineering', 'Health', 'Length', 'Load and Resistance Factor Design', 'Materials Testing', 'Pascal', 'Polyethylene', 'Professional Engineer', 'Resistance', 'Siemens', 'Size', 'Traffic Engineering', 'Velocity'], 'Project\\u\x01Management': ['Construction Costs', 'Contract Administration', 'Contracts and Agreements', 'Design', 'Design for Manufacturability', 'Flexible Manufacturing Systems', 'Methodology', 'Monitoring and Control', 'Planning', 'Procurement', 'Project Budget', 'Project Evaluation', 'Project Leader', 'Project Management', 'Project Manager', 'Project Team Members (Initiation)', 'Quality', 'Scheduling', 'Scope', 'Task'], 'O\\u\x01G': ['Drilling', 'Environmental Protection Agency', 'Helium', 'Monitoring Wells', 'Oil Depot', 'Organizations', 'Planning'], 'Environmental': ['Air', 'Air Quality', 'Asbestos', 'Asphalt', 'Boron', 'Department of Transportation', 'Environmental Consulting', 'Environmental Protection Agency', 'European Agreement Concerning the International Carriage of Dangerous Goods by Inland Waterways', 'Family', 'Groundwater', 'Helium', 'Indoor Air Quality', 'Lead', 'Phosphorus', 'Recycling', 'Soil', 'Sustainable Building', 'Transportation', 'U.S. Green Building Council Leadership in Energy and Environmental Design', 'Water Drainage', 'Wells', 'Wells']}, file_created_datetime='2024-09-12T21:00:28Z', file_modified_datetime='2024-09-12T21:00:28Z')
new_df4 = spark.createDataFrame([new_row4])

df = new_df1.union(new_df2).union(new_df3).union(new_df4)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files_list = df.collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for file in files_list:
    filename = file['file_name']
    file_path = file['bronze_save_path']
    abs_onelake_path = file_path.split('/lakehouse/default/')[1]
    file_name, extension = os.path.splitext(filename)
    file_type = file_path.split('.')[1]
    tags = json.dumps(file['tags'])
    extraction_flag = None

    try:

        files = mssparkutils.fs.ls(f'{onelakepath}/{abs_onelake_path}')
        file_size = files[0].size/(1024*1000)

        if file_size < 100:
            if file_type.lower() == 'docx':
                schema = StringType()
                doc_text, extraction_flag = read_docx_file(file_path)
            else:
                continue

        rdd = spark.sparkContext.parallelize([doc_text])
        df = spark.createDataFrame(rdd.map(lambda x: (x,)), schema=["text"])

        df_final = df.withColumn("filename", lit(file_name)).withColumn("sharepointURL", lit(file['file_url'])).withColumnRenamed('text', 'content').withColumn('id', lit(file['id'])).withColumn('file_extension', lit(file_type)).withColumn('tags', lit(tags)).withColumn('content_extractor', lit(extraction_flag))
        df_final = df_final.withColumn('filetype', lit(file['data_tag'])).withColumn('BronzeFilePath', lit(file['bronze_save_path'])).withColumn('file_created_on', lit(file['file_created_datetime'])).withColumn('file_modified_on', lit(file['file_modified_datetime']))
        # df_final = df_final.withColumn('Environmental', lit(file['Environmental'])).withColumn('Engineering', lit(file['Engineering'])).withColumn('Water', lit(file['Water'])).withColumn('Project_and_Management', lit(file['Project_and_Management'])).withColumn('O_and_G', lit(file['O_and_G']))
        df_final = df_final.select('id', 'filetype', 'filename', 'file_extension', 'sharepointURL', 'BronzeFilePath', 'file_created_on', 'file_modified_on', 'content', 'tags', 'content_extractor')
        temp = temp.union(df_final)
        print(f"{file_name} load completed")
        processed_files_list.append(file_name)
    
    except Exception as e:
        print('Error', e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp_1 = temp.filter(temp.file_extension != 'pdf')
display(temp_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merge_keys = ['filename', 'id', 'file_extension']
delta_table_path = "Tables/br_m365_sharepoint_file_content_ADI"

if DeltaTable.isDeltaTable(spark, delta_table_path):
    
    tgt_table = DeltaTable.forPath(spark, delta_table_path)

    merge_condition = "and".join(
        [f" target.{col} = updates.{col} " for col in merge_keys]
    )

    tgt_table.alias('target').merge(
        source=temp_1.alias("updates"), condition=merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

else:
    temp_1.write.format("delta").mode("append").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
