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

df1 = spark.read.load('Tables/sv_nds_drug_testing')
df2 = spark.read.load('Tables/br_nds_drug_testing')
df3 = spark.read.load('Tables/br_nds_marijuana_laws')
df4 = spark.read.load('Tables/br_nds_drug_screening')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df4)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

df1 = spark.read.load('Tables/sv_nds_drug_testing')
df2 = spark.read.load('Tables/br_nds_drug_testing')
df3 = spark.read.load('Tables/br_nds_marijuana_laws')
df4 = spark.read.load('Tables/br_nds_drug_screening')

pandas_df1 = df1.toPandas()
pandas_df2 = df2.toPandas()
pandas_df3 = df3.toPandas()
pandas_df4 = df4.toPandas()
 
# Write Pandas DataFrame to Excel using openpyxl
# excel_path = "abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Files/Archive/4-9-2024/excel_4_Sept_data.xlsx"
# excel_path = "Files/Archive/7-8-2024/excel_4_Sept_data.xlsx"

with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
    pandas_df1.to_excel(writer, sheet_name='Sheet1', index=False)
    pandas_df2.to_excel(writer, sheet_name='Sheet2', index=False)
    pandas_df3.to_excel(writer, sheet_name='Sheet3', index=False)
    pandas_df4.to_excel(writer, sheet_name='Sheet3', index=False)


# pandas_df.to_excel(excel_path, index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load('Tables/br_m365_sharepoint_file_content_ADI')
df = df.filter(df.filetype == 'VPC_OLT DEMO data')
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
df = spark.read.load('Tables/br_m365_sharepoint_file_content_ADI')
df = df.filter(df.filetype == 'VPC_OLT DEMO data')
pandas_df = df.toPandas()

# Write Pandas DataFrame to Excel using openpyxl
excel_path = "abfss://297572de-b7d7-4285-a88e-1388e2598d4a@onelake.dfs.fabric.microsoft.com/24113e54-6f3f-4157-8c30-a3c5e66de623/Files/Archive/29-8-2024/excel_VPC_extract.xlsx"
pandas_df.to_excel(excel_path, index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# !pip install docx2txt
# !pip install python-docx
# !pip install pywin32
# !pip install pywin32==305.1
!pip install pypandoc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
# import docx2txt
import pypandoc
# import win32com.client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src_table_path = "Tables/br_m365_sharepoint_download_file_status"

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
flist = df.collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from docx import Document

def read_docx(file_path):
    doc = Document(file_path)
    full_text = []
    for para in doc.paragraphs:
        full_text.append(para.text)
    return '\n'.join(full_text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for file in flist:
    filename = file['file_name']
    file_path = file['bronze_save_path']
    abs_onelake_path = file_path.split('/lakehouse/default/')[1]
    file_name, extension = os.path.splitext(filename)
    file_type = file_path.split('.')[1]

    if file_type == 'doc':
        print(filename)
        output = pypandoc.convert_file(file_path, 'docx')
        print(output)
        # convert_doc_to_docx(doc_path)
        # text = read_docx(file_path)
        # print(text)
        break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import comtypes.client
 
def convert_doc_to_docx(input_file, output_file):
    word = comtypes.client.CreateObject('Word.Application')
    doc = word.Documents.Open(input_file)
    doc.SaveAs(output_file, FileFormat=16)
    doc.Close()
    word.Quit()

input_file = r"C:\Users\padarsh\OneDrive - SGA\Documents\Document Intelligence\4_VERD_SF330.doc"
output_file = 'example.docx'
convert_doc_to_docx(input_file, output_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install comtypes


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import subprocess

subprocess.run(['wget', 'https://hackage.haskell.org/package/pandoc-1.17.0.3/pandoc-1.17.0.3.tar.gz'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ls

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pypandoc

output = pypandoc.convert_file('4_VERD_SF330.doc', 'docx')
print(output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# wget https://hackage.haskell.org/package/pandoc-1.17.0.3/pandoc-1.17.0.3.tar.gz
# tar xvzf pandoc-1.17.0.3.tar.gz
# cd pandoc-1.17.0.3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sh
# MAGIC sudo visudo


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sh
# MAGIC echo '#!/bin/bash' > /tmp/askpass.sh
# MAGIC echo 'echo ""' >> /tmp/askpass.sh
# MAGIC chmod +x /tmp/askpass.sh

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sh
# MAGIC export SUDO_ASKPASS=/tmp/askpass.sh
# MAGIC sudo -A dnf install ./LibreOffice_7.6.4_Linux_x86-64_rpm.tar.gz

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sh
# MAGIC # wget https://download.documentfoundation.org/libreoffice/stable/24.2.5/rpm/x86_64/LibreOffice_24.2.5_Linux_x86-64_rpm_sdk.tar.gz
# MAGIC # tar xzvf LibreOffice_24.2.5_Linux_x86-64_rpm_sdk.tar.gz.1
# MAGIC # cd LibreOffice_24.2.5.2_Linux_x86-64_rpm_sdk/RPMS
# MAGIC # rpm -ivh *.rpm
# MAGIC # sudo su "IA!#Sga5030#@!" 
# MAGIC echo "" | sudo -S yum install ./LibreOffice_24.2.5_Linux_x86-64_rpm_sdk.tar.gz
# MAGIC # sudo yum install ./LibreOffice_24.2.5_Linux_x86-64_rpm_sdk.tar.gz
# MAGIC # sudo dnf install ./LibreOffice_7.6.4_Linux_x86-64_rpm.tar.gz --allowerasing

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sh
# MAGIC yum install libreoffice unoconv

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sh
# MAGIC sudo su

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
