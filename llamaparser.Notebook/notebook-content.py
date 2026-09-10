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
# META     }
# META   }
# META }

# CELL ********************

# LLAMA_CLOUD_API_KEY="llx-LBsnbL68apVR0ROLLDNyJDqhJReLVsZOCl2QhOJRFgzgleMA"

import os 
os.environ['LLAMA_CLOUD_API_KEY'] = "llx-LBsnbL68apVR0ROLLDNyJDqhJReLVsZOCl2QhOJRFgzgleMA"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install llama-index-core llama-parse llama-index-readers-file python-dotenv
!pip install nest-asyncio


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install typing-extensions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # bring in our LLAMA_CLOUD_API_KEY
# from dotenv import load_dotenv
# load_dotenv()
import nest_asyncio
nest_asyncio.apply()

# bring in deps
from llama_parse import LlamaParse
from llama_index.core import SimpleDirectoryReader

# set up parser
parser = LlamaParse(
    result_type="text"  # "markdown" and "text" are available
)

# use SimpleDirectoryReader to parse our file
file_extractor = {".pdf": parser}
# pdf = SimpleDirectoryReader(input_files=['/lakehouse/default/Files/bronze/Resumes/20171120 Dempsey Timothy.pdf'], file_extractor=file_extractor).load_data()
pdf = SimpleDirectoryReader(input_files=['/lakehouse/default/Files/bronze/microsoft365/sharepoint/2024/07/23/036P000022358  AbbVie - Research Pedestrian Bridge GE - FINAL||1721734512.pdf'], file_extractor=file_extractor).load_data()

# print(documents)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdf_string = ""
for i in range(3):
    text = pdf[i].text
    pdf_string+=text

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_pdf = pdf[0].metadata['file_path']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdf_string

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
df = pd.DataFrame({'file_path':[path_pdf],'file_content':[pdf_string]})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print(doc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # bring in our LLAMA_CLOUD_API_KEY
# from dotenv import load_dotenv
# load_dotenv()
import nest_asyncio
nest_asyncio.apply()

# bring in deps
from llama_parse import LlamaParse
from llama_index.core import SimpleDirectoryReader

# set up parser
parser = LlamaParse(
    result_type="markdown"  # "markdown" and "text" are available
)

# use SimpleDirectoryReader to parse our file
file_extractor = {".docx": parser}
# docx = SimpleDirectoryReader(input_files=['/lakehouse/default/Files/Projects/20231023 draft PA jc.docx'], file_extractor=file_extractor).load_data()
docx = SimpleDirectoryReader(input_files=['/lakehouse/default/Files/bronze/microsoft365/sharepoint/2024/07/23/Tonn, Gina Resume||1721736298.docx'], file_extractor=file_extractor).load_data()

# print(documents)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

docx_string = ""
for i in range(3):
    text = docx[i].text
    docx_string+=text

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_docx= docx[0].metadata['file_path']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_data = pd.DataFrame({'file_path': [path_docx], 'file_content': [docx_string]})


# Using concat instead of append
df = pd.concat([df, new_data], ignore_index=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

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
