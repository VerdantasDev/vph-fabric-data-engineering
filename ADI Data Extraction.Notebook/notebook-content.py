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

# Welcome to your new notebook
# Type here in the cell editor to add code!

# Azure AI Search
AI_SEARCH_NAME = "vpc-dev-aisearch"
# AI_SEARCH_INDEX_NAME = "rag-demo-index"
AI_SEARCH_API_KEY = ""

# Azure AI Services
AI_SERVICES_KEY = "e7b23cd86e7c4b7181bbc8019e5d97f6"
AI_SERVICES_LOCATION = "eastus"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# !pip install python-docx 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from synapse.ml.services import AnalyzeDocument

# Define the document path
document_path = "Files/bronze/microsoft365/sharepoint/2024/07/23/2024+Professional+Services+On-Call+Outreach+Event||1721734537.pdf"
document_path = "Files/bronze/microsoft365/sharepoint/2024/08/01/2024-06-19_Executed Contract_$339500||1722512993.pdf"
# Read binary file and select necessary columns
df = spark.read.format("binaryFile").load(document_path).select("_metadata.file_path", "content").limit(10).cache()

# Configure the AnalyzeDocument transformer
analyze_document = (
    AnalyzeDocument()
    .setPrebuiltModelId("prebuilt-layout")
    .setSubscriptionKey(AI_SERVICES_KEY)
    .setLocation(AI_SERVICES_LOCATION)
    .setImageBytesCol("content")
    .setOutputCol("result")
)

# Apply the document analysis and extract necessary columns
analyzed_df = (
    analyze_document.transform(df)
    .withColumn("output_content", col("result.analyzeResult.content"))
    .withColumn("paragraphs", col("result.analyzeResult.paragraphs"))
    .drop("content")
).cache()

# Convert to Pandas DataFrame and print the output content of the first document
df_pandas = analyzed_df.toPandas()
# print(df_pandas['output_content'][0])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# layout
print(df_pandas['output_content'][0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# document model
print(df_pandas['output_content'][0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import requests
# import os
# from docx import Document
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

# # # Download the DOCX file
# # url = "Files/bronze/microsoft365/sharepoint/2024/06/28/Bouwman_Michelle_Resume||1719572013.docx"
# # response = requests.get(url)

# # # Specify your path here
# path = "Files/bronze/microsoft365/sharepoint/2024/06/28/Bouwman_Michelle_Resume||1719572013.docx"

# # Ensure the directory exists
# # os.makedirs(path, exist_ok=True)

# # # Write the content to a file in the specified path
# # filename = url.rsplit("/")[-1]
# # file_path = os.path.join(path, filename)
# # with open(file_path, "wb") as f:
# #     f.write(response.content)

# # Function to read DOCX file content
# def read_docx(file_path):
#     doc = Document(file_path)
#     full_text = []
#     for para in doc.paragraphs:
#         full_text.append(para.text)
#     return '\n'.join(full_text)

# # Register UDF to read DOCX content
# read_docx_udf = udf(lambda path: read_docx(path), StringType())

# # Create DataFrame with file path
# df = spark.createDataFrame([(path,)], ["path"])

# # Add content column with DOCX content
# df = df.withColumn("content", read_docx_udf("path")).cache()

# # Display DataFrame
# display(df)

# # Analyze the document using Azure services
# from synapse.ml.services import AnalyzeDocument
# from pyspark.sql.functions import col

# analyze_document = (
#     AnalyzeDocument()
#     .setPrebuiltModelId("prebuilt-layout")
#     .setSubscriptionKey(AI_SERVICES_KEY)
#     .setLocation(AI_SERVICES_LOCATION)
#     .setImageBytesCol("content")
#     .setOutputCol("result")
# )

# analyzed_df = (
#     analyze_document.transform(df)
#     .withColumn("output_content", col("result.analyzeResult.content"))
#     .withColumn("paragraphs", col("result.analyzeResult.paragraphs"))
# ).cache()

# display(analyzed_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import requests
# import os
# from docx import Document
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

# # Specify your path here
# path = "Files/bronze/microsoft365/sharepoint/2024/06/28/Bouwman_Michelle_Resume||1719572013.docx"

# # Function to read DOCX file content
# def read_docx(file_path):
#     doc = Document(file_path)
#     full_text = []
#     for para in doc.paragraphs:
#         full_text.append(para.text)
#     return '\n'.join(full_text)

# # Register UDF to read DOCX content
# read_docx_udf = udf(lambda path: read_docx(path), StringType())

# # Create a DataFrame with file path
# df = spark.createDataFrame([(path,)], ["path"])

# # Add content column with DOCX content
# df = df.withColumn("content", read_docx_udf("path")).cache()

# # Display DataFrame
# # df.show(truncate=False)  # Use show() for Spark DataFrames instead of display()

# # Analyze the document using Azure services
# from synapse.ml.services import AnalyzeDocument
# from pyspark.sql.functions import col
# analyze_document = (
#     AnalyzeDocument()
#     .setPrebuiltModelId("prebuilt-layout")
#     .setSubscriptionKey(AI_SERVICES_KEY)
#     .setLocation(AI_SERVICES_LOCATION)
#     .setImageBytesCol("content")
#     .setOutputCol("result")
# )

# analyzed_df = (
#     analyze_document.transform(df)
#     .withColumn("output_content", col("result.analyzeResult.content"))
#     .withColumn("paragraphs", col("result.analyzeResult.paragraphs"))
# ).cache()

# # analyzed_df.show(truncate=False)  # Use show() for Spark DataFrames instead of display()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(analyzed_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_pandas = analyzed_df.toPandas()
# print(df_pandas['output_content'][0])

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
