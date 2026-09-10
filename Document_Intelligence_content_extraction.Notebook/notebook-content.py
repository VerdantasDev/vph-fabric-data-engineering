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

%pip install azure-core
%pip install azure-ai-documentintelligence

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import DocumentAnalysisFeature


endpoint = "https://vpc-dev-documentintelligence.cognitiveservices.azure.com/"
key = "e7b23cd86e7c4b7181bbc8019e5d97f6"


document_analysis_client = DocumentIntelligenceClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )


with open('/lakehouse/default/Files/bronze/microsoft365/sharepoint/2024/08/01/2024-06-19_Executed Contract_$339500||1722512993.pdf', "rb") as f:
    poller = document_analysis_client.begin_analyze_document(
            "prebuilt-layout",
            analyze_request=f,
            features=[DocumentAnalysisFeature.KEY_VALUE_PAIRS,DocumentAnalysisFeature.STYLE_FONT,DocumentAnalysisFeature.OCR_HIGH_RESOLUTION],
            content_type="application/octet-stream",
        )
result = poller.result()

# print(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(result['content'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import pandas as pd
# from azure.core.credentials import AzureKeyCredential
# from azure.ai.documentintelligence import DocumentIntelligenceClient
# from azure.ai.documentintelligence.models import DocumentAnalysisFeature


# Initialize Azure Document Analysis client
endpoint = "https://vpc-dev-documentintelligence.cognitiveservices.azure.com/"
key = "e7b23cd86e7c4b7181bbc8019e5d97f6"
document_analysis_client = DocumentIntelligenceClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )

# Directory containing the PDFs
directory = '/lakehouse/default/Files/bronze/microsoft365/sharepoint/2024/08/06'

# List to store data for DataFrame
data = []
count = 1
# Iterate over all PDF files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".pdf") and count < 10:
        file_path = os.path.join(directory, filename)
        with open(file_path, "rb") as f:
            poller = document_analysis_client.begin_analyze_document(
                "prebuilt-layout",
                analyze_request=f,
                features=[DocumentAnalysisFeature.KEY_VALUE_PAIRS,DocumentAnalysisFeature.STYLE_FONT,DocumentAnalysisFeature.OCR_HIGH_RESOLUTION],
                content_type="application/octet-stream",
            )
            result = poller.result()
            content = result['content']
            # Append to data list
            data.append({
                "url": file_path,
                "content": content
            })
            count+=1

# Create DataFrame
df = pd.DataFrame(data)

# Display or save DataFrame
print(df.head(2))
# df.to_csv('extracted_content.csv', index=False)  # To save the DataFrame to a CSV file

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
