# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {}
# META   }
# META }

# CELL ********************

!pip install azure-search-documents
!pip install azure-identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.identity import ClientSecretCredential
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchIndex, SimpleField, SearchableField, SearchFieldDataType, ComplexField

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

name = "temp"
service_endpoint = 'https://vpc-dev-aisearch.search.windows.net'
api_key = ''

credential = AzureKeyCredential(api_key)
search_index_client = SearchIndexClient(service_endpoint, credential)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

indexes = search_index_client.list_indexes()
index_list = [index.name for index in indexes]
print(index_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if name not in index_list:
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True),
        SimpleField(name="role", type=SearchFieldDataType.String),
        SimpleField(name="areas_of_expertise", type=SearchFieldDataType.String),
        SimpleField(name="experience", type=SearchFieldDataType.String),
        SimpleField(name="education", type=SearchFieldDataType.String),
        SimpleField(name="licenses_and_certifications", type=SearchFieldDataType.String),
        SimpleField(name="name_of_projects", type=SearchFieldDataType.String),
        SimpleField(name="filetype", type=SearchFieldDataType.String),
        SimpleField(name="sharepointURL", type=SearchFieldDataType.String),
        SimpleField(name="BronzeFilePath", type=SearchFieldDataType.String),
        SimpleField(name="file_created_on", type=SearchFieldDataType.String),
        SimpleField(name="file_modified_on", type=SearchFieldDataType.String),
        SimpleField(name="content", type=SearchFieldDataType.String),
        SimpleField(name="text", type=SearchFieldDataType.String),
        SimpleField(name="SOW", type=SearchFieldDataType.String),
        SimpleField(name="Requirements", type=SearchFieldDataType.String),
        SimpleField(name="Timeline", type=SearchFieldDataType.String),
    ]

    search_index = SearchIndex(name=name, fields=fields)
    result = search_index_client.create_or_update_index(search_index) 
    print(result)
else:
    print(f"{name} index already present")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
