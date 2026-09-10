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
# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
# Initialize Spark Session"
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables


lakehouse_path = "Tables"
table_paths = {
    "doc_data": f"{lakehouse_path}/doc_index_sample_V1"
}
# Read tables into DataFrames
data = spark.read.format("delta").load(table_paths["doc_data"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Index Creation

# CELL ********************

%pip install --upgrade pip
%pip install openai==1.12.0
%pip install langchain==0.0.354



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



import requests
import json
import pandas as pd
search_api_key = "8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u"
EMBEDDING_LENGTH = 1536
search_service_name = "vpc-dev-aisearch"
index_name = "simple_fields_documents-fabric-final"
azure_search_endpoint = f"https://{search_service_name}.search.windows.net"
from langchain.text_splitter import RecursiveCharacterTextSplitter
from typing import List, Dict
import asyncio
from openai import AzureOpenAI
import re
azure_openai_key = "c9ce949e276146d4a66b3e16b32613d3" if len("c9ce949e276146d4a66b3e16b32613d3") > 0 else None
azure_openai_api_version = "2022-12-01"
azure_openai_endpoint = "https://vpc-dev-azureopenai.openai.azure.com/"
embedding_model_name = "text-embedding-ada-002"




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

# CELL ********************


# url = f"{azure_search_endpoint}/indexes/{index_name}?api-version=2023-11-01"
# # url = f"{azure_search_endpoint}/indexes/{index_name}?api-version=2024-07-01"
# # url = "https://vpc-dev-aisearch.search.windows.net/indexes(simple_fields_documents-fabric-final)?allowIndexDowntime=False&api-version=2024-07-01"

# payload = json.dumps(
#     {
#         "name": index_name,
#         "fields": [ 
#             {
#             "name": "Id",
#             "type": "Edm.String",
#             "key": True,
#             "filterable": True,
#             "searchable": True,
#             "retrievable": True,
#             "sortable": False,
#             "facetable": True               
#             },                           
#             {
#             "name": "DocumentCategory",
#             "type": "Edm.String",
#             "key": False,
#             "filterable": True,
#             "searchable": True,
#             "retrievable": True,
#             "sortable": False,
#             "facetable": True  
#             },
            
#                 {
#                 "name": "PracticeGroupName",
#                 "type": "Edm.String",
#                 "key": False,
#                 "filterable": True,
#                 "searchable": True,
#                 "retrievable": True,
#                 "sortable": False,
#                 "facetable": True  
#                 },
#                 {
#                 "name": "ClientId",
#                 "type": "Edm.String",
#                 "key": False,
#                 "filterable": True,
#                 "searchable": True,
#                 "retrievable": True,
#                 "sortable": False,
#                 "facetable": True  
#                 },
#                 {
#                 "name": "ClientName",
#                 "type": "Edm.String",
#                 "key": False,
#                 "filterable": True,
#                 "searchable": True,
#                 "retrievable": True,
#                 "sortable": False,
#                 "facetable": True  
#                 },
#                 {
#                 "name": "ClientFolderId",
#                 "type": "Edm.String",
#                 "key": False,
#                 "filterable": True,
#                 "searchable": True,
#                 "retrievable": True,
#                 "sortable": False,
#                 "facetable": True  
#                 },
#                 {
#                 "name": "Worked",
#                 "type": "Edm.String",
#                 "key": False,
#                 "filterable": True,
#                 "searchable": True,
#                 "retrievable": True,
#                 "sortable": False,
#                 "facetable": True  
#                 },
#                 {
#                 "name": "ClientAddressCity",
#                 "type": "Edm.String",
#                 "key": False,
#                 "filterable": True,
#                 "searchable": True,
#                 "retrievable": True,
#                 "sortable": False,
#                 "facetable": True  
#                 },
#                 {
#                 "name": "ClientAddressState",
#                 "type": "Edm.String",
#                 "key": False,
#                 "filterable": True,
#                 "searchable": True,
#                 "retrievable": True,
#                 "sortable": False,
#                 "facetable": True  
#                 },
#                 {
#                 "name": "EmployeeName",
#                 "type": "Edm.String",
#                 "key": False,
#                 "filterable": True,
#                 "searchable": True,
#                 "retrievable": True,
#                 "sortable": False,
#                 "facetable": True  
#                 },
#                 {
#                 "name": "EmployeeId",
#                 "type": "Edm.String",
#                 "key": False,
#                 "filterable": True,
#                 "searchable": True,
#                 "retrievable": True,
#                 "sortable": False,
#                 "facetable": True  
#                 },
                                                                                                
#                         {
#                         "name": "FileName",
#                         "type": "Edm.String",
#                         "key": False,
#                         "filterable": True,
#                         "searchable": True,
#                         "retrievable": True,
#                         "sortable": False,
#                         "facetable": True  
#                         },                       
#                         {
#                         "name": "SharepointUrl",
#                         "type": "Edm.String",
#                         "key": False,
#                         "filterable": False,
#                         "searchable": True,
#                         "retrievable": True,
#                         "sortable": False,
#                         "facetable": True
#                         }, 
#                         {
#                         "name": "TagsAssociated",
#                         "type": "Collection(Edm.String)",
#                         "key": False,
#                         "filterable": True,
#                         "searchable": True,
#                         "retrievable": True,
#                         "sortable": False,
#                         "facetable": True 
#                         },  
#                     #     {
#                     # "name": "content_chunk_id",
#                     # "type": "Edm.String",
#                     # "key": False,
#                     # "filterable": False,
#                     # "searchable": True,
#                     # "retrievable": True,
#                     # "sortable": False,
#                     # "facetable": False               
#                     # },
#                     {
#                     "name": "ContentChunk",
#                     "type": "Edm.String",
#                     "key": False,
#                     "filterable": False,
#                     "searchable": True,
#                     "retrievable": True,
#                     "sortable": False,
#                     "facetable": False               
#                     },
#                     {
#                     "name": "ContentChunkEmbeddings",
#                     "type": "Collection(Edm.Single)",
#                     "key": False,
#                     "filterable": False,
#                     "searchable": True,
#                     "retrievable": True,
#                     "sortable": False,
#                     "facetable": False,
#                     "dimensions": EMBEDDING_LENGTH,
#                     "vectorSearchProfile": "vectorConfig",
#                     }
#                     ],                                                                             
            

#         "vectorSearch": {
#             "algorithms": [{"name": "hnswConfig", "kind": "hnsw", "hnswParameters": {"metric": "cosine"}}],
#             "profiles": [{"name": "vectorConfig", "algorithm": "hnswConfig"}],
#     # "vectorSearch": {
#     #  "compressions": [
#     #      {
#     #          "name": "scalar-quantization",
#     #          "kind": "scalarQuantization",
#     #          "rerankWithOriginalVectors": True,
#     #          "defaultOversampling": 10.0,
#     #              "scalarQuantizationParameters": {
#     #                  "quantizedDataType": "int8"
#     #              }
#     #      },
#     #      {
#     #          "name": "binary-quantization",
#     #          "kind": "binaryQuantization",
#     #          "rerankWithOriginalVectors": True,
#     #          "defaultOversampling": 10.0,
#     #      }
#     #  ],
#     #  "algorithms": [
#     #      {
#     #          "name": "hnsw-1",
#     #          "kind": "hnsw",
#     #          "hnswParameters": {
#     #              "m": 4,
#     #              "efConstruction": 400,
#     #              "efSearch": 500,
#     #              "metric": "cosine"
#     #          }
#     #      },
#     #      {
#     #          "name": "hnsw-2",
#     #          "kind": "hnsw",
#     #          "hnswParameters": {
#     #              "m": 8,
#     #              "efConstruction": 800,
#     #              "efSearch": 800,
#     #              "metric": "hamming"
#     #          }
#     #      },
#     #      {
#     #          "name": "eknn",
#     #          "kind": "exhaustiveKnn",
#     #          "exhaustiveKnnParameters": {
#     #              "metric": "euclidean"
#     #          }
#     #      }

#     #         ],
#     #         "profiles": [
#     #         {
#     #             "name": "vector-profile-hnsw-scalar",
#     #             # "compression": "scalar-quantization",
#     #             "algorithm": "hnsw-1"
#     #         }
#     #         ],

#         "semantic": {
#             "configurations": [
#                 {
#                     "name": "azure-ml-default",
#                     "prioritizedFields": {
#                         "titleField": {"fieldName": "Id"},                
#                         "prioritizedContentFields": [{"fieldName": 'ContentChunk'}],
#                         "prioritizedKeywordsFields": [
#                             {"fieldName": 'DocumentCategory'},
#                             {"fieldName": 'FileName'},
#                             {"fieldName": 'ClientId'},
#                             {"fieldName": 'ClientName'},
#                             {"fieldName": 'EmployeeName'},
#                             {"fieldName": 'EmployeeId'},
#                             {"fieldName": 'SharepointUrl'},
#                             {"fieldName": 'ClientFolderId'},
#                             {"fieldName": 'TagsAssociated'},
#                             {"fieldName": 'Worked'},
#                             {"fieldName": 'ClientAddressCity'},
#                             {"fieldName": 'ClientAddressState'},
#                             {"fieldName": 'PracticeGroupName'},
#                             ],

#                     }
#                 }
#             ]
#         }
#     }
# })
# headers = {"Content-Type": "application/json", "api-key": search_api_key}

# response = requests.request("PUT", url, headers=headers, data=payload)
# if response.status_code == 201:
#     print('Index schema created!')
#     status = 'successful'
# elif response.status_code == 204:
#     status = 'successful'
#     print('Index schema updated!')
# else:
#     status = 'un-successful'
#     print(f'Index schema failed with HTTP request status code {response.status_code}')
#     raise Exception(f"HTTP response body: {response.text}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# url = f"{azure_search_endpoint}/indexes/{index_name}?api-version=2023-11-01"
url = f"{azure_search_endpoint}/indexes/{index_name}?api-version=2024-07-01"
payload = json.dumps(
    {
        "name": index_name,
        "fields": [ 
            {
            "name": "Id",
            "type": "Edm.String",
            "key": True,
            "filterable": True,
            "searchable": True,
            "retrievable": True,
            "sortable": False,
            "facetable": True               
            },                           
            {
            "name": "DocumentCategory",
            "type": "Edm.String",
            "key": False,
            "filterable": True,
            "searchable": True,
            "retrievable": True,
            "sortable": False,
            "facetable": True  
            },
            
                {
                "name": "PracticeGroupName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True  
                },
                {
                "name": "ClientId",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True  
                },
                {
                "name": "ClientName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True  
                },
                {
                "name": "ClientFolderId",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True  
                },
                {
                "name": "Worked",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True  
                },
                {
                "name": "ClientAddressCity",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True  
                },
                {
                "name": "ClientAddressState",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True  
                },
                {
                "name": "EmployeeName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True  
                },
                {
                "name": "EmployeeId",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True  
                },
                                                                                                
                        {
                        "name": "FileName",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True  
                        },                       
                        {
                        "name": "SharepointUrl",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": False,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True
                        }, 
                        {
                        "name": "TagsAssociated",
                        "type": "Collection(Edm.String)",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 
                        },  
                    #     {
                    # "name": "content_chunk_id",
                    # "type": "Edm.String",
                    # "key": False,
                    # "filterable": False,
                    # "searchable": True,
                    # "retrievable": True,
                    # "sortable": False,
                    # "facetable": False               
                    # },
                    {
                    "name": "ContentChunk",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False               
                    },
                    {
                    "name": "ContentChunkEmbeddings",
                    "type": "Collection(Edm.Byte)",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False,
                    "vectorEncoding": "packedBit", 
                    "dimensions": 1024, 
                    # "dimensions": EMBEDDING_LENGTH,
                    "vectorSearchProfile": "vectorConfig",
                    },
                    ],                                                                             
            

        "vectorSearch": {
            "algorithms": [{"name": "hnswConfig", "kind": "hnsw", "hnswParameters": {"metric": "hamming"}}],
            "profiles": [{"name": "vectorConfig", "algorithm": "hnswConfig"}],
        },
        "semantic": {
            "configurations": [
                {
                    "name": "azure-ml-default",
                    "prioritizedFields": {
                        "titleField": {"fieldName": "Id"},                
                        "prioritizedContentFields": [{"fieldName": 'ContentChunk'}],
                        "prioritizedKeywordsFields": [
                            {"fieldName": 'DocumentCategory'},
                            {"fieldName": 'FileName'},
                            {"fieldName": 'ClientId'},
                            {"fieldName": 'ClientName'},
                            {"fieldName": 'EmployeeName'},
                            {"fieldName": 'EmployeeId'},
                            {"fieldName": 'SharepointUrl'},
                            {"fieldName": 'ClientFolderId'},
                            {"fieldName": 'TagsAssociated'},
                            {"fieldName": 'Worked'},
                            {"fieldName": 'ClientAddressCity'},
                            {"fieldName": 'ClientAddressState'},
                            {"fieldName": 'PracticeGroupName'},
                            ],

                    }
                }
            ]
        }
    }
)
headers = {"Content-Type": "application/json", "api-key": search_api_key}

response = requests.request("PUT", url, headers=headers, data=payload)
if response.status_code == 201:
    print('Index schema created!')
    status = 'successful'
elif response.status_code == 204:
    status = 'successful'
    print('Index schema updated!')
else:
    status = 'un-successful'
    print(f'Index schema failed with HTTP request status code {response.status_code}')
    raise Exception(f"HTTP response body: {response.text}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



client = AzureOpenAI(
  api_key = azure_openai_key,  
  api_version = azure_openai_api_version,
  azure_endpoint =azure_openai_endpoint
)
### Data preprocessing & Chunking
class Document:
    def __init__(self, page_content: str, metadata: Dict = None):
        self.page_content = page_content
        self.metadata = metadata or {}

# Adjust the split_text_using_splitter function
def split_into_chunks(text: str, chunk_size=600, chunk_overlap=10, separators_1=None) -> List[str]:
    # Initialize the text splitter
    text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        chunk_size=chunk_size, 
        chunk_overlap=chunk_overlap,
        separators=separators_1, 
        is_separator_regex=False,
        encoding_name="cl100k_base"
    )
    
    # Create a Document object
    document = Document(page_content=text)
    
    # Split the document
    texts = text_splitter.split_documents([document])
    return [chunk.page_content for chunk in texts]

def chunking(df, columns,chunk_group):
    if len(columns)>0:
        print('content chunking!')
        for column in columns:
            new_col = column+'_chunk'    
            df[new_col] = df[column].apply(lambda x: split_into_chunks(x))

        for column in columns:
            new_col = column+'_chunk'
            df = df.explode(new_col).reset_index(drop=True)

        df['chunk_index'] = df.groupby(chunk_group).cumcount()

        # Add a unique identifier for each chunk
        df['unique_id'] = df['id'] + "_" + df['chunk_index'].astype(str)

        df = df.fillna("NA")

    return df

def get_embedding(text: str):
    result = client.embeddings.create(
    input = text,
    model= embedding_model_name)
    # return result['data'][0]['embedding']
    return json.loads(result.model_dump_json(indent=2))['data'][0]['embedding']

def create_embeddings(df, embd_col, con_col):
    print('Creating embeddings for the given input')
    embeddings = []
    for text in df[con_col]:
        embedding = get_embedding(text)
        embeddings.append(embedding)
    df[embd_col] = embeddings
    return df

def add_prefix_if_underscore(value):
    if value.startswith('_'):
        return 'A' + value
    return value


def main(df, columns, embd_col, chunk_group):
    df = chunking(df, columns, chunk_group)
    df = create_embeddings(df, embd_col, f'{columns[0]}_chunk')  # Assuming the first column is used for embedding
    df["unique_id"] = df["unique_id"].apply(add_prefix_if_underscore)
    print(f"IDs are unique - {df.shape[0] == df.unique_id.nunique()}")
    
    return df, 'successful'


def insert_into_index(documents):
    """Uploads a list of 'documents' to Azure AI Search index."""
    url = f"{azure_search_endpoint}/indexes/{index_name}/docs/index?api-version=2023-11-01"
    payload = json.dumps({"value": documents})
    headers = {
        "Content-Type": "application/json",
        "api-key": search_api_key,
    }
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200 or response.status_code == 201:
        return "Success"
    else:
        return f"Failure: {response.text}"
    

def make_safe_id(row_id: str):
    """Strips disallowed characters from row id for use as Azure AI search document ID."""
    return re.sub("[^0-9a-zA-Z_-]", "_", row_id)


def upload_document_rows(rows):
    """Uploads the rows in a dataframe to Azure AI Search.
    Limits uploads to 1000 rows at a time due to Azure AI Search API limits.
    """
    BATCH_SIZE = 300
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:
            documents.append(
                {
                    "Id": make_safe_id(row["Id"]),
                    "DocumentCategory": row["DocumentCategory"],
                    "PracticeGroupName": row["PracticeGroupName"],
                    "ClientId": row["ClientId"],
                    "ClientName": row["ClientName"],
                    "ClientFolderId": row["ClientFolderId"],
                    "Worked": row["Worked"],
                    "ClientAddressCity": row["ClientAddressCity"],
                    "ClientAddressState": row["ClientAddressState"],
                    "EmployeeName": row["EmployeeName"],
                    "EmployeeId": row["EmployeeId"],
                    "FileName": row["FileName"],
                    "SharepointUrl": row["SharepointUrl"],
                    "TagsAssociated": row["TagsAssociated"],
                    # "content_chunk_id": row["content_chunk_id"],
                    "ContentChunk": row["ContentChunk"],
                    "ContentChunkEmbeddings": row["ContentChunkEmbeddings"],                    
                    "@search.action": "upload",
                }
            )
        status = insert_into_index(documents)
        print([row_batch[0]["row_index"], row_batch[-1]["row_index"], status])
        yield [row_batch[0]["row_index"], row_batch[-1]["row_index"], status]





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import pandas as pd
import re
lakehouse_path = "Tables"
table_paths = {
    "doc_data": f"{lakehouse_path}/doc_index_sample_V1"
}
# Read tables into DataFrames
data = spark.read.format("delta").load(table_paths["doc_data"])
df = data.toPandas()

df = df[~df['client_id'].isna()]    
df = df.head()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = df[~df['client_id'].isna()]    

    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sample1 = df[0:5]
# sample2 = df[5:]
# sample2 = df[df['id'] == '01LC474IULYLAHL7NMIJALIAZSTXTFZNC7']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sample2.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sample2
# sample2['file_name'] = 'SOW_Evaluating_Plant_City_Treatment_Options_RFP'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df['content'] = df['content'].apply(lambda x:x.lower())
df['content'] = df['content'].apply(lambda x:re.sub(r'\n\d+\.', ' ', x))
def extract_unique_values(row):
    unique_values = []
    for key in row:
        unique_values.extend(row[key])
    return list(set(unique_values))
content_columns = ['content']
chunk_group = 'file_name'

# Apply the function to the 'tags_associated' column
df['tags_associated'] = df['tags_associated'].apply(lambda x: extract_unique_values(eval(x)))

result_df, status = main(df, content_columns, 'content_chunk_embedding', chunk_group)
result_df.drop(columns=['id'],inplace=True)


rename_dict = {
    'unique_id':'Id',
    'document_category':'DocumentCategory',
    'practice_group_name':'PracticeGroupName',
    'client_id':'ClientId',
    'client_name':'ClientName',
    'client_folder_id':'ClientFolderId',
    'worked':'Worked',
    'client_address_city':'ClientAddressCity',
    'client_address_state':'ClientAddressState',
    'employee_name':'EmployeeName',
    'employee_id':'EmployeeId',
    'file_name':'FileName',
    'sharepoint_url':'SharepointUrl',
    'content':'Content',
    'tags_associated':'TagsAssociated',
    'content_chunk':'ContentChunk',
    'content_chunk_embedding':'ContentChunkEmbeddings'
}

result_df =result_df.rename(columns=rename_dict)
result_df = result_df[['Id','DocumentCategory', 'PracticeGroupName', 'ClientId', 'ClientName',
       'ClientFolderId', 'Worked', 'ClientAddressCity', 'ClientAddressState',
       'EmployeeName', 'EmployeeId', 'FileName', 'SharepointUrl', 'Content',
       'TagsAssociated', 'ContentChunk',
       'ContentChunkEmbeddings']]


result_df['row_index'] = range(len(result_df))    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = result_df.head(8)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# result_df = result_df[result_df['Id'] == '01LC474IULYLAHL7NMIJALIAZSTXTFZNC7_0']
# result_df.loc[result_df['Id'] == '01LC474IULYLAHL7NMIJALIAZSTXTFZNC7_0', ['FileName', 'ContentChunk']] = ['xyz', 'abc']
# result_df = result_df[result_df['Id'] == '01LC474IULYLAHL7NMIJALIAZSTXTFZNC7_2']
# result_df.loc[result_df['Id'] == '01LC474IULYLAHL7NMIJALIAZSTXTFZNC7_2', ['FileName']] = ['xyz']


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows = result_df.to_dict(orient='records')
print('Uploading data!')
results = list(upload_document_rows(rows))
if results:
# Convert results to DataFrame for display
    res_df = pd.DataFrame(results, columns=["start_index", "end_index", "insertion_status"])
    print(f'Data Upload Successful!')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
# Initialize Spark Session"
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables


lakehouse_path = "Tables"
table_paths = {
    "doc_data": f"{lakehouse_path}/mvp_resource_index_data"
}
# Read tables into DataFrames
data = spark.read.format("delta").load(table_paths["doc_data"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data.count()

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
