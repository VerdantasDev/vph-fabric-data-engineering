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
# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables


lakehouse_path = "Tables"
table_paths = {
    "web_data": f"{lakehouse_path}/sv_verdantas_website_data"
}
# Read tables into DataFrames
data = spark.read.format("delta").load(table_paths["web_data"])

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
index_name = "simple_fields_website-fabric"
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
index_api_version = "2023-11-01"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f"{azure_search_endpoint}/indexes/{index_name}?api-version={index_api_version}"
# url = f"{azure_search_endpoint}/indexes/{index_name}?api-version=2023-11-01"
payload = json.dumps(
        {
            "name": index_name,
            "fields": [
                # Unique identifier for each document
                {
                    "name": "Id",
                    "type": "Edm.String",
                    "key": True,
                    "filterable": True,
                    "searchable": True, 
                    "retrievable": True,               
                    "sortable": True,
                    "facetable": True                
                },
                {
                    "name": "ProjectName",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True               
                },
                {
                    "name": "ProjectUrl",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True               
                },            
                {
                    "name": "ProjectContent",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False
                },
                {
                    "name": "ExpertiseName",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True 

                }, 
                {
                    "name": "ExpertiseUrl",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True 

                },                     
                {
                    "name": "ExpertiseContent",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False
                },
                {
                    "name": "ExpertiseContactPerson",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True 

                }, 
                {
                    "name": "ExpertiseContactDesignation",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True 

                },
                {
                    "name": "ServiceName",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,   
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True           
                },
                {
                    "name": "ServiceContent",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False
                }, 
                {
                    "name": "MarketName",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True, 
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True              
                },
                {
                    "name": "MarketUrl",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True, 
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True              
                },            
                {
                    "name": "MarketContent",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False
                },
                {
                    "name": "MarketContactPerson",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True, 
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True              
                }, 
                {
                    "name": "MarketContactDesignation",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True, 
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True              
                },   
                {
                    "name": "SolutionName",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True            
                },
                {
                    "name": "SolutionUrl",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": True,
                    "facetable": True            
                },            
                {
                    "name": "SolutionContent",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False
                },            
                # Vector embedding of the text content
                {
                    "name": "ProjectContentEmbedding",
                    "type": "Collection(Edm.Single)",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False,
                    "dimensions": EMBEDDING_LENGTH,
                    "vectorSearchProfile": "vectorConfig",
                },
                {
                    "name": "ExpertiseContentEmbedding",
                    "type": "Collection(Edm.Single)",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False,
                    "dimensions": EMBEDDING_LENGTH,
                    "vectorSearchProfile": "vectorConfig",
                },
                {
                    "name": "ServiceContentEmbedding",
                    "type": "Collection(Edm.Single)",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False,
                    "dimensions": EMBEDDING_LENGTH,
                    "vectorSearchProfile": "vectorConfig",
                },
                {
                    "name": "MarketContentEmbedding",
                    "type": "Collection(Edm.Single)",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False,
                    "dimensions": EMBEDDING_LENGTH,
                    "vectorSearchProfile": "vectorConfig",
                },
                {
                    "name": "SolutionContentEmbedding",
                    "type": "Collection(Edm.Single)",
                    "key": False,
                    "filterable": False,
                    "searchable": True,
                    "retrievable": False,
                    "sortable": False,
                    "facetable": False,
                    "dimensions": EMBEDDING_LENGTH,
                    "vectorSearchProfile": "vectorConfig",
                },            
            ],
            "vectorSearch": {
                "algorithms": [{"name": "hnswConfig", "kind": "hnsw", "hnswParameters": {"metric": "cosine"}}],
                "profiles": [{"name": "vectorConfig", "algorithm": "hnswConfig"}],
            },
            "semantic": {
                "configurations": [
                    {
                        "name": "azure-ml-default",
                        "prioritizedFields": {
                            "titleField": {"fieldName": "Id"},                
                            "prioritizedContentFields": [
                                {"fieldName": "ProjectContent"},
                                {"fieldName": "ExpertiseContent"},
                                {"fieldName": "ServiceContent"},
                                {"fieldName": "MarketContent"},
                                {"fieldName": "SolutionContent"}                            
                            ],
                            "prioritizedKeywordsFields": [
                            {"fieldName": "ProjectName"},
                            {"fieldName": "ExpertiseName"},
                            {"fieldName": "ExpertiseContactPerson"},
                            {"fieldName": "ExpertiseContactDesignation"},
                            {"fieldName": "ServiceName"},
                            {"fieldName": "MarketName"},
                            {"fieldName": "MarketContactPerson"},
                            {"fieldName": "MarketContactDesignation"},
                            {"fieldName": "SolutionName"}]
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
    logging.warning(f'Index schema failed with HTTP request status code {response.status_code}')
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


def main(df,columns,chunk_group):
    df = chunking(df,columns,chunk_group)
    rename = {
        'SolutionContent_chunk':'SolutionContentChunk',
        'ProjectContent_chunk':'ProjectContentChunk',
        'MarketContent_chunk':'MarketContentChunk',
        'ServiceContent_chunk':'ServiceContentChunk',
        'ExpertiseContent_chunk':'ExpertiseContentChunk'
            }
    df = df.rename(columns = rename)

    uniqueExpertise = df[['ExpertiseName','ExpertiseContent','ExpertiseContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)
    unique_service = df[['ServiceName','ServiceContent','ServiceContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)
    unique_market = df[['MarketName','MarketContent','MarketContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)
    unique_project = df[['ProjectName','ProjectContent','ProjectContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)
    unique_solution = df[['SolutionName','SolutionContent','SolutionContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)

    unique_project = create_embeddings(unique_project,'ProjectContentEmbedding','ProjectContentChunk')
    uniqueExpertise = create_embeddings(uniqueExpertise,'ExpertiseContentEmbedding','ExpertiseContentChunk')
    unique_service = create_embeddings(unique_service,'ServiceContentEmbedding','ServiceContentChunk')
    unique_market = create_embeddings(unique_market,'MarketContentEmbedding','MarketContentChunk')
    unique_solution = create_embeddings(unique_solution,'SolutionContentEmbedding','SolutionContentChunk')

    df = pd.merge(df,uniqueExpertise[['ExpertiseName','ExpertiseContent','ExpertiseContentChunk','ExpertiseContentEmbedding']],how='left',on=['ExpertiseName','ExpertiseContent','ExpertiseContentChunk'])
    df = pd.merge(df,unique_service[['ServiceName','ServiceContent','ServiceContentChunk','ServiceContentEmbedding']],how='left',on=['ServiceName','ServiceContent','ServiceContentChunk'])
    df = pd.merge(df,unique_market[['MarketName','MarketContent','MarketContentChunk','MarketContentEmbedding']],how='left',on=['MarketName','MarketContent','MarketContentChunk'])
    df = pd.merge(df,unique_project[['ProjectName','ProjectContent','ProjectContentChunk','ProjectContentEmbedding']],how='left',on=['ProjectName','ProjectContent','ProjectContentChunk'])
    df = pd.merge(df,unique_solution[['SolutionName','SolutionContent','SolutionContentChunk','SolutionContentEmbedding']],how='left',on=['SolutionName','SolutionContent','SolutionContentChunk'])

    df["UniqueId"] = df["unique_id"].apply(add_prefix_if_underscore)
    print(f"Id's are unique - {df.shape[0]==df.UniqueId.nunique()}")   
    
    return df,'successful'


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


def upload_website_rows(rows):
    """Uploads the rows in a dataframe to Azure AI Search.
    Limits uploads to 1000 rows at a time due to Azure AI Search API limits.
    """
    BATCH_SIZE = 700
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:
            documents.append(
                {
                    "Id": make_safe_id(row["UniqueId"]),
                    "ProjectName": row["ProjectName"],
                    "ProjectUrl": row["ProjectUrl"],
                    "ProjectContent": row["ProjectContentChunk"],
                    "ExpertiseName": row["ExpertiseName"],
                    "ExpertiseUrl": row["ExpertiseUrl"],
                    "ExpertiseContent": row["ExpertiseContentChunk"],
                    "ExpertiseContactPerson": row["ExpertiseContactPerson"],
                    "ExpertiseContactDesignation": row["ExpertiseContactDesignation"],
                    "ServiceName": row["ServiceName"],
                    "ServiceContent": row["ServiceContentChunk"],
                    "MarketName": row["MarketName"],
                    "MarketUrl": row["MarketUrl"],
                    "MarketContent": row["MarketContentChunk"],
                    "MarketContactPerson": row["MarketContactPerson"],
                    "MarketContactDesignation": row["MarketContactDesignation"],
                    "SolutionName": row["SolutionName"],   
                    "SolutionUrl": row["SolutionUrl"],   
                    "SolutionContent": row["SolutionContentChunk"],          
                    "ProjectContentEmbedding": row["ProjectContentEmbedding"],
                    "ExpertiseContentEmbedding": row["ExpertiseContentEmbedding"],
                    "ServiceContentEmbedding": row["ServiceContentEmbedding"],
                    "MarketContentEmbedding": row["MarketContentEmbedding"],
                    "SolutionContentEmbedding": row["SolutionContentEmbedding"],
                    "@search.action": "upload",
                }
            )
        status = insert_into_index(documents) #search_client.merge_or_upload_documents(documents)
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

df = data.toPandas()
df =df.head()
df['id'] = range(1, len(df) + 1)
df['id'] = df['id'].astype(str)
rename_dict = {
    'project_name':'ProjectName',
    'project_url':'ProjectUrl',
    'project_content':'ProjectContent',
    'expertise_name':'ExpertiseName',
    'expertise_url':'ExpertiseUrl',
    'expertise_content':'ExpertiseContent',
    'expertise_contact_person':'ExpertiseContactPerson',
    'expertise_contact_person_designation':'ExpertiseContactDesignation',
    'service_name':'ServiceName',
    'service_content':'ServiceContent',
    'market_name':'MarketName',
    'market_url':'MarketUrl',
    'market_content':'MarketContent',
    'market_contact_person':'MarketContactPerson',
    'market_contact_person_designation':'MarketContactDesignation',
    'solution_name':'SolutionName',
    'solution_url':'SolutionUrl',
    'solution_content':'SolutionContent'
}
df = df.rename(columns=rename_dict)
content_columns = ['ProjectContent','ExpertiseContent','ServiceContent','SolutionContent','MarketContent']
chunk_group = 'ExpertiseName'

result_df, status = main(df=df, columns=content_columns, chunk_group=chunk_group)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = result_df[['ProjectName', 'ProjectUrl', 'ProjectContent',
       'ExpertiseUrl', 'ExpertiseName', 'ExpertiseContent', 'ServiceName',
        'SolutionName', 'SolutionUrl','SolutionContent','MarketName', 'MarketUrl', 'MarketContent',
       'ExpertiseContactPerson', 'ExpertiseContactDesignation',
       'ServiceContent', 'MarketContactPerson', 'MarketContactDesignation',
       'UniqueId', 'ProjectContentChunk', 'ExpertiseContentChunk',
       'ServiceContentChunk', 'SolutionContentChunk', 'MarketContentChunk',
       'ExpertiseContentEmbedding',
       'ServiceContentEmbedding', 'MarketContentEmbedding',
       'ProjectContentEmbedding', 'SolutionContentEmbedding',]]


result_df['row_index'] = range(len(result_df))    
rows = result_df.to_dict(orient='records')
print('Uploading data!')
results = list(upload_website_rows(rows))

# Convert results to DataFrame for display
res_df = pd.DataFrame(results, columns=["start_index", "end_index", "insertion_status"])
print('Data Upload Successful!')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df

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
