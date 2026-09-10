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

%run logging_config


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential


azure_search_endpoint = "https://vpc-dev-aisearch.search.windows.net"
credential = AzureKeyCredential("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") if len("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") > 0 else DefaultAzureCredential()
azure_openai_endpoint = "https://vpc-dev-azureopenai.openai.azure.com/"
azure_openai_key = "c9ce949e276146d4a66b3e16b32613d3" if len("c9ce949e276146d4a66b3e16b32613d3") > 0 else None
azure_openai_chat_deployment = 'gpt-4o'
azure_openai_api_version = "2022-12-01"
embedding_model_name = "text-embedding-ada-002"
search_api_key = "8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u"
search_openai_credential = AzureKeyCredential("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") if len("8uVsHQYzHm1dKvsuCg2lALw5afwVB29mJtHup2pjnZAzSeCkfm8u") > 0 else DefaultAzureCredential()
EMBEDDING_LENGTH = 1536
# change var
index_name = 'continuous_indexing_fabric'
data_name = 'website'
data_table = 'sv_verdantas_website_data'
index_api_version = '2023-11-01'
client = AzureOpenAI(
  api_key = azure_openai_key,  
  api_version = azure_openai_api_version,
  azure_endpoint =azure_openai_endpoint
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import requests
import logging
# from logging_config import setup_logging
setup_logging()
# from dotenv import load_dotenv
# load_dotenv(override=True) # take environment variables from .env.

# from config import (
#     index_api_version,
#     azure_search_endpoint,
#     EMBEDDING_LENGTH,
#     search_api_key,
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def website_schema(azure_search_endpoint:str,index_name:str,index_api_version) -> None:

    # Create index for AI Search with fields id, content, and contentVector
    url = f"{azure_search_endpoint}/indexes/{index_name}?api-version={index_api_version}"
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
        logging.info('Index schema created!')
        status = 'successful'
    elif response.status_code == 204:
        status = 'successful'
        logging.info('Index schema updated!')
    else:
        status = 'un-successful'
        logging.warning(f'Index schema failed with HTTP request status code {response.status_code}')
        raise Exception(f"HTTP response body: {response.text}")
    
    return status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def document_schema(azure_search_endpoint:str,index_name:str) -> None:

    # Create index for AI Search with fields id, content, and contentVector    
    url = f"{azure_search_endpoint}/indexes/{index_name}?api-version={index_api_version}"
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
        logging.info('Index schema created!')
        status = 'successful'
    elif response.status_code == 204:
        status = 'successful'
        logging.info('Index schema updated!')
    else:
        status = 'un-successful'
        logging.warning(f'Index schema failed with HTTP request status code {response.status_code}')
        raise Exception(f"HTTP response body: {response.text}")
    
    return status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def adpdeltek_schema(azure_search_endpoint:str,index_name:str) -> None:

    # Create index for AI Search with fields id, content, and contentVector    
    url = f"{azure_search_endpoint}/indexes/{index_name}?api-version={index_api_version}"
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
                "name": "ClientId",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True, 
                "retrievable": True,               
                "sortable": True,
                "facetable": True                
            },
            {
                "name": "ClientFolderId",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True               
            },            
            {
                "name": "ClientName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True
            },
            {
                "name": "Worked",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True 

            },            
            {
                "name": "ClientAddressCity",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True
            },
            {
                "name": "ClientAddressState",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True 

            }, 
            
            {
            "name": "ProjectId",
            "type": "Edm.String",
            "key": False,
            "filterable": True,
            "searchable": True,
            "retrievable": True,
            "sortable": False,
            "facetable": True 

            },
            {
                "name": "ProjectName",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "LaborBudget",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable": False,
                "facetable": False 

            },
            {
                "name": "LaborRemaining",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable": False,
                "facetable": False 

            },            
            {
                "name": "ProjectStatus",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "ProjectAddressStreet",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "ProjectAddressCity",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "ProjectAddressState",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "ProjectType",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "ProjectPracticeGroup",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "ProjectBusinessUnit",
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
                        "name": "EmployeePosition",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 

                    },                    
                    {
                        "name": "EmployeePracticeGroupDeltek",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 
                    },                   
                
                    {
                        "name": "EmployeeBusinessUnitDeltek",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 

                    },
                    {
                        "name": "EmployeeDepartmentAdp",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 

                    },
                    {
                        "name": "BillingLaborCategoryDeltek",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": False,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": False 

                    },
                    # {
                    #     "name": "Skills",
                    #     "type": "Edm.String",
                    #     "key": False,
                    #     "filterable": True,
                    #     "searchable": True,
                    #     "retrievable": True,
                    #     "sortable": False,
                    #     "facetable": True 
                    # },
                    {
                        "name": "CertificationCodeAdp",
                        "type": "Edm.String",
                        "key": False,
                        "filterable": True,
                        "searchable": True,
                        "retrievable": True,
                        "sortable": False,
                        "facetable": True 

                    },                   
                    
                    {
                    "name": "NameOfCertification",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    
                    {
                    "name": "EmployeeStatus",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "LicenseCertificationDescription",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "LicenseCertificationState",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "LicenseCertificationId",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "ResumeUrl",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "ProjectManager",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "ProjectManagerId",
                    "type": "Edm.Int64",
                    "key": False,
                    "filterable": True,
                    "searchable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "ProjectRevenueToDate",
                    "type": "Edm.Int64",
                    "key": False,
                    "filterable": True,
                    "searchable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "ProjectBilledToDate",
                    "type": "Edm.Int64",
                    "key": False,
                    "filterable": True,
                    "searchable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "UnbilledToDate",
                    "type": "Edm.Double",
                    "key": False,
                    "filterable": True,
                    "searchable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "ProjectTotalRevenueBudget",
                    "type": "Edm.Double",
                    "key": False,
                    "filterable": True,
                    "searchable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "ProjectPracticeServiceGroup",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "ProjectServiceGroup",
                    "type": "Edm.String",
                    "key": False,
                    "filterable": True,
                    "searchable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "EmployeeJobCostRate",
                    "type": "Edm.Double",
                    "key": False,
                    "filterable": True,
                    "searchable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "EmployeeUtilization",
                    "type": "Edm.Double",
                    "key": False,
                    "filterable": True,
                    "searchable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
                    },
                    {
                    "name": "TargetUtilization",
                    "type": "Edm.Int64",
                    "key": False,
                    "filterable": True,
                    "searchable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": True
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
                        "prioritizedContentFields": [],
                        "prioritizedKeywordsFields": [
                        {"fieldName":"Id"},                        
                        {"fieldName": "ClientId"},
                        {"fieldName": "ClientFolderId"},
                        {"fieldName": "ClientName"},
                        {"fieldName": "Worked"},
                        {"fieldName": "ClientAddressCity"},
                        {"fieldName": "ClientAddressState"},
                        {"fieldName": 'ProjectId'},                        
                        {"fieldName": "ProjectName"},                       
                        {"fieldName": "ProjectStatus"},
                        {"fieldName": "ProjectAddressStreet"},
                        {"fieldName": "ProjectAddressCity"},
                        {"fieldName": "ProjectAddressState"},
                        {"fieldName": "ProjectType"},
                        {"fieldName": "ProjectPracticeGroup"},
                        {"fieldName": "ProjectBusinessUnit"},
                        {"fieldName": "EmployeeId"},
                        {"fieldName": "EmployeeName"},
                        {"fieldName": "EmployeePosition"},                        
                        {"fieldName": "EmployeePracticeGroupDeltek"},
                        {"fieldName": "EmployeeBusinessUnitDeltek"},
                        {"fieldName": "EmployeeDepartmentAdp"},
                        {"fieldName": "BillingLaborCategoryDeltek"},
                        # {"fieldName": "Skills"},
                        {"fieldName": "CertificationCodeAdp"},
                        {"fieldName": "NameOfCertification"},
                        {"fieldName": "EmployeeStatus"},
                        {"fieldName": "LicenseCertificationDescription"},
                        {"fieldName": "LicenseCertificationState"},
                        {"fieldName": "LicenseCertificationId"},
                        {"fieldName": "ResumeUrl"},
                        {"fieldName": "ProjectManager"},                        
                        {"fieldName": "ProjectPracticeServiceGroup"},
                        {"fieldName": "ProjectServiceGroup"},                        
                        
                        ]

                    }
                }
            ]
        }
    }
)
    headers = {"Content-Type": "application/json", "api-key": search_api_key}

    response = requests.request("PUT", url, headers=headers, data=payload)
    if response.status_code == 201:
        logging.info('Index schema created!')
        status = 'successful'
    elif response.status_code == 204:
        status = 'successful'
        logging.info('Index schema updated!')
    else:
        status = 'un-successful'
        logging.warning(f'Index schema failed with HTTP request status code {response.status_code}')
        raise Exception(f"HTTP response body: {response.text}")
    
    return status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def linkedin_schema(azure_search_endpoint:str,index_name:str) -> None:

    # Create index for AI Search with fields id, content, and contentVector    
    url = f"{azure_search_endpoint}/indexes/{index_name}?api-version={index_api_version}"
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
                "name": "PostId",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True, 
                "retrievable": True,               
                "sortable": True,
                "facetable": True                
            },
            {
                "name": "PostType",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True               
            },            
            {
                "name": "PostContent",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True
            },
            {
                "name": "ImageUrl",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True 

            },            
            {
                "name": "IsReshareDisabledByAuthor",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True
            },
            {
                "name": "VisibilityType",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": True,
                "facetable": True 

            }, 
            
            {
            "name": "Like",
            "type": "Edm.Int64",
            "key": False,
            "filterable": True,
            "searchable": True,
            "retrievable": True,
            "sortable": False,
            "facetable": True 

            },
            {
                "name": "Praise",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "Appreciation",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable": False,
                "facetable": False 

            },
            {
                "name": "Empathy",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": False,
                "retrievable": True,
                "sortable": False,
                "facetable": False 

            },            
            {
                "name": "Interest",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "Maybe",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "Entertainment",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "Comment",
                "type": "Edm.String",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

            },
            {
                "name": "TotalEngagement",
                "type": "Edm.Int64",
                "key": False,
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "sortable": False,
                "facetable": True 

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
                        "prioritizedContentFields": [],
                        "prioritizedKeywordsFields": [
                        {"fieldName":"PostId"},                        
                        {"fieldName": "PostType"},
                        {"fieldName": "PostContent"},
                        {"fieldName": "ImageUrl"},
                        {"fieldName": "IsReshareDisabledByAuthor"},
                        {"fieldName": "VisibilityType"},                                                
                        
                        ]

                    }
                }
            ]
        }
    }
)
    headers = {"Content-Type": "application/json", "api-key": search_api_key}

    response = requests.request("PUT", url, headers=headers, data=payload)
    if response.status_code == 201:
        logging.info('Index schema created!')
        status = 'successful'
    elif response.status_code == 204:
        status = 'successful'
        logging.info('Index schema updated!')
    else:
        status = 'un-successful'
        logging.warning(f'Index schema failed with HTTP request status code {response.status_code}')
        raise Exception(f"HTTP response body: {response.text}")
    
    return status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def index_schema_main(data_name,index_name):
    if data_name == 'website':
        website_schema(azure_search_endpoint,index_name,index_api_version)
    elif data_name == 'document':
        document_schema(azure_search_endpoint,index_name)
    elif data_name == 'linkedin':
        linkedin_schema(azure_search_endpoint,index_name)
    else:
        adpdeltek_schema(azure_search_endpoint,index_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
