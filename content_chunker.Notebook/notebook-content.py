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

%pip install --upgrade pip
%pip install openai==1.12.0
%pip install langchain==0.0.354

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd
import json
from openai import AzureOpenAI
import asyncio
from typing import List, Dict
import logging
# from logging_config import setup_logging
setup_logging()
# from dotenv import load_dotenv
# load_dotenv(override=True) # take environment variables from .env.
# from config import (
# embedding_model_name,
# data_name,
# client)

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

### Data preprocessing & Chunking
class Document:
    def __init__(self, page_content: str, metadata: Dict = None):
        self.page_content = page_content
        self.metadata = metadata or {}

# Adjust the split_text_using_splitter function
def split_intoChunks(text: str, chunk_size=600, chunk_overlap=10, separators_1=None) -> List[str]:
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def chunking(df, columns,chunk_group):
    if len(columns)>0:
        logging.info('content chunking!')
        for column in columns:
            newCol = column+'Chunk'    
            df[newCol] = df[column].apply(lambda x: split_intoChunks(x))

        for column in columns:
            newCol = column+'Chunk'
            df = df.explode(newCol).reset_index(drop=True)

        df['ChunkIndex'] = df.groupby(chunk_group).cumcount()

        # Add a unique identifier for each chunk
        df['UniqueId'] = df['UniqueId'] + "_" + df['ChunkIndex'].astype(str)

        df = df.fillna("NA")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

async def get_embedding(text: str):
    result = client.embeddings.create(
    input = text,
    model= embedding_model_name)
    # return result['data'][0]['embedding']
    return json.loads(result.model_dump_json(indent=2))['data'][0]['embedding']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

async def create_embeddings(df,embdCol,conCol):
    logging.info('Create embedding for the given input')
    df[embdCol] = await asyncio.gather(*[get_embedding(text) for text in df[conCol]])    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_prefix_if_underscore(value):
    if value.startswith('_'):
        return 'A' + value
    return value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def content_chunk_main(df,columns,chunk_group):
    df = chunking(df,columns,chunk_group)
    if data_name == 'website':
        df = chunking(df,columns,chunk_group)
        rename = {
        'SolutionContent_chunk':'SolutionContentChunk',
        'ProjectContent_chunk':'ProjectContentChunk',
        'MarketContent_chunk':'MarketContentChunk',
        'ServiceContent_chunk':'ServiceContentChunk',
        'ExpertiseContent_chunk':'ExpertiseContentChunk'
            }
        df = df.rename(columns = rename)
        # unique contents
        uniqueExpertise = df[['ExpertiseName','ExpertiseContent','ExpertiseContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)
        unique_service = df[['ServiceName','ServiceContent','ServiceContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)
        unique_market = df[['MarketName','MarketContent','MarketContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)
        unique_project = df[['ProjectName','ProjectContent','ProjectContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)
        unique_solution = df[['SolutionName','SolutionContent','SolutionContentChunk']].drop_duplicates(keep='first').reset_index(drop=True)

        unique_project = asyncio.run(create_embeddings(unique_project,'ProjectContentEmbedding','ProjectContentChunk'))
        uniqueExpertise = asyncio.run(create_embeddings(uniqueExpertise,'ExpertiseContentEmbedding','ExpertiseContentChunk'))
        unique_service = asyncio.run(create_embeddings(unique_service,'ServiceContentEmbedding','ServiceContentChunk'))
        unique_market = asyncio.run(create_embeddings(unique_market,'MarketContentEmbedding','MarketContentChunk'))
        unique_solution = asyncio.run(create_embeddings(unique_solution,'SolutionContentEmbedding','SolutionContentChunk'))

        df = pd.merge(df,uniqueExpertise[['ExpertiseName','ExpertiseContent','ExpertiseContentChunk','ExpertiseContentEmbedding']],how='left',on=['ExpertiseName','ExpertiseContent','ExpertiseContentChunk'])
        df = pd.merge(df,unique_service[['ServiceName','ServiceContent','ServiceContentChunk','ServiceContentEmbedding']],how='left',on=['ServiceName','ServiceContent','ServiceContentChunk'])
        df = pd.merge(df,unique_market[['MarketName','MarketContent','MarketContentChunk','MarketContentEmbedding']],how='left',on=['MarketName','MarketContent','MarketContentChunk'])
        df = pd.merge(df,unique_project[['ProjectName','ProjectContent','ProjectContentChunk','ProjectContentEmbedding']],how='left',on=['ProjectName','ProjectContent','ProjectContentChunk'])
        df = pd.merge(df,unique_solution[['SolutionName','SolutionContent','SolutionContentChunk','SolutionContentEmbedding']],how='left',on=['SolutionName','SolutionContent','SolutionContentChunk'])

        df["UniqueId"] = df["unique_id"].apply(add_prefix_if_underscore)
        logging.warning(f"Id's are unique - {df.shape[0]==df.UniqueId.nunique()}")   

    elif data_name == 'document':
        df = asyncio.run(create_embeddings(df,'ContentChunkEmbedding','ContentChunk'))
        df["UniqueId"] = df["UniqueId"].apply(add_prefix_if_underscore)
        logging.warning(f"Id's are unique - {df.shape[0]==df.UniqueId.nunique()}")         

    return df,'successful'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
