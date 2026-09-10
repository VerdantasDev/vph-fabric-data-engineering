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

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import numpy as np 
import pandas as pd
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"
table_paths = {"expertise_df": f"{lakehouse_path}/br_verdantas_expertise_data"}
# Read tables into DataFrames
expertise_df = spark.read.format("delta").load(table_paths["expertise_df"])
expertise_df = expertise_df.toPandas()
expertise_df = expertise_df.fillna('other')

#Market
table_paths = {"market_df": f"{lakehouse_path}/br_verdantas_market_data"}
# Read tables into DataFrames
market_df = spark.read.format("delta").load(table_paths["market_df"])
market_df = market_df.toPandas()
market_df = market_df.fillna('other')

#project
table_paths = {"project_df": f"{lakehouse_path}/br_verdantas_project_data"}
# Read tables into DataFrames
project_df = spark.read.format("delta").load(table_paths["project_df"])
project_df = project_df.toPandas()
project_df = project_df.fillna('other')

#solution
table_paths = {"solution_df": f"{lakehouse_path}/br_verdantas_solution_data"}
# Read tables into DataFrames
solution_df = spark.read.format("delta").load(table_paths["solution_df"])
solution_df = solution_df.toPandas()
solution_df = solution_df.fillna('other')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

market_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise_content_df = expertise_df[['expertise_url','expertise_content']]
expertise_content_df = expertise_content_df.dropna(subset = 'expertise_content').reset_index(drop=True)
market_content_df = market_df[['market_url','market_content']]
market_content_df = market_content_df.dropna(subset = 'market_content').reset_index(drop=True)
project_content_df = project_df[['project_url','project_content']]
project_content_df = project_content_df.dropna(subset = 'project_content').reset_index(drop=True)
solution_content_df = solution_df[['solution_url','solution_content']]
solution_content_df = solution_content_df.dropna(subset = 'solution_url').reset_index(drop=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# expertise_df = expertise_df[expertise_df.expertise_name.isin(['Architecture','Automation Engineering','Digital Technologies'])]
# expertise_df.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise_df['Solution'] = expertise_df['Solution_name'] + '=' + expertise_df['']
expertise_df['Project'] = expertise_df['Project_name'] + '=' + expertise_df['Project_url']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def aggregate_no_none(series):
    return list(set(x for x in series if x != 'other=other'))

expertise_df1 = expertise_df.groupby(['expertise_url', 'expertise_name', 'expertise_content']).agg({
    'Solution': aggregate_no_none,
    'Project': aggregate_no_none,
}).reset_index()
expertise_df1['Solution'] = expertise_df1['Solution'].apply(lambda x:[item for item in x if item != 'other=other'])
expertise_df1.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise_df1 = expertise_df1.explode('Project')
expertise_df1 = expertise_df1.explode('Solution')
expertise_df1.reset_index(drop=True,inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise_df1.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.merge(expertise_df[['expertise_url','expertise_name','expertise_content','Service_name','Service_description']],expertise_df1,how='left',on=['expertise_url', 'expertise_name', 'expertise_content'])
df.drop_duplicates(keep='first',inplace=True)
df = df[df.Service_name != 'other']
df.reset_index(drop=True,inplace=True)
df[['Solution_name', 'Solution_url']] = df['Solution'].apply(lambda x: pd.Series(x.split('=')))
df[['Project_name', 'Project_url']] = df['Project'].apply(lambda x: pd.Series(x.split('=')) if pd.notnull(x) else pd.Series([None, None]))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**Market**</mark>

# CELL ********************

market_df.rename(columns={'Expertise_name':'expertise_name','Expertise_url':'expertise_url'},inplace=True)
market_df.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

market_df['expertise'] = market_df['expertise_name'] + '=' + market_df['expertise_url']
market_df['Project'] = market_df['Project_name'] + '=' + market_df['Project_url']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def aggregate_no_none(series):
    return list(set(x for x in series if x != 'other=other'))

market_df1 = market_df.groupby(['market_url', 'market_name', 'market_content']).agg({
    'expertise': aggregate_no_none,
    'Project': aggregate_no_none,
}).reset_index()
# market_df1['Project'] = market_df1['Project'].apply(lambda x:[item for item in x if item != 'other=other'])
market_df1.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

market_df1 = market_df1.explode('Project')
market_df1 = market_df1.explode('expertise')
market_df1.reset_index(drop=True,inplace=True)
market_df1[['expertise_name', 'expertise_url']] = market_df1['expertise'].apply(lambda x: pd.Series(x.split('=')) if pd.notnull(x) else pd.Series([None, None]))
market_df1[['Project_name', 'Project_url']] = market_df1['Project'].apply(lambda x: pd.Series(x.split('=')) if pd.notnull(x) else pd.Series([None, None]))
market_df1.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# market_df2 = market_df1[market_df1.expertise_name.isin(['Architecture','Automation Engineering','Digital Technologies'])]
# display(market_df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def aggregate_no_none(series):
    return list(set(x for x in series if x is not None))
market_df1['market'] = market_df1['market_name'] + '=' + market_df1['market_url'] + '=' + market_df1['market_content']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

market = market_df1.groupby(['expertise_name','Project_name'], dropna=False).agg({'market':list}).reset_index()
# market.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise = df.copy()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise = pd.merge(expertise,market,how='left',on=['expertise_name','Project_name'])
expertise.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mapping = expertise.dropna(subset=['Project_name']).set_index('Project_name')['market']
mapping = mapping.dropna()
mapping_df = pd.DataFrame(mapping).reset_index()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(mapping_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mapping_df.drop_duplicates(subset='Project_name',inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(mapping_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# mapping of each unique project with respective list of uniaque markets and then replacing the empty market fields
expertise =expertise.drop(columns =['market'])
expertise = expertise.merge(mapping_df,on = 'Project_name',how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(expertise)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded = expertise.explode('market')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_split = df_exploded['market'].str.split('=', n=2, expand=True)
df_split.columns = ['market_name', 'market_url', 'market_content']
df_exploded = df_exploded.join(df_split)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded = df_exploded.reset_index(drop=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded = df_exploded.merge(project_content_df,left_on = 'Project_url',right_on = 'project_url',how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded = df_exploded.merge(solution_content_df,left_on = 'Solution_url',right_on = 'solution_url',how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded = df_exploded[['Project','Project_name', 'Project_url','project_content','expertise_url', 'expertise_name', 
                           'expertise_content','Service_name','Service_description', 'Solution','Solution_name',
       'Solution_url','solution_content','market', 'market_name',
       'market_url', 'market_content' ]]

df_exploded.columns = ['project','project_name', 'project_url','project_content','expertise_url', 'expertise_name', 
                           'expertise_content','service_name','service_description', 'solution','solution_name',
       'solution_url','solution_content','market', 'market_name',
       'market_url', 'market_content' ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_exploded = df_exploded.merge(market_content_df,left_on = 'market_url',right_on = 'market_url',how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df = spark.DataFrame(df_exploded)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_exploded)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise_df.head(2)

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
