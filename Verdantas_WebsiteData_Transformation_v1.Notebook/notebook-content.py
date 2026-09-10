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
pd.set_option('display.max_rows', None)
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables
lakehouse_path = "Tables"

#expertise
table_paths = {"expertise_df": f"{lakehouse_path}/br_verdantas_expertise_data"}
# Read tables into DataFrames
expertise_df = spark.read.format("delta").load(table_paths["expertise_df"])
expertise_df = expertise_df.toPandas()
expertise_df = expertise_df.fillna('other')
expertise_df.columns = expertise_df.columns.str.lower()

#Market
table_paths = {"market_df": f"{lakehouse_path}/br_verdantas_market_data"}
# Read tables into DataFrames
market_df = spark.read.format("delta").load(table_paths["market_df"])
market_df = market_df.toPandas()
market_df = market_df.fillna('other')
market_df.columns = market_df.columns.str.lower()

#project
table_paths = {"project_df": f"{lakehouse_path}/br_verdantas_project_data"}
# Read tables into DataFrames
project_df = spark.read.format("delta").load(table_paths["project_df"])
project_df = project_df.toPandas()
project_df = project_df.fillna('other')
project_df.columns = project_df.columns.str.lower()

#solution
table_paths = {"solution_df": f"{lakehouse_path}/br_verdantas_solution_data"}
# Read tables into DataFrames
solution_df = spark.read.format("delta").load(table_paths["solution_df"])
solution_df = solution_df.toPandas()
solution_df = solution_df.fillna('other')
solution_df.columns = solution_df.columns.str.lower()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Expertise

# CELL ********************

expertise_df['solution'] = expertise_df['solution_name'] + '=' + expertise_df['solution_url']
expertise_df['project'] = expertise_df['project_name'] + '=' + expertise_df['project_url']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def aggregate_no_none(series):
    return list(set(x for x in series if x != 'other=other'))

expertise_grp = expertise_df.groupby(['expertise_url', 'expertise_name', 'expertise_content']).agg({
    'solution': aggregate_no_none,
    'project': aggregate_no_none,
}).reset_index()
expertise_grp['solution'] = expertise_grp['solution'].apply(lambda x:[item for item in x if item != 'other=other'])
expertise_grp['project'] = expertise_grp['project'].apply(lambda x:[item for item in x if item != 'other=other'])
expertise_grp.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise_grp = expertise_grp.explode('project')
expertise_grp = expertise_grp.explode('solution')
expertise_grp.reset_index(drop=True,inplace=True)
expertise_grp.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise_df = pd.merge(expertise_df[['expertise_url','expertise_name','expertise_content','expertise_contact_person','expertise_contact_person_designation','service_name','service_description',]],expertise_grp,how='left',on=['expertise_url', 'expertise_name', 'expertise_content'])
expertise_df.drop_duplicates(keep='first',inplace=True)
expertise_df = expertise_df[expertise_df.service_name != 'other']
expertise_df.reset_index(drop=True,inplace=True)
expertise_df[['solution_name', 'solution_url']] = expertise_df['solution'].apply(lambda x: pd.Series(x.split('=')))
expertise_df[['project_name', 'project_url']] = expertise_df['project'].apply(lambda x: pd.Series(x.split('=')) if pd.notnull(x) else pd.Series([None, None]))
expertise_df.drop(['solution','project'],axis=1,inplace=True)
display(expertise_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Market

# CELL ********************

market_df['market'] = market_df['market_name'] + '=' + market_df['market_url'] + '=' + market_df['market_content'] + '=' + market_df['market_contact_person'] + '=' + market_df['market_contact_person_designation']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def aggregate_no_none(series):
    return list(set(x for x in series if x != 'other=other=other=other=other'))

market_grp = market_df.groupby(['project_name','project_url']).agg({
    'market': aggregate_no_none
}).reset_index()
market_grp['market'] = market_grp['market'].apply(lambda x:[item for item in x if item != 'other=other=other=other=other'])
market_grp.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Expertise-Market

# CELL ********************

expertise_market = pd.merge(expertise_df,market_grp,how='left',on=['project_name','project_url'])
expertise_market = expertise_market.explode('market')
expertise_market.reset_index(drop=True,inplace=True)
expertise_market.market.fillna('other=other=other=other=other',inplace=True)
expertise_market.market.fillna('other',inplace=True)
expertise_market.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expertise_market[['market_name','market_url','market_content','market_contact_person','market_contact_person_designation']] = expertise_market['market'].apply(lambda x: pd.Series(x.split('=')))
expertise_market.drop(['market'],axis=1,inplace=True)
display(expertise_market)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Final

# CELL ********************

project_df = project_df[['project_name','project_content']].drop_duplicates(subset='project_name',keep='first')
print(expertise_market.shape)
final_df = pd.merge(expertise_market,project_df,how='left',on='project_name')
final_df.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

solution_df = solution_df[['solution_name','solution_content']].drop_duplicates(subset='solution_name',keep='first')
print(final_df.shape)
final_df = pd.merge(final_df,solution_df,how='left',on='solution_name')
final_df.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = final_df.fillna('other')
final_df.rename(columns={'service_description':'service_content'},inplace=True)
final_df = final_df[['expertise_url','expertise_name','expertise_content','expertise_contact_person','expertise_contact_person_designation','service_name','service_content','market_name','market_url','market_content','market_contact_person','market_contact_person_designation','project_name','project_url','project_content','solution_name','solution_url','solution_content']]
display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

split_index = 322  # Half of 644

# Split the DataFrame into two parts
df1 = final_df.iloc[:split_index]  # First 322 rows
df2 = final_df.iloc[split_index:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df2)

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
