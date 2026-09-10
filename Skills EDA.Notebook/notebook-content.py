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

%run ntbk_deltek_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ntbk_deltek_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Common

# CELL ********************

import ast
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mapping_dict = {
    "Digital Technologies": "Precise Visual Technologies / Applied Data & Technology",
    "Water & Wastewater Systems Engineering": "Process Engineering",
    "Sustainability Advisory": "Sustainability"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = dict()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## SharePoint with Skills DB 

# CELL ********************

db_df = pd.read_excel(f"{notebookutils.nbResPath}/builtin/Skills_Database.xlsx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

db_df['Practice'] = db_df['Practice'].str.lower().apply(lambda x:x.replace('&','and'))
db_df['Skill'] = db_df['Skill'].str.lower().apply(lambda x:x.replace('&','and'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

skills_db_unique = db_df[['Practice','Skill']].drop_duplicates()
summary['Total Skills (Mike)'] = len(skills_db_unique)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.read_excel(f"{notebookutils.nbResPath}/builtin/sharepoint_documents_tagged_skills.xlsx")

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

df= df[df['document_category'] == 'Resume']
df['tag_skills'] = df['tags'].apply(lambda d: [item for sublist in ast.literal_eval(d).values() for item in sublist])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Total Resumes'] = len(list(set(df['sharepointURL'])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded = df.explode('tag_skills')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded = spark.createDataFrame(df_exploded)
df_exploded = df_exploded.toPandas()

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

df_exploded['practice_group'] = df_exploded['practice_group'].map(mapping_dict).fillna(df_exploded['practice_group'])
df_exploded = spark.createDataFrame(df_exploded)
df_exploded = df_exploded.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded['practice_group'] = df_exploded['practice_group'].str.lower().apply(lambda x:x.replace('&','and'))
df_exploded['tag_skills'] = df_exploded['tag_skills'].str.lower().apply(lambda x: x.replace('&', 'and') if x is not None else x)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tag_skills_unique = df_exploded[['practice_group','tag_skills']].drop_duplicates()
summary['Total Tags (Sharepoint)'] = len(tag_skills_unique)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_df = pd.merge(df_exploded, db_df, left_on=['practice_group', 'tag_skills'],right_on = ['Practice','Skill'], how='inner')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(merged_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Matches b/w Mike Shared Skills and tag Skills'] = len(merged_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Resume Skills with Skills DB 

# CELL ********************

db_df = pd.read_excel(f"{notebookutils.nbResPath}/builtin/Skills_Database.xlsx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

db_df['Practice'] = db_df['Practice'].str.lower().apply(lambda x:x.replace('&','and'))
db_df['Skill'] = db_df['Skill'].str.lower().apply(lambda x:x.replace('&','and'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.read_excel(f"{notebookutils.nbResPath}/builtin/sharepoint_documents_tagged_skills.xlsx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df= df[df['document_category'] == 'Resume']
df['skills'] = df['skills'].fillna('[]')
df['skills'] = df['skills'].apply(lambda x: ast.literal_eval(x) if x != "[]" else [])
df_exploded = df.explode('skills')
df_exploded['practice_group'] = df_exploded['practice_group'].map(mapping_dict).fillna(df_exploded['practice_group'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded = spark.createDataFrame(df_exploded)
df_exploded = df_exploded.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded['practice_group'] = df_exploded['practice_group'].str.lower().apply(lambda x:x.replace('&','and'))
df_exploded['skills'] = df_exploded['skills'].str.lower().apply(lambda x:x.replace('&','and'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resume_skills_df = df_exploded.copy()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resume_skills_unique = df_exploded[['practice_group','skills']].drop_duplicates()
summary['Total Skills (Sharepoint Resumes)'] = len(resume_skills_unique)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_df = pd.merge(df_exploded, db_df, left_on=['practice_group', 'skills'],right_on = ['Practice','Skill'], how='inner')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Matches b/w Mike Shared Skills and Resume Skills'] = len(merged_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##

# MARKDOWN ********************

# ## Industrial Skills with Skills DB 

# CELL ********************

import pandas as pd
is_df = pd.read_excel(f"{notebookutils.nbResPath}/builtin/industrial skills required.xlsx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(is_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

is_df = is_df[['skill_group_name']].drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

is_df['skill_group_name'] = is_df['skill_group_name'].str.lower().apply(lambda x:x.replace('&','and'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Total Industry skills'] = len(is_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_df = pd.merge(is_df, db_df, left_on=[ 'skill_group_name'],right_on = ['Skill'], how='inner')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(merged_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary['Matches  b/w Mike Shared Skills and Total Industry skills'] = len(merged_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM VPC_Dev_Fablh_data.sv_adp_data_v1 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Aquia Skills with Resume DB 

# CELL ********************

aq_df = load_table(lakehouse_path, 'sv_acquia_resumes')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_df = load_table(lakehouse_path, 'br_adp_workers_data_v1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = aq_df.join(adp_df, on="associateOID", how="left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded['practice_group'] = df_exploded['practice_group'].str.lower().apply(lambda x:x.replace('&','and'))
df_exploded['skills'] = df_exploded['skills'].str.lower().apply(lambda x:x.replace('&','and'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
