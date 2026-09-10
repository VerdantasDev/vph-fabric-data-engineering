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
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JoinLakehouseTables") \
    .getOrCreate()

# Define paths to your Lakehouse tables

lakehouse_path = "Tables"
table_paths = {
    "sharepoint": f"{lakehouse_path}/br_m365_sharepoint_file_content_ADI"
}
# Read tables into DataFrames
sharepoint = spark.read.format("delta").load(table_paths["sharepoint"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df =sharepoint.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['file_extension'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df[['id', 'filetype', 'filename', 'sharepointURL', 'content','tags']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.describe().T

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['filetype'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.head()['tags'].values[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import pandas as pd
def expand_tags_column(df):
    tags_df = df['tags'].apply(json.loads).apply(pd.Series)
    return pd.concat([df.drop(columns=['tags']), tags_df], axis=1)

# Expand tags column
expanded_df = expand_tags_column(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expanded_df[expanded_dfp]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filetypes = expanded_df['filetype'].unique().tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# Define the filetypes you want to process
filetypes = ['Reports', 'RFPs', 'Proposals']

# Initialize the final structure
final_structure = {'documents': []}

# Loop through each filetype
for filetype in filetypes:
    # Filter the DataFrame for the current filetype
    df_filetype = df[df['filetype'] == filetype]
    first_row = df_filetype.iloc[0]
    common_structure = {
        'id': first_row['id'],
        'file_types': [
            {
                'filetype': filetype,
                'business_domains': [
                    {
                        'domain': 'Environmental',
                        'keywords': first_row['Environmental_keyword'].split(', '),
                        'file_contents': []
                    },
                    {
                        'domain': 'Engineering',
                        'keywords': first_row['Engineering_keyword'].split(', '),
                        'file_contents': []
                    },
                    {
                        'domain': 'Water',
                        'keywords': first_row['Water_keyword'].split(', '),
                        'file_contents': []
                    },
                    {
                        'domain': 'Project_and_Management',
                        'keywords': first_row['Project_and_Management_keyword'].split(', '),
                        'file_contents': []
                    },
                    {
                        'domain': 'O_and_G',
                        'keywords': first_row['O_and_G_keyword'].split(', '),
                        'file_contents': []
                    }
                ]
            }
        ]
    }

    # Map domain names to their corresponding index
    domain_index = {domain['domain']: index for index, domain in enumerate(common_structure['file_types'][0]['business_domains'])}

    # Populate the file_contents for each row
    for _, row in df_filetype.iterrows():
        # Create chunk data
        chunk_data = {
            'chunk_id': row['unique_id'],
            'chunk_content': row['chunk']
        }

        # Find the index for the domain and append chunk data
        for domain in common_structure['file_types'][0]['business_domains']:
            domain_name = domain['domain']
            if domain_name in ['Environmental', 'Engineering', 'Water', 'Project_and_Management', 'O_and_G']:
                common_structure['file_types'][0]['business_domains'][domain_index[domain_name]]['file_contents'].append(chunk_data)

    # Append the common_structure for the current filetype to final_structure
    final_structure['documents'].append(common_structure)

# Convert to JSON string if needed
json_output = json.dumps(final_structure, indent=4)

# Print or save the JSON output
print(json_output)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
