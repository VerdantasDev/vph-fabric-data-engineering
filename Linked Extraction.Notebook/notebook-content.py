# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

url = "https://api.linkedin.com/rest/dmaFeedContentsExternal?author=urn%3Ali%3Aorganization%3A80975288&maxPaginationCount=1000&q=postsByAuthor"

payload = {}
access_token = 'AQUv_3yCocXZAdTZysrOx97fiqJMe26r1zGTbEitTg_aCDTbGrDUe0IF42WObbe5jThccYWk_uxpAxhBI-pB1vUzlOLsbJkMRF7mvlbdA0khLJNXZV4gZ1p-y1I69vqmL7PDMN5KJDInrDjLS7wz590_IN1VGGU_A1zworpAGZQULAKw1h1XW8-naQr36IeMor_j9I95nMJJzR3m_TmFjiibHP6xihmnj_trGFg833kGG7I_KYtXTSwN8s9yJikiuCYzjyl9jXB_xGcyJh5_hy9XOUQmK-2JzPqAQm01mp2IdKqwySafpWdPMNm0wIm4PHAZ85K3Tww-lUGUaLm6kAhx3a1k_A'
headers = {
  'Linkedin-Version': '202405',
  'X-Restli-Protocol-Version': '2.0.0',
  'Authorization':f'Bearer {access_token}',
}

print(headers)

response = requests.request("GET", url, headers=headers, data=payload)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

post_list = pd.DataFrame(data['elements'])['id']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

encoded_post_list = [url.replace(':','%3A') for url in post_list]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

posts_data = list()
for post_id in encoded_post_list:
    url = f"https://api.linkedin.com/rest/dmaPosts?ids=List({post_id})"

    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
        data = response.json()
        posts_data.append(data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

encoded_post_list_data = [d['results'] for d in posts_data]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(encoded_post_list_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_nested_dicts_from_list(dict_list):
    nested_dicts = []
    for d in dict_list:
        for v in d.values():
            if isinstance(v, dict):
                nested_dicts.append(v)
    return nested_dicts
 
# Example usage
nested_dicts = extract_nested_dicts_from_list(encoded_post_list_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.DataFrame(nested_dicts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import ast

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
 
def extract_urls_from_json(json_str):
    try:
        # Load the JSON data
        json_str = ast.literal_eval(json_str)
        data = json.loads(json_str)
        # Extract download URLs
        urls = [
            image['media']['image']['downloadUrl']
            for image in data.get('multiImage', {}).get('images', [])
            if isinstance(image, dict) and 'media' in image and 'image' in image['media']
        ]
        return ', '.join(urls)
    except (json.JSONDecodeError, KeyError):
        return None  # or return an empty string or handle the error as needed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_urls_from_dict(d):
    try:
        # Access the download URL from the nested structure
        urls = [
            item['media']['media']['image']['downloadUrl']
            for item in [d]  # Process each dictionary item
            if isinstance(item, dict) and 'media' in item and 'media' in item['media'] and 'image' in item['media']['media']
        ]
        return ', '.join(urls)
    except KeyError:
        return None  # Handle the case where keys are missing

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['download_urls'] = df['content'].apply(extract_urls_from_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

display(df)

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

df = df[['id','publishedAt', 'commentary','download_urls', 'isReshareDisabledByAuthor',  'visibility']]

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

posts_engagement_data = list()
for post_id in encoded_post_list:
    url = f"https://api.linkedin.com/rest/dmaSocialMetadata?ids=List({post_id})"

    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
        data = response.json()
        .append(data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

encoded_post_enagement_list_data = [d['results'] for d in posts_engagement_data]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nested_engagements_dicts = extract_nested_dicts_from_list(encoded_post_enagement_list_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = pd.DataFrame(nested_engagements_dicts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nested_engagements_dicts[167]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_counts_from_json(json_str):
    try:
        # Load the JSON data
        data = json.loads(json_str)
        # Extract the counts, using the reactionType as the key and count as the value
        counts = {reaction['reactionType']: reaction['count'] for reaction in data.values()}
        return counts
    except json.JSONDecodeError:
        return {}
 
# Apply the function to the DataFrame column and convert the result to a DataFrame
counts_df = pd.json_normalize(df1['reactionSummaries'].apply(extract_counts_from_json))
 
# Merge the counts DataFrame with the original DataFrame, if needed
result_df = pd.concat([df1, counts_df], axis=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

def extract_data_from_dict(d):
    try:
        # Extract reaction summaries and create a flat dictionary with reaction counts
        reaction_summaries = d.get('reactionSummaries', {})
        reactions_counts = {reaction['reactionType']: reaction['count'] for reaction in reaction_summaries.values()}
        
        # Extract entity and comment summary count
        result = {
            **reactions_counts,  # Unpack reaction counts
            'entity': d.get('entity'),
            'commentSummary_count': d.get('commentSummary', {}).get('count', None)
        }
        
        return result
    except KeyError:
        return {}  # Return an empty dictionary in case of missing keys

# Apply the function to each dictionary in the list and create a DataFrame
df2 = pd.DataFrame([extract_data_from_dict(d) for d in nested_engagements_dicts])


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

final_df = pd.merge(df,df2,left_on = 'id',right_on = 'entity',how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List of columns to sum
columns_to_sum = ['LIKE', 'PRAISE', 'APPRECIATION', 'EMPATHY', 'INTEREST', 'MAYBE', 'ENTERTAINMENT','commentSummary_count']
 
# Sum the values of the specified columns
final_df['total_engagements'] = final_df[columns_to_sum].fillna(0).astype(float).astype(int).sum(axis= 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.drop(columns = ['entity'],inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df)

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
