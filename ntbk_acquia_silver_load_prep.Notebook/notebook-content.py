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

%pip install openai==1.12.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import ast
from openai import AzureOpenAI

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.appName("SkillsExtraction").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_chat_completion(messages,model):
    response = client.chat.completions.create(
        model=model,
        messages=messages
    )
    return response.choices[0].message.content

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def model_sills(text):
    messages = [
        {"role": "system", "content": """Analyze the given content summary and provide consolidated list of skills.             
            Do not provide any other details.
            
            Follow below response format only -
            
            ['geostatic engineer','project management']
            """},
        {"role": "user", "content": text}
    ]

    # Assuming this function gets the completion and works as expected
    response = get_chat_completion(messages, model=azure_openai_chat_deployment)

    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def safe_apply(content):
    try:
        return ast.literal_eval(model_sills(content))
    except Exception as e:
        print(f"Error: {e}")
        try:
            return ast.literal_eval(model_sills(content))
        except Exception as e:
            print(f"Error: {e}")
            return ['issue']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

azure_openai_chat_deployment = 'gpt-4o-dev'

azure_openai_api_version = "2023-03-15-preview"
azure_openai_endpoint = "https://devops-test-openai.openai.azure.com"
azure_openai_key = "b19770cbae334eccbc0ace41b7e23438" if len("c9ce949e276146d4a66b3e16b32613d3") > 0 else None


client = AzureOpenAI(
        api_key = azure_openai_key,  
        api_version=azure_openai_api_version,
        azure_endpoint = azure_openai_endpoint)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.read.load('Tables/br_acquia_content_ADI_v2')
df = spark.read.load('Tables/br_m365_sharepoint_content_acquia')
# df = df.filter(df.collection_name == 'Verdantas Resumes')

df = df.toPandas()
df['extractedskills'] = df['content'].apply(safe_apply)

df_acquia = spark.createDataFrame(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

acquia_cols_list = df_acquia.columns

df_acquia = df_acquia.withColumn('template', trim(regexp_replace(translate(regexp_replace(col("filename"), r"\d+", ""), '_', ' '), r"\s+", " ")))\
                     .withColumn('test', trim(regexp_replace(col('template'), "Resume", "")))\
                     .withColumn('name', split(trim(lower(col("test"))), r"[ ,]+"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_adp_deltek = spark.read.load('Tables/br_employee_deltek_adp_merged')
# br_deltek_EmployeesData
adp_deltek_cols_list = df_adp_deltek.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_adp_deltek = df_adp_deltek.withColumn("FirstName1", regexp_replace(col("FirstName"), r"\.", ""))\
                             .withColumn("MiddleName1", regexp_replace(col("MiddleName"), r"\.", ""))\
                             .withColumn("LastName1", regexp_replace(col("LastName"), r"\.", ""))\
                             .withColumn("EmployeeName1", regexp_replace(col("EmployeeName"), r"\.", ""))

df_adp_deltek = df_adp_deltek.withColumn('temp', split(trim(lower(col('EmployeeName1'))), r"[ ,]+"))
df_adp_deltek = df_adp_deltek.withColumn('temp1', split(trim(lower(concat_ws(', ', col('FirstName1'), col('MiddleName1'), col('LastName1')))), r"[ ,]+"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp = df_adp_deltek.withColumn('final', when(col('temp').isNull(), col('temp1')).otherwise(col('temp')))
df_adp_deltek_final = temp.select(*adp_deltek_cols_list, 'final')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_acquia.join(
                df_adp_deltek_final,
                size(array_intersect(col("name"), col("final"))) >= size(col("name")),
                "inner"
    )

df_acquia_adp_deltek = df_joined.select(*acquia_cols_list, *adp_deltek_cols_list)
merged_cols_list = df_acquia_adp_deltek.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_acquia_adp_deltek = df_acquia_adp_deltek.withColumn('PracticeGroup_1', lower(trim(regexp_replace(regexp_replace("DeltekPracticeGroup", "[,]", ""), "[&]", "and"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_practice_grp = spark.read.load('Tables/br_practicegroup_manual')
practice_grp_cols_list = df_practice_grp.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_practice_grp = df_practice_grp.withColumn('Practice1', lower(trim(regexp_replace(regexp_replace("Practice", "[,]", ""), "[&]", "and"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Join with Practice Group

# CELL ********************

df_final = df_acquia_adp_deltek.join(df_practice_grp, on = [df_acquia_adp_deltek.PracticeGroup_1 == df_practice_grp.Practice1], how = 'left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_final.select(*merged_cols_list, *practice_grp_cols_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # SKILLS MAP

# CELL ********************

# test = df_acquia_adp_deltek.withColumn('rte', explode(df_acquia_adp_deltek.extractedskills))
# test = test.withColumn('rte', lower(col('rte')))

# df_practice_grp = df_practice_grp.withColumn('Skill', lower(df_practice_grp.Skill))
# test2 = test.join(df_practice_grp, on = [df_practice_grp.Practice1 == test.PracticeGroup_1, df_practice_grp.Skill == test.rte])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_test = spark.read.load('Tables/br_deltek_EmployeesData')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_test = spark.read.load('Tables/br_deltek_license_certification')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_test = spark.read.load('Tables/sv_deltek_Employees_merged')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_practice_grp = spark.read.load('Tables/br_practicegroup_manual')
df_practice_grp = df_practice_grp.withColumn('Practice1', lower(trim(regexp_replace(regexp_replace("Practice", "[,]", ""), "[&]", "and"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_practice_grp)

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
