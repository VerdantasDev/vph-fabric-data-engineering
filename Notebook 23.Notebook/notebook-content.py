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

from pyspark.sql.types import StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load('Tables/br_m365_sharepoint_all_file_status_resume')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

path = '/lakehouse/default/Files/bronze/microsoft365/sharepoint/2024/10/03'
l = os.listdir(path)


l2 = []
for i in l:
    l1 = {}
    path_1 = path + '/' + i 
    val = i.split('||')[0] + '.' + i.split('.')[1]
    l1['bronze_save_path'] = path_1
    l1['filename'] = val
    l2.append(l1)


df3 = spark.createDataFrame(l2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = df.join(df3, on = df.display_name == df3.filename)
df2.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = df2.select(*col, 'bronze_save_path')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = df2.withColumn('to_download', lit('yes'))\
          .withColumn('download_status', lit(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = df2.withColumn('custom_tags', df2.custom_tags.cast(StringType()))

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

df2.write.format('delta').mode('overwrite').save('Tables/br_m365_resumes_spo1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('here')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tyrt = spark.read.load('Tables/br_m365_sharepoint_content_resume_mike')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tyrt.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(tyrt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load('Tables/mvp_resource_data')

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

temp = df.select('EmployeeSkills', 'EmployeePracticeGroupSkills')
temp = temp.filter(temp.EmployeeSkills.isNotNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import array_except, col

result = temp.withColumn("unique_values", array_except(col("EmployeeSkills"), col("EmployeePracticeGroupSkills")))
result = result.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = result.withColumn('lenght_skills_LLM', size(col('EmployeeSkills')))\
               .withColumn('lenght_skills_EmployeePracticeGroupSkills', size(col('EmployeePracticeGroupSkills')))\
               .withColumn('lenght_skills_unique_values', size(col('unique_values')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp2 = df.select('EmployeeSkills', 'EmployeeTaxonomySkills')
temp2.schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, from_json, explode, array, lit, array_distinct, collect_list, array_except
from pyspark.sql.types import MapType, ArrayType, StringType

def find_unique_skills(df):
    # Parse the JSON string in EmployeeTaxonomySkills
    df = df.withColumn("parsed_taxonomy", from_json(col("EmployeeTaxonomySkills"), MapType(StringType(), ArrayType(StringType()))))
    
    # Explode the map to get each category and its skills
    df = df.select("EmployeeSkills", explode("parsed_taxonomy").alias("category", "category_skills"))
    
    # Find unique skills for each category
    df = df.withColumn("unique_skills", array_except(col("EmployeeSkills"), col("category_skills")))
    
    # Collect all unique skills across categories
    result = df.groupBy("EmployeeSkills").agg(
        array_distinct(flatten(collect_list("unique_skills"))).alias("all_unique_skills")
    )
    
    return result

# Apply the function to your DataFrame
result_df = find_unique_skills(temp2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, from_json, explode, array, lit, array_distinct, collect_list, array_except, struct, to_json
from pyspark.sql.types import MapType, ArrayType, StringType

def find_detailed_unique_skills(df):
    # Parse the JSON string in EmployeeTaxonomySkills
    df = df.withColumn("parsed_taxonomy", from_json(col("EmployeeTaxonomySkills"), MapType(StringType(), ArrayType(StringType()))))
    
    # Explode the map to get each category and its skills
    df = df.select("EmployeeSkills", explode("parsed_taxonomy").alias("category", "category_skills"))
    
    # Find unique skills for each category
    df = df.withColumn("unique_skills", array_except(col("EmployeeSkills"), col("category_skills")))
    
    # Collect detailed information
    result = df.groupBy("EmployeeSkills").agg(
        collect_list(
            struct(
                col("category"),
                col("category_skills"),
                col("unique_skills")
            )
        ).alias("taxonomy_details"),
        array_distinct(flatten(collect_list("unique_skills"))).alias("all_unique_skills")
    )
    
    # Convert the collected list to a JSON string for easier access
    result = result.withColumn(
        "taxonomy_json",
        to_json(col("taxonomy_details"))
    )
    
    return result

# Apply the function to your DataFrame
result_df = find_detailed_unique_skills(temp2)

# Show the results
result_df.select("EmployeeSkills", "all_unique_skills", "taxonomy_json")

# To access specific taxonomy details, you'll need to parse the JSON string
# This can be done in a subsequent step or by using a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import json

@udf(returnType=StringType())
def get_category_details(taxonomy_json, category):
    details = json.loads(taxonomy_json)
    for item in details:
        if item['category'] == category:
            return json.dumps(item)
    return None

result_df = result_df.withColumn("Environmental_details", get_category_details(col("taxonomy_json"), lit("Environmental")))
result_df = result_df.withColumn("Engineering_details", get_category_details(col("taxonomy_json"), lit("Engineering")))
result_df = result_df.withColumn("Project_Management_details", get_category_details(col("taxonomy_json"), lit("Project Management")))

result_df = result_df.select("EmployeeSkills", "all_unique_skills", "Environmental_details", "Engineering_details", "Project_Management_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp2.collect()[0]

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
