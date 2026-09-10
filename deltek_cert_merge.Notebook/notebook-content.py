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

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install us

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_path = "Tables"
table_paths = {
    "ADP": f"{lakehouse_path}/sv_adp_data_v2",
    "Deltek": f"{lakehouse_path}/br_deltek_DemoData_v1",
    "ELC": f"{lakehouse_path}/br_deltek_license_certification"
   # 'worker': f"{lakehouse_path}/br_deltek_workers_data_v1"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read tables into DataFrames
df_ADP = spark.read.format("delta").load(table_paths["ADP"])
df_Deltek = spark.read.format("delta").load(table_paths["Deltek"])
elc_df = spark.read.format("delta").load(table_paths["ELC"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# remove leading space from license certificate description
elc_df = elc_df.withColumn("LicenseCertificationDescription", F.trim(elc_df["LicenseCertificationDescription"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# remove leading space from license certificate state
elc_df = elc_df.withColumn("LicenseCertificationState", 
                           F.when(F.col("LicenseCertificationState").isNotNull() & (F.trim(F.col("LicenseCertificationState")) != ''), 
                                  F.trim(F.col("LicenseCertificationState")))
                           .otherwise(F.col("LicenseCertificationState")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_lic = df_ADP.select('EmploymentStatus','EmployeeNumber','licenseName').dropna().dropDuplicates()
# df_Cert = df_ADP.select('EmploymentStatus','EmployeeNumber','NameOfCertification').dropna().dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_lic_null = df_lic.filter(df_lic.licenseName.isNull())
# df_Cert_null = df_Cert.filter(df_Cert.NameOfCertification.isNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import functions as F
# from pyspark.sql.types import StringType

# # Mapping of state abbreviations to full names
# state_mapping = {
#     "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
#     "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
#     "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
#     "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts",
#     "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana",
#     "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
#     "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
#     "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
#     "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
#     "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"
# }

# # Function to get full state name
# def get_full_state_name(abbreviation):
#     return state_mapping.get(abbreviation, None)

# # Register the function as a UDF
# get_full_state_name_udf = F.udf(get_full_state_name, StringType())

# # Split the column and add full state name if the state is a US state
# df_ADP = df_ADP.withColumn("StatePart", F.split(df_ADP["LicenseCertificationDescription"], "-").getItem(1)) \
#                .withColumn("CertificationState", 
#                            F.when(F.col("StatePart").isin(list(state_mapping.keys())), 
#                                   get_full_state_name_udf(F.col("StatePart")))
#                            .when(F.col("StatePart").isin(list(state_mapping.values())), 
#                                  F.col("StatePart"))
#                            .otherwise(F.lit(None))) \
#                .withColumn("NameOfCertification", 
#                            F.when(F.col("StatePart").isin(list(state_mapping.keys())), 
#                                   F.trim(F.split(df_ADP["LicenseCertificationDescription"], "-").getItem(0)))
#                            .when(F.col("StatePart").isin(list(state_mapping.values())), 
#                                  F.trim(F.split(df_ADP["LicenseCertificationDescription"], "-").getItem(0)))
#                            .otherwise(F.lit(None))) \
#                .drop("StatePart")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

us_states  = [
    "AL", "Alabama",
    "AK", "Alaska",
    "AZ", "Arizona",
    "AR", "Arkansas",
    "CA", "California",
    "CO", "Colorado",
    "CT", "Connecticut",
    "DE", "Delaware",
    "FL", "Florida",
    "GA", "Georgia",
    "HI", "Hawaii",
    "ID", "Idaho",
    "IL", "Illinois",
    "IN", "Indiana",
    "IA", "Iowa",
    "KS", "Kansas",
    "KY", "Kentucky",
    "LA", "Louisiana",
    "ME", "Maine",
    "MD", "Maryland",
    "MA", "Massachusetts",
    "MI", "Michigan",
    "MN", "Minnesota",
    "MS", "Mississippi",
    "MO", "Missouri",
    "MT", "Montana",
    "NE", "Nebraska",
    "NV", "Nevada",
    "NH", "New Hampshire",
    "NJ", "New Jersey",
    "NM", "New Mexico",
    "NY", "New York",
    "NC", "North Carolina",
    "ND", "North Dakota",
    "OH", "Ohio",
    "OK", "Oklahoma",
    "OR", "Oregon",
    "PA", "Pennsylvania",
    "RI", "Rhode Island",
    "SC", "South Carolina",
    "SD", "South Dakota",
    "TN", "Tennessee",
    "TX", "Texas",
    "UT", "Utah",
    "VT", "Vermont",
    "VA", "Virginia",
    "WA", "Washington",
    "WV", "West Virginia",
    "WI", "Wisconsin",
    "WY", "Wyoming"
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to check if the state is a US state
def is_us_state(state):
    return state in us_states

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Register the function as a UDF
is_us_state_udf = F.udf(is_us_state)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import BooleanType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Split the column into NameOfCertification and StatePart
df_ADP = df_ADP.withColumn("NameOfCertification", F.split(F.col("LicenseCertificationDescription"), "-").getItem(0)) \
               .withColumn("StatePart", F.split(F.col("LicenseCertificationDescription"), "-").getItem(1))

# Check if StatePart is a valid US state
df_ADP = df_ADP.withColumn("CertificationState", 
                           F.when(F.col("StatePart").isNotNull() & is_us_state_udf(F.col("StatePart")), 
                                  F.trim(F.col("StatePart")))
                           .otherwise(F.lit(None))) \
               .withColumn("NameOfCertification", 
                           F.when(F.col("StatePart").isNotNull() & is_us_state_udf(F.col("StatePart")), 
                                  F.trim(F.col("NameOfCertification")))
                           .otherwise(F.col("LicenseCertificationDescription"))) \
               .drop("StatePart")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Register the function as a UDF
is_us_state_udf = F.udf(is_us_state, BooleanType())

# Split the column only if the state is a US state and handle null values
df_ADP = df_ADP.withColumn("StatePart", F.split(F.col("LicenseCertificationDescription"), "-").getItem(1)) \
               .withColumn("CertificationState", 
                           F.when(F.col("LicenseCertificationDescription").isNotNull() & 
                                  is_us_state_udf(F.col("StatePart")), 
                                  F.trim(F.col("StatePart")))
                           .otherwise(F.lit(None))) \
               .withColumn("NameOfCertification", 
                           F.when(F.col("LicenseCertificationDescription").isNotNull() & 
                                  is_us_state_udf(F.col("StatePart")), 
                                  F.trim(F.split(F.col("LicenseCertificationDescription"), "-").getItem(0)))
                           .otherwise(F.lit(None))) \
               .drop("StatePart")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_ADP = df_ADP.withColumn("CertificationState", F.trim(F.split(df_ADP["LicenseCertificationDescription"], "-").getItem(1))) \
#                  .withColumn("NameOfCertification", F.trim(F.split(df_ADP["LicenseCertificationDescription"], "-").getItem(0)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_ADP)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_column_names = [col + "_ADP" for col in df_ADP.columns]
df_renamed = df_ADP.toDF(*new_column_names)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_renamed = df_renamed.fillna("")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

elc_df = elc_df.fillna("")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# adp_elc_df = elc_df.join(
#     df_renamed,
#     (elc_df['LicenseCertificationDescription'] == df_renamed['NameOfCertification_ADP']) & (elc_df['EmployeeID'] == df_renamed['EmployeeNumber_ADP']) & (elc_df['LicenseCertificationState'] == df_renamed['CertificationState_ADP']),
#     how="outer"
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Perform the join on three columns
joined_df = elc_df.join(df_renamed, 
                     (elc_df['LicenseCertificationDescription'] == df_renamed['NameOfCertification_ADP']) & 
                     (elc_df['EmployeeID'] == df_renamed['EmployeeNumber_ADP']) & 
                     (elc_df['LicenseCertificationState'] == df_renamed['CertificationState_ADP']), 
                     'outer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df = joined_df.withColumn('is_match', F.when(F.col('NameOfCertification_ADP').isNotNull(), 1).otherwise(0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fallback_joined_df = elc_df.join(df_renamed, 
                              (elc_df['LicenseCertificationDescription'] == df_renamed['NameOfCertification_ADP']) & 
                              (elc_df['EmployeeID'] == df_renamed['EmployeeNumber_ADP']),
                              'left')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = joined_df.withColumn('LicenseCertificationDescription', F.when(F.col('is_match') == False, fallback_joined_df['LicenseCertificationDescription']).otherwise(joined_df['LicenseCertificationDescription'])) \
                    .withColumn('EmployeeID', F.when(F.col('is_match') == False, fallback_joined_df['EmployeeID']).otherwise(joined_df['EmployeeID'])) \
                    .withColumn('LicenseCertificationState', F.when(F.col('is_match') == False, fallback_joined_df['LicenseCertificationState']).otherwise(joined_df['LicenseCertificationState']))


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

joined_df = joined_df.withColumn('second_match', 
                                 F.when((F.col('first_match') == 0) & F.col('df2.col1').isNotNull(), 1).otherwise(0))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_elc_df_inner = elc_df.join(
    df_renamed,
    (elc_df['LicenseCertificationDescription'] == df_renamed['NameOfCertification_ADP']) & (elc_df['EmployeeID'] == df_renamed['EmployeeNumber_ADP']) & (elc_df['LicenseCertificationState'] == df_renamed['CertificationState_ADP']),
    how="left"
)
adp_elc_df_inner.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_elc_df_inner = elc_df.join(
    df_renamed,
    (elc_df['LicenseCertificationDescription'] == df_renamed['NameOfCertification_ADP']) & (elc_df['EmployeeID'] == df_renamed['EmployeeNumber_ADP']) & (elc_df['LicenseCertificationState'] == df_renamed['CertificationState_ADP']),
    how="inner"
)
adp_elc_df_inner.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# adp_elc_df = adp_elc_df.join(
#     df_lic,
#     (adp_elc_df['LicenseCertificationDescription'] == df_lic['licenseName']) & (adp_elc_df['EmployeeID'] == df_lic['EmployeeNumber']),
#     how="outer"
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adp_elc_df = adp_elc_df.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(adp_elc_df)

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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
