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

from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = ArrayType(StructType([
    StructField("itemID", StringType(), True),
    StructField("certificationID", StructType([
        StructField("idValue", StringType(), True)
    ]), True),
    StructField("certificationNameCode", StructType([
        StructField("codeValue", StringType(), True),
        StructField("longName", StringType(), True)
    ]), True),
    StructField("categoryCode", StructType([
        StructField("codeValue", StringType(), True),
        StructField("shortName", StringType(), True)
    ]), True),
    StructField("issuingParty", StructType([
        StructField("nameCode", StructType([
            StructField("codeValue", StringType(), True),
            StructField("shortName", StringType(), True)
        ]), True)
    ]), True),
    StructField("firstIssueDate", StringType(), True),
    StructField("expirationDate", StringType(), True),
    StructField("employerPaidAmount", StructType([
        StructField("amountValue", DoubleType(), True),
        StructField("currencyCode", StructType([
            StructField("codeValue", StringType(), True),
            StructField("shortName", StringType(), True)
        ]), True)
    ]), True),
    StructField("comments", StringType(), True),
    StructField("renewalComments", StringType(), True)
]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Archive/certification_data.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_parsed = df.withColumn("associateCertifications", from_json(col("associateCertifications"), schema))

# Explode the array to create individual rows for each certification
df_exploded = df_parsed.withColumn("certification", explode(col("associateCertifications")))

# Flatten the nested structure
df_flattened = df_exploded.select(
    col("certification.itemID").alias("itemID"),
    col("certification.certificationID.idValue").alias("certificationID"),
    col("certification.certificationNameCode.codeValue").alias("certificationCode"),
    col("certification.certificationNameCode.longName").alias("certificationName"),
    col("certification.categoryCode.codeValue").alias("categoryCode"),
    col("certification.categoryCode.shortName").alias("categoryShortName"),
    col("certification.issuingParty.nameCode.codeValue").alias("issuingPartyCode"),
    col("certification.issuingParty.nameCode.shortName").alias("issuingPartyShortName"),
    col("certification.firstIssueDate").alias("firstIssueDate"),
    col("certification.expirationDate").alias("expirationDate"),
    col("certification.employerPaidAmount.amountValue").alias("employerPaidAmount"),
    col("certification.employerPaidAmount.currencyCode.codeValue").alias("currencyCode"),
    col("certification.employerPaidAmount.currencyCode.shortName").alias("currencyShortName"),
    col("certification.comments").alias("comments"),
    col("certification.renewalComments").alias("renewalComments")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_flattened)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
