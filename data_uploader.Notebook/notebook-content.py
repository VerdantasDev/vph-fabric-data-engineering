# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run logging_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import re
import pandas as pd
import requests
import logging
# from logging_config import setup_logging
setup_logging()
# from dotenv import load_dotenv
# load_dotenv(override=True) # take environment variables from .env.
# from config import (
# search_api_key,
# azure_search_endpoint,
# index_name,
# index_api_version)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def insert_into_index(documents):
    """Uploads a list of 'documents' to Azure AI Search index."""
    url = f"{azure_search_endpoint}/indexes/{index_name}?api-version={index_api_version}"
    
    payload = json.dumps({"value": documents})
    headers = {
        "Content-Type": "application/json",
        "api-key": search_api_key,
    }
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200 or response.status_code == 201:
        return "Success"
    else:
        return f"Failure: {response.text}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def make_safe_id(row_id: str):
    """Strips disallowed characters from row id for use as Azure AI search document ID."""
    return re.sub("[^0-9a-zA-Z_-]", "_", row_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upload_website_rows(rows):
    """Uploads the rows in a dataframe to Azure AI Search.
    Limits uploads to 1000 rows at a time due to Azure AI Search API limits.
    """
    BATCH_SIZE = 700
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:
            documents.append(
                {
                    "Id": make_safe_id(row["UniqueId"]),
                    "ProjectName": row["ProjectName"],
                    "ProjectUrl": row["ProjectUrl"],
                    "ProjectContent": row["ProjectContentChunk"],
                    "ExpertiseName": row["ExpertiseName"],
                    "ExpertiseUrl": row["ExpertiseUrl"],
                    "ExpertiseContent": row["ExpertiseContentChunk"],
                    "ExpertiseContactPerson": row["ExpertiseContactPerson"],
                    "ExpertiseContactDesignation": row["ExpertiseContactDesignation"],
                    "ServiceName": row["ServiceName"],
                    "ServiceContent": row["ServiceContentChunk"],
                    "MarketName": row["MarketName"],
                    "MarketUrl": row["MarketUrl"],
                    "MarketContent": row["MarketContentChunk"],
                    "MarketContactPerson": row["MarketContactPerson"],
                    "MarketContactDesignation": row["MarketContactDesignation"],
                    "SolutionName": row["SolutionName"],   
                    "SolutionUrl": row["SolutionUrl"],   
                    "SolutionContent": row["SolutionContentChunk"],          
                    "ProjectContentEmbedding": row["ProjectContentEmbedding"],
                    "ExpertiseContentEmbedding": row["ExpertiseContentEmbedding"],
                    "ServiceContentEmbedding": row["ServiceContentEmbedding"],
                    "MarketContentEmbedding": row["MarketContentEmbedding"],
                    "SolutionContentEmbedding": row["SolutionContentEmbedding"],
                    "@search.action": "upload",
                }
            )
        status = insert_into_index(documents) #search_client.merge_or_upload_documents(documents)
        logging.info([row_batch[0]["row_index"], row_batch[-1]["row_index"], status])
        yield [row_batch[0]["row_index"], row_batch[-1]["row_index"], status]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upload_document_rows(rows):
    """Uploads the rows in a dataframe to Azure AI Search.
    Limits uploads to 1000 rows at a time due to Azure AI Search API limits.
    """
    BATCH_SIZE = 300
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:
            documents.append(
                {
                    "Id": make_safe_id(row["UniqueId"]),
                    "DocumentCategory": row["DocumentCategory"],
                    "PracticeGroupName": row["PracticeGroupName"],
                    "ClientId": row["ClientId"],
                    "ClientName": row["ClientName"],
                    "ClientFolderId": row["ClientFolderId"],
                    "Worked": row["Worked"],
                    "ClientAddressCity": row["ClientAddressCity"],
                    "ClientAddressState": row["ClientAddressState"],
                    "EmployeeName": row["EmployeeName"],
                    "EmployeeId": row["EmployeeId"],
                    "FileName": row["FileName"],
                    "SharepointUrl": row["SharepointUrl"],
                    "TagsAssociated": row["TagsAssociated"],
                    # "content_chunk_id": row["content_chunk_id"],
                    "ContentChunk": row["ContentChunk"],
                    "ContentChunkEmbeddings": row["ContentChunkEmbeddings"],                    
                    "@search.action": "upload",
                }
            )
        status = insert_into_index(documents)
        logging.info([row_batch[0]["row_index"], row_batch[-1]["row_index"], status])
        yield [row_batch[0]["row_index"], row_batch[-1]["row_index"], status]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upload_adpdeltek_rows(rows):
    """Uploads the rows in a dataframe to Azure AI Search.
    Limits uploads to 1000 rows at a time due to Azure AI Search API limits.
    """
    BATCH_SIZE = 300
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:
            documents.append({
            "Id": make_safe_id(row["UniqueId"]),
            "ClientId": row["ClientId"],
            "ClientFolderId": row["ClientFolderId"],
            "ClientName": row["ClientName"],
            "Worked": row["Worked"],
            "ClientAddressCity": row["ClientAddressCity"],
            "ClientAddressState": row["ClientAddressState"],
            "ProjectId": row["ProjectId"],
            "ProjectName": row["ProjectName"],
            "LaborBudget": row["LaborBudget"],
            "LaborRemaining": row["LaborRemaining"],
            "ProjectStatus": row["ProjectStatus"],
            "ProjectAddressStreet": row["ProjectAddressStreet"],
            "ProjectAddressCity": row["ProjectAddressCity"],
            "ProjectAddressState": row["ProjectAddressState"],
            "ProjectType": row["ProjectType"],
            "ProjectPracticeGroup": row["ProjectPracticeGroup"],
            "ProjectBusinessUnit": row["ProjectBusinessUnit"],
            "EmployeeId": row["EmployeeId"],
            "EmployeeName": row["EmployeeName"],
            "EmployeePosition": row["EmployeePosition"],
            "EmployeePracticeGroupDeltek": row["EmployeePracticeGroupDeltek"],
            "EmployeeBusinessUnitDeltek": row["EmployeeBusinessUnitDeltek"],
            "EmployeeDepartmentAdp": row["EmployeeDepartmentAdp"],
            "BillingLaborCategoryDeltek": row["BillingLaborCategoryDeltek"],
            # "Skills": row["Skills"],
            "CertificationCodeAdp": row["CertificationCodeAdp"],
            "NameOfCertification": row["NameOfCertification"],
            "EmployeeStatus": row["EmployeeStatus"],
            "LicenseCertificationDescription": row["LicenseCertificationDescription"],
            "LicenseCertificationState": row["LicenseCertificationState"],
            "LicenseCertificationId": row["LicenseCertificationId"],
            "ResumeUrl": row["ResumeUrl"],
            "ProjectManager": row["ProjectManager"],
            "ProjectManagerId": row["ProjectManagerId"],
            "ProjectRevenueToDate": row["ProjectRevenueToDate"],
            "ProjectBilledToDate": row["ProjectBilledToDate"],
            "UnbilledToDate": row["UnbilledToDate"],
            "ProjectTotalRevenueBudget": row["ProjectTotalRevenueBudget"],
            "ProjectPracticeServiceGroup": row["ProjectPracticeServiceGroup"],
            "ProjectServiceGroup": row["ProjectServiceGroup"],
            "EmployeeJobCostRate": row["EmployeeJobCostRate"],
            "EmployeeUtilization": row["EmployeeUtilization"],
            "TargetUtilization": row["TargetUtilization"],
            "@search.action": "upload",
            }
    )
        status = insert_into_index(documents)
        logging.info([row_batch[0]["row_index"], row_batch[-1]["row_index"], status])
        yield [row_batch[0]["row_index"], row_batch[-1]["row_index"], status]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upload_linkedin_rows(rows):
    """Uploads the rows in a dataframe to Azure AI Search.
    Limits uploads to 1000 rows at a time due to Azure AI Search API limits.
    """
    BATCH_SIZE = 300
    for i in range(0, len(rows), BATCH_SIZE):
        row_batch = rows[i: i + BATCH_SIZE]
        documents = []
        for row in row_batch:
            documents.append(
                {
                    "Id": make_safe_id(row["UniqueId"]),
                    "PostId": row["PostId"],
                    "PostType": row["PostType"],
                    "PostContent": row["PostContent"],
                    "ImageUrl": row["ImageUrl"],
                    "IsReshareDisabledByAuthor": row["IsReshareDisabledByAuthor"],
                    "VisibilityType": row["VisibilityType"],
                    "Like": row["Like"],
                    "Praise": row["Praise"],
                    "Appreciation": row["Appreciation"],
                    "Empathy": row["Empathy"],
                    "Interest": row["Interest"],
                    "Maybe": row["Maybe"],
                    "Entertainment": row["Entertainment"],
                    "Comment": row["Comment"],
                    "TotalEngagement": row["TotalEngagement"],                    
                    "@search.action": "upload",
                }
            )
        status = insert_into_index(documents)
        logging.info([row_batch[0]["row_index"], row_batch[-1]["row_index"], status])
        yield [row_batch[0]["row_index"], row_batch[-1]["row_index"], status]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def data_uploader_main(df,data_name):
    # Run upload_batch on partitions of the dataframe
    if data_name == 'website':
        df['row_index'] = range(len(df))    
        rows = df.to_dict(orient='records')
        logging.info('Uploading data!')
        results = list(upload_website_rows(rows))

        # Convert results to DataFrame for display
        res_df = pd.DataFrame(results, columns=["start_index", "end_index", "insertion_status"])
        logging.info(f'Data Upload Successful!')
        return 'successful'

    elif data_name == 'document':
        df['row_index'] = range(len(df))    
        rows = df.to_dict(orient='records')
        logging.info('Uploading data!')
        results = list(upload_document_rows(rows))

        # Convert results to DataFrame for display
        res_df = pd.DataFrame(results, columns=["start_index", "end_index", "insertion_status"])
        logging.info(f'Data Upload Successful!')
        return 'successful'

    elif data_name == 'linkedin':
        df['row_index'] = range(len(df))    
        rows = df.to_dict(orient='records')
        logging.info('Uploading data!')
        results = list(upload_linkedin_rows(rows))

        # Convert results to DataFrame for display
        res_df = pd.DataFrame(results, columns=["start_index", "end_index", "insertion_status"])
        logging.info(f'Data Upload Successful!')
        return 'successful'

    else: # json file
        logging.info('Uploading data!')
        results = insert_into_index(df)

        logging.info(f'Data Upload Successful!')
        return 'successful'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
