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

import re
import json
import requests
import traceback
from datetime import datetime
from bs4 import BeautifulSoup

from delta.tables import *
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, Row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_request_content(url):

    try:
        response = requests.get(url)
        response.raise_for_status()
        content = response.content

        soup = BeautifulSoup(content, "html.parser")
        return soup
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None  # Return None to indicate failure


def extract_date(input):

    pattern = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", input)
    date_str = pattern.group(1)

    date_obj = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
    return str(date_obj)


def replace_unicode_characters(input_string):
    # Replace all unicode characters with an empty string or a placeholder
    if input_string is not None:
        return input_string.encode('ascii', 'ignore').decode('ascii')
    else:
        return ""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_dataframe_schema():

    drug_testing_schema = StructType([
        StructField("state", StringType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("update_date", StringType(), True),
        StructField("content", StringType(), True),
        StructField("drug_testing_protocols", StructType([
            StructField("Drug_Testing_Issue", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Comments", StringType(), True)
        ]), True)
    ])

    drug_screening_schema = StructType([
        StructField("state", StringType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("update_date", StringType(), True),
        StructField("content", StringType(), True)
    ])

    marijuana_test_schema = StructType([
        StructField("state", StringType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("update_date", StringType(), True),
        StructField("content", StringType(), True)
    ])

    return drug_testing_schema, drug_screening_schema, marijuana_test_schema


def process_spark_df(df, today):
    
    df = df.filter(df.state != "")
    df = df.withColumn('created_date', lit(today))

    if 'drug_testing_protocols' in df.columns:
        df = df.select('state', 'title', 'update_date', 'content', 'url', 'drug_testing_protocols', 'created_date')
    else:
        df = df.select('state', 'title', 'update_date', 'content', 'url', 'created_date')

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_list_items(ul):
    items = []
    for li in ul.find_all("li", recursive=False):
        item = li.get_text(separator=" ", strip=True)
        nested_ul = li.find("ul")
        if nested_ul:
            item += "\n" + "\n".join(extract_list_items(nested_ul))
        items.append(item)
    return items


def get_content(soup):

    content = """
    """

    for element in soup.find_all(["h2", "p", "ul", "ol"]):
        if element.name == "h2":
            current_heading = element.get_text(strip=True)
            content += current_heading
        elif element.name == "p":
            content = content + " " + element.get_text(strip=True)
        elif element.name == "ul" or element.name == "ol":
            # Extract list items
            list_items = extract_list_items(element)
            content = content + " " + " ".join(list_items)

    content = content.strip()
    content = replace_unicode_characters(content)

    return content


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_subpage_data(soup, title_data_id, date_data_id, body_data_id):

    title = (
        soup.find(attrs={"data-id": title_data_id})
        .find("h1", class_="elementor-heading-title elementor-size-default")
        .get_text(strip=True)
    )

    update_date = (
        soup.find(attrs={"data-id": date_data_id})
        .find("span", class_="elementor-icon-list-text")
        .get_text(strip=True)
    )

    update_date = extract_date(update_date)

    body = soup.find(attrs={"data-id": body_data_id})
    content = get_content(body)

    result = {"title": title, "update_date": update_date, "content": content}
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_drug_screening_data(url, state, df_schema):
    
    soup = get_request_content(url)
    if soup is None:
        raise Exception("no response from client")
    
    result = get_subpage_data(soup, "b54d005", "769eb0e", "420fc44")
    result["state"] = state
    result["url"] = url

    temp = spark.createDataFrame([result], schema = df_schema)
    return temp


def get_marijuana_laws_data(url, state, df_schema):

    soup = get_request_content(url)
    if soup is None:
        raise Exception("no response from client")

    result = get_subpage_data(soup, "411c56c", "33d8163", "727d096")
    result["state"] = state
    result["url"] = url

    temp = spark.createDataFrame([result], schema = df_schema)
    return temp


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_sink(temp, delta_table_path, merge_keys):

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        
        tgt_table = DeltaTable.forPath(spark, delta_table_path)

        merge_condition = " and ".join(
            [f" target.{col} = updates.{col} " for col in merge_keys]
        )

        tgt_table.alias('target').merge(
            source=temp.alias("updates"), condition=merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    else:
        temp.write.format("delta").mode("append").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def main():
try:
    today = datetime.today().date()
    url = "https://www.nationaldrugscreening.com/us-state-laws/"

    soup = get_request_content(url)
    if soup is None:
        raise Exception("no response from client")

    links = [a["href"] for a in soup.find_all("a", href=True)]

    state_url_list = [link for link in links if link.startswith(url)]

    drug_testing_schema, drug_screening_schema, marijuana_test_schema = get_dataframe_schema()

    drug_testing_df = spark.createDataFrame([], drug_testing_schema)
    drug_screening_df = spark.createDataFrame([], drug_screening_schema)
    marijuana_test_df = spark.createDataFrame([], marijuana_test_schema)

    drug_testing_tbl_path = "Tables/br_nds_drug_testing"
    drug_screening_tbl_path = "Tables/br_nds_drug_screening"
    marijuana_test_tbl_path = "Tables/br_nds_marijuana_laws"
    merge_keys = ['state', 'update_date']

    error_log = []

    for url in state_url_list:
        try:

            state = url.rsplit('/', 2)[1]
            soup = get_request_content(url)
            if soup is None:
                raise Exception("no response from client")
            
            urls = [a["href"] for a in soup.find_all("a", href=True)]

            title = (
                soup.find(attrs={"data-id": "b78977b"})
                .find("h1", class_="elementor-heading-title elementor-size-default")
                .get_text(strip=True)
            )
            update_date = (
                soup.find(attrs={"data-id": "ef2de92"})
                .find("span", class_="elementor-icon-list-text")
                .get_text(strip=True)
            )
            intro = (
                soup.find(attrs={"data-id": "f45aa9a"}).find(attrs={"data-id": "96542ec"})
                if soup.find(attrs={"data-id": "f45aa9a"})
                else ""
            )

            tbl = soup.find(attrs={"data-id": "e6d9ccf"}).find("table")
            headers = [th.get_text(strip=True) for th in tbl.find_all("th")]
            headers = [header.replace(' ', '_') for header in headers]

            rows = []
            for tr in tbl.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                rows.append(cells)

            body = soup.find(attrs={"data-id": "f40bd1f"})
            post_table_content = get_content(body)

            if intro != None:
                pre_table_content = get_content(intro)
                content = pre_table_content + " " + post_table_content
            else:
                content = post_table_content

            temp_tbl_structure = [dict(zip(headers, row)) for row in rows]
            drug_testing_protocols = Row(**temp_tbl_structure[0]) if temp_tbl_structure else None

            result = [
                {
                    "state": state,
                    "url": url,
                    "title": replace_unicode_characters(title),
                    "update_date": extract_date(update_date),
                    "content": content,
                    "drug_testing_protocols": drug_testing_protocols,
                }
            ]

            temp_drug_testing_df = spark.createDataFrame(result, schema = drug_testing_schema)
            drug_testing_df = drug_testing_df.union(temp_drug_testing_df)
            process_count = 1

            for link in urls:
                if link.startswith("https://www.nationaldrugscreening.com/states"):
                    temp_drug_screening_df = get_drug_screening_data(link, state, drug_screening_schema)
                    drug_screening_df = drug_screening_df.union(temp_drug_screening_df)
                    process_count += 1
                if link.startswith("https://www.nationaldrugscreening.com/marijuana-considerations/"):
                    temp_marijuana_test_df = get_marijuana_laws_data(link, state, marijuana_test_schema)
                    marijuana_test_df = marijuana_test_df.union(temp_marijuana_test_df)
                    process_count += 1

            if process_count == 3:
                print(f"{state} Processed")
    
        except Exception as e:
            error_log.append(state)
            print('Error', e)
            print(traceback.format_exc())

    drug_testing_df = process_spark_df(drug_testing_df, today)
    drug_screening_df = process_spark_df(drug_screening_df, today)
    marijuana_test_df = process_spark_df(marijuana_test_df, today)

    write_to_sink(drug_testing_df, drug_testing_tbl_path, merge_keys)
    write_to_sink(drug_screening_df, drug_screening_tbl_path, merge_keys)
    write_to_sink(marijuana_test_df, marijuana_test_tbl_path, merge_keys)

except Exception as e:
    print('Error', e)
    print(traceback.format_exc())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(error_log)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
