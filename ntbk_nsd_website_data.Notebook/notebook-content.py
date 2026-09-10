# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import re
import json
import requests
import traceback
from datetime import datetime
from bs4 import BeautifulSoup

from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, Row

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

    return content


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_date(input):

    pattern = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", input)
    date_str = pattern.group(1)

    date_obj = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
    return str(date_obj)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

def get_drug_screening_data(url, state, df_schema):

    response = requests.get(url)
    content = response.content

    soup = BeautifulSoup(content, "html.parser")

    title = (
        soup.find(attrs={"data-id": "b54d005"})
        .find("h1", class_="elementor-heading-title elementor-size-default")
        .get_text(strip=True)
    )

    update_date = (
        soup.find(attrs={"data-id": "769eb0e"})
        .find("span", class_="elementor-icon-list-text")
        .get_text(strip=True)
    )

    body = soup.find(attrs={"data-id": "420fc44"})
    content = get_content(body)

    result = [{"state": state, "url": url, "title": title, "update_date": extract_date(update_date), "content": content}]
    temp = spark.createDataFrame(result, schema = df_schema)
    return temp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_marijuana_laws_data(url, state, df_schema):

    response = requests.get(url)
    content = response.content

    soup = BeautifulSoup(content, "html.parser")
    urls = [a["href"] for a in soup.find_all("a", href=True)]

    title = (
        soup.find(attrs={"data-id": "411c56c"})
        .find("h1", class_="elementor-heading-title elementor-size-default")
        .get_text(strip=True)
    )
    update_date = (
        soup.find(attrs={"data-id": "33d8163"})
        .find("span", class_="elementor-icon-list-text")
        .get_text(strip=True)
    )

    body = soup.find(attrs={"data-id": "727d096"})
    content = get_content(body)

    result = [{"state": state, "url": url, "title": title, "update_date": extract_date(update_date), "content": content}]
    temp = spark.createDataFrame(result, schema = df_schema)
    
    return temp


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_request_content(url):

    response = requests.get(url)
    content = response.content

    soup = BeautifulSoup(content, "html.parser")
    return soup

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_spark_df(df):
    
    df = df.filter(df.state != "")
    df = df.withColumn('created_date', lit(today))
    return df

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
    urls = [a["href"] for a in soup.find_all("a", href=True)]

    state_list_url = []

    for link in urls:
        if link.startswith(url):
            state_list_url.append(link)

    # drug_testing_result = [{"state": "", "url": "", "title": "", "update_date": "", "content": "", "drug_testing_protocols": ""}]
    # drug_screening_result = [{"state": "", "url": "", "title": "", "update_date": "", "content": ""}]
    # marijuana_test_result = [{"state": "", "url": "", "title": "", "update_date": "", "content": ""}]
    
    drug_testing_result_schema = StructType([
        StructField("state", StringType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("update_date", StringType(), True),
        StructField("content", StringType(), True),
        StructField("drug_testing_protocols", StructType([
            StructField("Drug Testing Issue", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Comments", StringType(), True)
        ]), True)
    ])

    drug_screening_result_schema = StructType([
        StructField("state", StringType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("update_date", StringType(), True),
        StructField("content", StringType(), True)
    ])

    marijuana_test_result_schema = StructType([
        StructField("state", StringType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("update_date", StringType(), True),
        StructField("content", StringType(), True)
    ])

    drug_testing_df = spark.createDataFrame([], drug_testing_result_schema)
    drug_screening_df = spark.createDataFrame([], drug_screening_result_schema)
    marijuana_test_df = spark.createDataFrame([], marijuana_test_result_schema)

    for url in state_list_url:

        state = url.rsplit('/', 2)[1]
        soup = get_request_content(url)
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
        if intro != None:
            pre_table_content = get_content(intro).strip()
        else:
            pre_table_content = ""

        tbl = soup.find(attrs={"data-id": "e6d9ccf"}).find("table")
        headers = [th.get_text(strip=True) for th in tbl.find_all("th")]

        rows = []
        for tr in tbl.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            rows.append(cells)
        
        temp_tbl_struct = [dict(zip(headers, row)) for row in rows]
        tbl_data = Row(**temp_tbl_struct[0]) if temp_tbl_struct else None

        body = soup.find(attrs={"data-id": "f40bd1f"})
        content_2 = get_content(body).strip()

        if pre_table_content == "":
            content = content_2
        else:
            content = pre_table_content + " " + content_2

        result = [
            {
                "state": state,
                "url": url,
                "title": replace_unicode_characters(title),
                "update_date": extract_date(update_date),
                "content": replace_unicode_characters(content),
                "drug_testing_protocols": tbl_data,
            }
        ]

        temp_drug_testing_df = spark.createDataFrame(result, schema = drug_testing_result_schema)
        drug_testing_df = drug_testing_df.union(temp_drug_testing_df)
        print(f"{state} Drug Laws Processed")

        for link in urls:
            if link.startswith("https://www.nationaldrugscreening.com/states"):
                temp_drug_screening_df = get_drug_screening_data(link, state, drug_screening_result_schema)
                drug_screening_df = drug_screening_df.union(temp_drug_screening_df)
                print(f"{state} Drug Screening laws Processed")
            if link.startswith("https://www.nationaldrugscreening.com/marijuana-considerations/"):
                temp_marijuana_test_df = get_marijuana_laws_data(link, state, marijuana_test_result_schema)
                marijuana_test_df = marijuana_test_df.union(temp_marijuana_test_df)
                print(f"{state} Drug marijuana laws Processed")
    
    drug_testing_df = process_spark_df(drug_testing_df)
    drug_screening_df = process_spark_df(drug_screening_df)
    marijuana_test_df = process_spark_df(marijuana_test_df)

    # drug_testing_df = drug_testing_df.select('state', 'title', 'update_date', 'content', 'url', 'created_date')
    # drug_screening_df = drug_screening_df.select('state', 'title', 'update_date', 'content', 'url', 'created_date')
    # marijuana_test_df = marijuana_test_df.select('state', 'title', 'update_date', 'content', 'url', 'created_date')

except Exception as e:
    print('Error', e)
    print(traceback.format_exc())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(drug_testing_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(drug_screening_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(marijuana_test_df)

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
