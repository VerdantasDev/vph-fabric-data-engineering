# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json
import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup

from pyspark.sql.functions import lit

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

    return content

def extract_date(input):

    pattern = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", input)
    date_str = pattern.group(1)

    date_obj = datetime.strptime(date_str, "%B %d, %Y")
    return date_obj

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

def get_drug_screening_data(url, state):

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

    result = {"url": url, "title": title, "update_date": str(extract_date(update_date)), "content": content}

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_marijuana_laws_data(url, state):

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

    result = {"url": url, "title": title, "update_date": str(extract_date(update_date)), "content": content}
    
    return result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_date(input):

    pattern = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", input)
    date_str = pattern.group(1)

    date_obj = datetime.strptime(date_str, "%B %d, %Y")
    return date_obj

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

temp_list = []
result_dict = {
    'NDS' : temp_list
}

today = datetime.today().date()
url = "https://www.nationaldrugscreening.com/us-state-laws/"

response = requests.get(url)
content = response.content

soup = BeautifulSoup(content, "html.parser")
urls = [a["href"] for a in soup.find_all("a", href=True)]

state_list = []

for link in urls:
    if link.startswith(url):
        state_list.append(link)


for state in state_list:

    url = state
    name = url.rsplit('/', 2)[1]

    response = requests.get(url)
    content = response.content

    soup = BeautifulSoup(content, "html.parser")
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
    intro = soup.find(attrs={"data-id": "f45aa9a"}).find(attrs={"data-id": "96542ec"}) if soup.find(attrs={"data-id": "f45aa9a"}) else ''
    if intro != None:
        pre_table_content = get_content(intro)
    else:
        pre_table_content = None

    tbl = soup.find(attrs={"data-id": "e6d9ccf"}).find("table")
    headers = [th.get_text(strip=True) for th in tbl.find_all("th")]

    rows = []
    for tr in tbl.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        rows.append(cells)

    body = soup.find(attrs={"data-id": "f40bd1f"})
    content_2 = get_content(body)

    for link in urls:
        if link.startswith('https://www.nationaldrugscreening.com/states'):
            drug_testing_services = get_drug_screening_data(link, name)
        if link.startswith('https://www.nationaldrugscreening.com/marijuana-considerations/'):
            marijuana_laws = get_marijuana_laws_data(link, name)

    result = {
        "state": name,
        "url": url,
        "drug testing laws" :{
            "title": replace_unicode_characters(title),
            "last_updated_on": update_date,
            "pre_table_content": replace_unicode_characters(pre_table_content),
            "Drug_Testing_Protocols" : [dict(zip(headers, row)) for row in rows],
            "post_table_content": replace_unicode_characters(content_2)
        },
        "drug testing services" : drug_testing_services,
        "marijuana laws" : marijuana_laws
    }

    temp_list.append(result)
    break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
res = json.dumps(result_dict)
print(res)

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
