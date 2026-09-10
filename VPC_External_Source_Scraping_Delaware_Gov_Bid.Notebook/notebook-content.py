# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7f83389f-7ed0-448f-9472-29df96e82584",
# META       "default_lakehouse_name": "dev_lkh_vrdtcp",
# META       "default_lakehouse_workspace_id": "2da14148-7603-4e6e-8eb8-88c5924d132a"
# META     }
# META   }
# META }

# CELL ********************

!pip install selenium

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import re
from dateutil.parser import parse


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# lambda creds

#Getting app secret keys from key vault
lambda_test_username = mssparkutils.credentials.getSecret('https://dev-kv-vrdtcp.vault.azure.net/','dev-url-dataext-username')
lambda_test_access_key = mssparkutils.credentials.getSecret('https://dev-kv-vrdtcp.vault.azure.net/','dev-urldataext-secret')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Initialize the WebDriver
options = Options() 
driver = webdriver.Remote(
    command_executor=f'https://{lambda_test_username}:{lambda_test_access_key}@hub.lambdatest.com/wd/hub',
    options=options
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = 'https://mmp.delaware.gov/Bids'

driver.get(url)

# Let the page load
driver.implicitly_wait(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# while True:
#     main_element = driver.find_element(By.ID, "gbox_jqGridBids")

#     element_html = main_element.get_attribute('outerHTML')

#     soup = BeautifulSoup(element_html, 'html.parser')

#     links_path = soup.find_all('a')

#     pattern = r'title="([^"]*)"'
#     final_list = list()
#     for link in links_path:    
#         # Using re.search to find the pattern match
#         text = link.text
#         if text.strip() != '' and text.strip() !='×':
#             print(text)
#             match = re.search(pattern, str(link))
#             if match:
                
#                 link_title = match.group(1).replace('&amp;','&')
#                 link = driver.find_element(By.XPATH,f"""//a[@title="{link_title}"]""")
#                 # Click the link
#                 link.click()
#                 dic = dict()
#                 time.sleep(5)
#                 element = driver.find_element(By.ID, "dynamicDialogInnerHtml")

#                 element_html = element.get_attribute('outerHTML')
#                 soup = BeautifulSoup(element_html, 'html.parser')

#                 ls = element.text.split('\n')
#                 index = ls.index("Bid Status Details for")
#                 dic['contractTitle'] = ls[index+1]
#                 dic['contractNumber'] = ls[index+2]
#                 index = ls.index('Solicitation Ad Date')
#                 dic['openDate'] = parse(ls[index+1],fuzzy=True).strftime('%Y-%m-%d')
#                 index = ls.index('Deadline for Bid Responses')
#                 dic['closeDate'] = parse(ls[index+1],fuzzy=True).strftime('%Y-%m-%d')  
#                 for link in soup.find_all('a', title=lambda title: title and re.search(r'\bRFP\b', title)):
#                     dic['rfp_document_url'] = link.get('href')  # Extract the URL from the href attribute
#                     print("Found URL:",  dic['rfp_document_url'])
                    
#                 final_list.append(dic)
#             # time.sleep(5)
#                 button = WebDriverWait(driver, 10).until(
#                             EC.element_to_be_clickable((By.CSS_SELECTOR, 'input.btn.btn-primary[value="Close"]'))
#                                     )
#                 button.click()
#     next_page = driver.find_element(By.XPATH,"""//*[@title="Next Page"]""") 
#     if next_page.is_enabled():
#         next_page.click()
               

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_html_element(driver, element_id):
    """Extracts the HTML content of an element by its ID."""
    element = driver.find_element(By.ID, element_id)
    return element.get_attribute('outerHTML')

def parse_html(html_content):
    """Parses HTML content and returns a BeautifulSoup object."""
    return BeautifulSoup(html_content, 'html.parser')

def extract_links(soup):
    """Extracts and filters links from the BeautifulSoup object."""
    return [link for link in soup.find_all('a') if link.text.strip() and link.text.strip() != '×']

def process_link(driver, link):
    """Processes a single link, extracts information, and clicks the link."""
    pattern = r'title="([^"]*)"'
    text = link.text
    print(text)
    match = re.search(pattern, str(link))
    if match:
        doc_list = list()
        link_title = match.group(1).replace('&amp;','&')
        link_element = driver.find_element(By.XPATH, f"""//a[@title="{link_title}"]""")
        link_element.click()
        time.sleep(5)
        
        # Extract information from the dialog
        dialog_html = extract_html_element(driver, "dynamicDialogInnerHtml")
        dialog_soup = parse_html(dialog_html)
        details = dialog_soup.text.split('\n')
        
        # Extract specific details
        dic = {}
        try:
            index = details.index("Bid Status Details for")
            dic['contractTitle'] = details[index + 1]
            dic['contractNumber'] = details[index + 2]
        except ValueError:
            pass
        
        try:
            index = details.index('Solicitation Ad Date')
            dic['openDate'] = parse(details[index + 1], fuzzy=True).strftime('%Y-%m-%d')
        except ValueError:
            dic['openDate'] = None
        
        try:
            index = details.index('Deadline for Bid Responses')
            dic['closeDate'] = parse(details[index + 1], fuzzy=True).strftime('%Y-%m-%d')
        except ValueError:
            dic['closeDate'] = None

        # Check for "Load More" button and click it if present
        try:
            load_more_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@title="Load more Documents"]'))
            )
            
            load_more_button.click()
            print("Clicked 'Load More' button.")
        except:
            print("'Load More' button not found or not clickable.")

            # Retrieve the links again after loading more
        links = driver.find_elements(By.XPATH, '//div[@id="bidDocuments"]//table//tr//td//a')
           # print("Retrieved all links after clicking 'Load More'.")
        for link in links:
            link_dic = dict()
            link_dic.update(dic)
            link_dic['document_name'] = link.text
            link_dic['document_url'] = link.get_attribute('href')
           # print(f"Link Text: {link.text}")
            doc_list.append(link_dic)
        # Close the dialog
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'input.btn.btn-primary[value="Close"]'))
        )
        button.click()
        
        return doc_list
    return None

def click_next_page(driver):
    """Attempts to click the 'Next Page' button and returns True if successful, False otherwise."""
    try:
        next_page = driver.find_element(By.XPATH, '//*[@title="Next Page"]')
        if 'disabled' not in next_page.get_attribute('class'):
            next_page.click()
            return True
        else:
            return False
    except Exception as e:
        print(f"Next page button click failed: {e}")
    return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main(driver):
    final_list = []
    final_df = pd.DataFrame()
    while True:
        # Extract and parse the main element
        bid_status_list = ['Open Bids','Recently Closed Bids','Not Awared Bids']
        for bsl in bid_status_list:
            bsl_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH,  f"""//*[@title="{bsl}"]"""))
            )
            
            bsl_button.click()
            main_html = extract_html_element(driver, "gbox_jqGridBids")
            main_soup = parse_html(main_html)
            
            # Extract and process links
            links = extract_links(main_soup)
            for link in links:
                doc_list = process_link(driver, link)
                if doc_list:
                    final_list.extend(doc_list)
            
            # Move to the next page
            if not click_next_page(driver):
                df = pd.DataFrame(final_list)
                df['bid_status'] = bsl
                final_df = pd.concat([final_df,df])
            

    return final_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_list = main(driver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.DataFrame(final_list)
df['download_status'] = 0
df['bronze_rfp_document_save_path'] = ''

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

spark_df = spark.createDataFrame(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df.write.option("overwriteSchema", "true").mode("overwrite").format("delta").saveAsTable("br_external_delware_gov")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
