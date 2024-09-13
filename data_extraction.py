"""
Following code contains functions to collect content from the BBC Website Specifically related to Medical Strikes
"""
import os
import re
import time
import json
import spacy
import shutil
import logging
import traceback
import concurrent.futures
from bs4 import BeautifulSoup
from selenium import webdriver
from spacy.matcher import Matcher
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException


def make_dir_if_not_exists(directory_name:str=''):
    if not directory_name.strip():
        return None
    main_directory = os.getcwd()
    directory_path = os.path.join(main_directory, directory_name)
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    return directory_path


log_directory_name = "Logs"
log_directory_path = make_dir_if_not_exists(directory_name=log_directory_name)
# Set up logging
log_file_path = os.path.join(log_directory_path, 'data-extractor.log')

# Configure the logging module
logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the format for log messages
    handlers=[
        logging.FileHandler(log_file_path),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

# Create a logger
logger = logging.getLogger(__name__)
logging.getLogger('WDM').setLevel(logging.WARNING)

# Set the logging level for the Selenium library to CRITICAL to suppress its logs
selenium_logger = logging.getLogger('selenium')
selenium_logger.setLevel(logging.CRITICAL)

# Set the logging level for the urllib3 library to CRITICAL to suppress its logs
urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.CRITICAL)

extracted_data = {} # using this globally as it will be used in multi-threading  

# BBC URL related to Strike search

URL = fr"https://www.bbc.co.uk/search?q=healthcare+strike&seqId=829cd2e0-66ee-11ef-9592-770fb43e53e0&d=NEWS_PS&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTcyNTAzNjQyNiwibmJmIjogMTcyNTAzNjA2NiwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEaGVhbHRoY2FyZSUyQnN0cmlrZSUyNnNlcUlkJTNEODI5Y2QyZTAtNjZlZS0xMWVmLTk1OTItNzcwZmI0M2U1M2UwJTI2ZCUzRE5FV1NfUFMifQ.PmZi3_ojwKt7EsiZAwIADDzt2zsUZQDyd2sG2kRAw1Q"

def create_driver():
    # Clear the webdriver-manager cache to force download of the correct version
    cache_dir = ChromeDriverManager().install().split('drivers')[0] + 'drivers'
    shutil.rmtree(cache_dir, ignore_errors=True)

    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument('--disable-gpu')  # Disable GPU acceleration
    options.add_argument('--no-sandbox')  # Bypass OS security model
    options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems

    # Use ChromeDriverManager to get the path to the WebDriver
    service = Service(ChromeDriverManager().install())

    # Create the WebDriver instance
    driver = webdriver.Chrome(service=service, options=options)
    return driver

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def fetch_content(link):
    """
    Function to fetch the content of a given link.
    """
    logger.info(f"Fetching content from -> {link}")
    driver = create_driver()
    try:
        driver.get(link)
        time.sleep(15)
        link_page_source = driver.page_source
        link_soup = BeautifulSoup(link_page_source, 'html.parser')
        content = '\n'.join([p.get_text() for p in link_soup.find_all('p')])
        logger.info(f"Completed extracting article for: {link}")
        return link, content
    except Exception as e:
        logger.error(f"Error fetching content for {link}: {str(e)}")
        return link, None
    finally:
        driver.quit()


def get_links_content(links: list = None):
    """
    Function to get the content of all the provided links concurrently.
    """
    site_content = {}
    try:
        # with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        #     logger.info(f"Fetching the content links")
        #     future_to_link = {executor.submit(fetch_content, link): link for link in links}
        #     for future in concurrent.futures.as_completed(future_to_link):
        #         link, content = future.result()
        for link in links:
            content = fetch_content(link)
            if content:
                site_content[link] = content
    except Exception as e:
        logger.error(f"Exception occured -> {str(e)}")

    return site_content


def get_article_links() -> list:
    """
    Function will go to the URL mentioned globally
    Will get all the article links mentioned on all pages of the pagination
    Returns:
        list: contains the article links
    """
    article_links = []
    try:
        # logger.info("Article finding process started")
        driver = webdriver.Firefox()
        driver.get(URL)

        while True:
            # Get the page source after it's fully loaded
            page_source = driver.page_source
            # logger.info("Article finding process completed")

            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(page_source, 'html.parser')

            # Find all <li> elements with the specified structure
            articles = soup.select('li:has(div[data-testid="default-promo"])')
            # logger.info(f"Extracted {len(articles)} articles")

            for article in articles:
                link_tag = article.find('a', class_='ssrcss-its5xf-PromoLink exn3ah91')
                # print(f"{article} : {link_tag}")
                if link_tag:
                    link = link_tag.get('href')
                    article_links.append(link)
                    # logger.info(f"Found article link: {link}")

            # Check if there is a next page button and click it
            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="main-content"]/div[5]/div/div/nav/div/div/div[4]/div/a'))
                )
                # logger.info("Found next button, clicking it")
                next_button.click()
            except StaleElementReferenceException:
                logger.warning("StaleElementReferenceException encountered, retrying...")
                continue  # Retry the loop to find the next button again
            except TimeoutException:
                # logger.info("No more pages found or error encountered while clicking next button")
                break

        driver.quit()
    except Exception as e:
        logger.error(f"Exception encountered -> {str(e)}")
        logger.error(traceback.format_exc())
        driver.quit()
    
    return article_links

def get_content_information(content:str='')->dict:
    """
    function will take in the content extracted from the article and feed it to a NLP, which in return will extract the relevant data and share it in format of a dictionary

    Args:
        content (str, optional): Content extracted from article. Defaults to ''.

    Returns:
        dict: Extracted required information
    """

    # loading the spacy english model
    nlp = spacy.load("en_core_web_sm")

    # Process the text with SpaCy
    doc = nlp(content)
    
    # Initialize the matcher with the shared vocabulary
    matcher = Matcher(nlp.vocab)
    
    # Define patterns for matching
    duration_pattern = [{"IS_DIGIT": True}, {"LOWER": "hour"}]
    group_pattern = [{"POS": "NOUN", "OP": "+"}, {"IS_PUNCT": True, "OP": "?"}, {"LOWER": "and", "OP": "?"}, {"POS": "NOUN", "OP": "?"}]
    cause_pattern = [{"LOWER": "for"}, {"POS": "ADJ", "OP": "+"}, {"POS": "NOUN", "OP": "+"}]
    location_pattern = [{"LOWER": "in"}, {"POS": "PROPN", "OP": "+"}]
    people_type_pattern = [{"POS": "NOUN", "OP": "+"}, {"IS_PUNCT": True, "OP": "?"}, {"LOWER": "and", "OP": "?"}, {"POS": "NOUN", "OP": "?"}]
    
    # Add patterns to the matcher
    matcher.add("DURATION", [duration_pattern])
    matcher.add("GROUP", [group_pattern])
    matcher.add("CAUSE", [cause_pattern])
    matcher.add("LOCATION", [location_pattern])
    
    # Find matches in the text
    matches = matcher(doc)
    
    # Initialize result dictionary
    result = {
        "number_of_people": None,
        "group_involved": [],
        "cause_of_strike": [],
        "location": [],
        "duration": None
    }
    
    # Extract the number of people involved using regex
    number_of_people_match = re.search(r'\b\d{1,3}(?:,\d{3})*\b', content)
    if number_of_people_match:
        result["number_of_people"] = number_of_people_match.group()
    
    # Process matches
    for match_id, start, end in matches:
        span = doc[start:end]
        match_label = nlp.vocab.strings[match_id]
        
        if match_label == "DURATION":
            result["duration"] = span.text
        elif match_label == "GROUP":
            result["group_involved"].append(span.text)
        elif match_label == "CAUSE":
            result["cause_of_strike"].append(span.text)
        elif match_label == "LOCATION":
            result["location"].append(span.text)
    
    # Post-process to clean up results
    result["group_involved"] = ", ".join(result["group_involved"]).replace(" and", ",").strip(", ")
    result["cause_of_strike"] = ", ".join(result["cause_of_strike"]).replace("for ", "").strip(", ")
    result["location"] = ", ".join(result["location"]).replace("in ", "").strip(", ")

    
    return result


def process_article(article:str='', content:str='')->None:
    """
    Function will take in the article link and the content extracted from the link and feed it to the NLP model for data extraction

    Args:
        article (str, optional): Link of the article. Defaults to ''.
        content (str, optional): Content extracted from the article. Defaults to ''.
    """
    logger.info(f"Data extraction started for article : {article}")
    data = get_content_information(content=content)
    extracted_data[article] = data
    logger.info(f"Data extraction completed for article : {article}")


def store_as_json(filepath, data_dict):
    open(filepath, "w").write(json.dumps(data_dict, indent=4))


def main():
    """
    Main process flow of the usecase
    """
    logger.info("Main process started")
    logger.info("Article Link extraction process started")
    # get the articles and their related content
    article_links = get_article_links()
    logger.info(f"Found links -> {len(article_links)}")
    logger.info("Article Link extraction process completed")

    logger.info("Article content extraction started")
    article_content_dict = get_links_content(article_links)
    print(f"Found content -> {len(article_content_dict)}")
    logger.info("Article content extraction completed")

    
    logger.info("Data extraction process started")
    for article, content in article_content_dict.items():
        extracted_data[article] = get_content_information(content=content)
    
    logger.info("Data extraction process completed")
    
    # check if the output directory present if not make one
    output_directory_name = "Output"
    output_directory_path = make_dir_if_not_exists(directory_name=output_directory_name)

    logger.info("Storing Article-Content dictionary")
    # store article and content as JSON
    store_as_json(filepath=fr"{output_directory_path}\article-content.json", data_dict=article_content_dict)
    logger.info(fr"Stored Article-Content dictionary at location : {output_directory_path}\article-content.json")

    logger.info("Storing Extracted data dictionary")
    # store the extracted data as JSON
    store_as_json(filepath=fr"{output_directory_path}\extracted-data.json", data_dict=extracted_data)
    logger.info(fr"Stored Extracted data dictionary at location : {output_directory_path}\extracted-data.json")


if __name__ == '__main__':
    main()