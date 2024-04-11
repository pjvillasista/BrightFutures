import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark.session import Session
import os
from dotenv import load_dotenv
from uuid import uuid4

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
HEADLESS_MODE = True
TIMEOUT = 10
MAX_WORKERS = 4


def init_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")  # Disable GPU hardware acceleration
    options.add_argument("--disable-extensions")  # Disable extensions
    options.add_argument("--no-sandbox")  # Bypass OS security model
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def get_school_data_for_city(city, grades, batch_id, load_timestamp):
    driver = init_driver(HEADLESS_MODE)
    all_schools_list = []

    try:
        for grade_key, grade_name in grades.items():
            url = f"https://www.greatschools.org/california/{city.lower().replace(' ', '-')}/schools/?gradeLevels={grade_key}"
            driver.get(url)

            while True:
                WebDriverWait(driver, TIMEOUT).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "li.school-card")
                    )
                )
                school_cards = driver.find_elements(By.CSS_SELECTOR, "li.school-card")
                for school in school_cards:
                    school_data = extract_school_data(
                        school, city, batch_id, load_timestamp
                    )
                    all_schools_list.append(school_data)

                # Check if a next page is available
                try:
                    next_button = WebDriverWait(driver, TIMEOUT).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.next_page"))
                    )
                    driver.execute_script("arguments[0].click();", next_button)
                except TimeoutException:
                    # No next page, break the loop
                    break

    finally:
        driver.quit()

    return all_schools_list


def extract_school_data(school, city, batch_id, load_timestamp):
    # Extract the school name
    name = school.find_element(By.CSS_SELECTOR, "a.name").text

    # Extract the address, if available
    try:
        address = (
            school.find_element(By.CSS_SELECTOR, ".address").text.split("•")[0].strip()
        )
    except NoSuchElementException:
        address = None

    # Extract the GreatSchools Rating
    try:
        gso_rating = school.find_element(
            By.CSS_SELECTOR, ".gs-rating .circle-rating--search-page"
        ).text.split("/")[0]
    except NoSuchElementException:
        gso_rating = None

    # Extract subratings like Academic Progress and Test Scores
    subratings = {}
    for subrating in school.find_elements(By.CSS_SELECTOR, ".subratings .subrating"):
        try:
            subrating_name = subrating.find_element(By.CSS_SELECTOR, ".name").text
            subrating_value = subrating.find_element(
                By.CSS_SELECTOR, ".circle-rating--xx-small"
            ).text
            subratings[subrating_name] = subrating_value
        except NoSuchElementException:
            subratings[subrating_name] = None

    # Extract School Types
    try:
        filter_chips = school.find_elements(
            By.CSS_SELECTOR, ".filter-chips .filter-chip"
        )
        school_types = [chip.text for chip in filter_chips]
    except NoSuchElementException:
        school_types = []

    # Extract the star rating and review link
    try:
        star_rating = school.find_element(
            By.CSS_SELECTOR, ".user-rating .five-stars .rating-value"
        ).text
    except NoSuchElementException:
        star_rating = None

    try:
        review_link_element = school.find_element(
            By.XPATH, ".//a[contains(@href, '/reviews/')]"
        )
        review_link = review_link_element.get_attribute("href")
    except NoSuchElementException:
        review_link = None

    # Extract the school link
    try:
        school_link = school.find_element(
            By.CSS_SELECTOR, "div.header > a"
        ).get_attribute("href")
    except NoSuchElementException:
        school_link = None

    # Prepare the school data dictionary
    school_data = {
        "school_name": name,
        "address": address,
        "gs_rating": gso_rating,
        "academic_progress": subratings.get("Academic Progress", None),
        "test_scores": subratings.get("Test Scores", None),
        "equity_scores": subratings.get("Equity", None),
        "school_types": school_types,
        "star_rating": star_rating,
        "review_link": review_link,
        "school_link": school_link,
        "city": city,
        "batch_id": batch_id,
        "extracted_at": load_timestamp,
    }

    return school_data


def snowpark_session_create():
    connection_params = {
        "account": "<account_id>",
        "user": "<user_created_in_snowflake>",
        "password": "<password_for_user>",
        "role": "transform",
        "warehouse": "<your_warehouse>",
        "database": "<your database>",
        "schema": "<target schema>",
    }
    session = Session.builder.configs(connection_params).create()
    return session


def load_data_to_snowflake(session, df, table_name):
    # Convert Pandas DataFrame to Snowpark DataFrame
    sp_df = session.create_dataframe(df)

    # Write the Snowpark DataFrame to a Snowflake table
    sp_df.write.save_as_table(table_name, mode="append", table_type="temporary")


def main():
    session = snowpark_session_create()
    # Generate a unique batch ID and current timestamp
    batch_id = str(uuid4())
    load_timestamp = pd.Timestamp.now(tz="UTC")

    # with open('cities.txt') as file:
    #     cities = file.read()

    # cities = [city.strip() for city in cities.split('\n') if city.strip()]

    cities = ["Irvine"]
    grades = {"e": "Elementary", "m": "Middle", "h": "High"}

    # Use ThreadPoolExecutor to parallelize data collection
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks
        futures = {
            executor.submit(
                get_school_data_for_city, city, grades, batch_id, load_timestamp
            ): city
            for city in cities
        }

        all_schools_data = []
        # Collect results
        for future in as_completed(futures):
            all_schools_data.extend(future.result())

    # Construct DataFrame from collected data
    df = pd.DataFrame(all_schools_data)

    # Save the DataFrame to Snowflake
    table_name = "RAW_SCHOOL_INFO"
    load_data_to_snowflake(session, df, table_name)


if __name__ == "__main__":
    main()
