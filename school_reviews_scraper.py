from snowflake.snowpark.session import Session
from uuid import uuid4
import pandas as pd
from datetime import datetime
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
)


def snowpark_session_create():
    connection_params = {
        "account": "...",
        "user": "...",
        "password": "...!",
        "role": "transform",
        "warehouse": "...",
        "database": "BRIGHTFUTURES",
        "schema": "RAW",
    }
    session = Session.builder.configs(connection_params).create()

    return session


def get_reviews(review_link):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    with webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=chrome_options
    ) as driver:
        wait = WebDriverWait(driver, 10)
        reviews = []
        if not review_link.startswith("http"):
            logging.info(f"Invalid review link: {review_link}")
            return reviews

        driver.get(review_link)

        while True:
            # Try to find "More" links and click if available
            try:
                more_links = driver.find_elements(
                    By.CSS_SELECTOR,
                    "div.review-list-column div.five-star-review div.comment > span > span > a",
                )
                for link in more_links:
                    try:
                        wait.until(EC.element_to_be_clickable(link))
                        driver.execute_script("arguments[0].click();", link)
                        # Adding a short delay to ensure the page has time to render after clicking
                        wait.until(
                            lambda driver: link.get_attribute("aria-expanded") == "true"
                            or not link.is_displayed(),
                            "Failed to expand review content.",
                        )
                    except Exception as e:
                        print(f"Error clicking a 'More' link: {e}")
            except Exception as e:
                print(f"Error finding 'More' links: {e}")

            # Extract the review texts
            try:
                review_elements = driver.find_elements(
                    By.CSS_SELECTOR,
                    "div.review-list-column div.five-star-review div.comment > span",
                )
                reviews = [
                    element.text for element in review_elements if element.text != ""
                ]
            except Exception as e:
                print(f"Error extracting reviews: {e}")

            # Attempt to go to the next page
            try:
                next_page_buttons = driver.find_elements(
                    By.CSS_SELECTOR, "a.anchor-button:not(.disabled)"
                )
                if next_page_buttons and "icon-chevron-right" in next_page_buttons[
                    -1
                ].get_attribute("innerHTML"):
                    next_button = next_page_buttons[-1]
                    driver.execute_script(
                        "arguments[0].scrollIntoView(true);", next_button
                    )
                    driver.execute_script("arguments[0].click();", next_button)
                    # Wait for a condition that indicates the page has loaded.
                    wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "div.review-list-column")
                        )
                    )
                else:
                    print("No more pages to navigate.")
                    break
            except (NoSuchElementException, TimeoutException):
                print("No more pages or timed out waiting for page to load.")
                break

        return reviews


def load_data_to_snowflake(session, df, table_name):
    # Convert Pandas DataFrame to Snowpark DataFrame
    sp_df = session.create_dataframe(df)

    # Write the Snowpark DataFrame to a Snowflake table
    sp_df.write.save_as_table(table_name, mode="append", table_type="temporary")


def main():
    # Start snowpark session
    session = snowpark_session_create()

    # Call the table, make it a pandas dataframe
    df = session.table("RAW_SCHOOL_INFO").to_pandas()
    df = df.dropna(subset=["REVIEW_LINK"])

    batch_id = str(uuid4())
    extract_timestamp = pd.Timestamp.now(tz="UTC")

    review_data = []

    for _, row in df.iterrows():
        review_link = row["REVIEW_LINK"]
        school_name = row["SCHOOL_NAME"]
        address = row["ADDRESS"]
        individual_reviews = get_reviews(review_link)

        for review in individual_reviews:
            review_data.append(
                {
                    "school_name": school_name,
                    "address": address,
                    "review_text": review,
                    "batch_id": batch_id,
                    "extracted_at": extract_timestamp,
                }
            )

    reviews_df = pd.DataFrame(review_data)

    load_data_to_snowflake(session, reviews_df, "RAW_SCHOOL_REVIEW")

    # Make sure to close session
    session.close()


if __name__ == "__main__":
    main()
