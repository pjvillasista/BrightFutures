import logging
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
HEADLESS_MODE = True
TIMEOUT = 10  # seconds for WebDriverWait
SLEEP_TIME_AFTER_PAGE_LOAD = 1  # seconds, adjust based on your network speed
SCHOOL_DATA_CSV_FILE = "../data_test/raw/raw_school_data_test.csv"
MAX_WORKERS = 4  # Number of concurrent threads for parallel execution

# Initialize WebDriver
def init_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("start-maximized")  # Start maximized to avoid certain elements being hidden

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

# Get school data from a city
def get_school_data_for_city(city, grades):
    driver = init_driver(HEADLESS_MODE)
    schools_seen = set()
    all_schools_list = []
    try:
        for grade_key, grade_name in grades.items():
            url = f"https://www.greatschools.org/california/{city.lower().replace(' ', '-')}/schools/?gradeLevels={grade_key}"
            driver.get(url)

            while True:
                try:
                    WebDriverWait(driver, TIMEOUT).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.school-card"))
                    )
                except TimeoutException:
                    logging.warning(f"Timeout waiting for school cards in {city} for grade {grade_name}.")
                    break

                school_cards = driver.find_elements(By.CSS_SELECTOR, "li.school-card")
                for school in school_cards:
                    school_data = extract_school_data(school, city)
                    school_id = f"{school_data['School Name']} - {school_data['Address']}"
                    if school_id not in schools_seen:
                        schools_seen.add(school_id)
                        all_schools_list.append(school_data)

                if not navigate_to_next_page(driver):
                    break
    finally:
        driver.quit()
    return all_schools_list

# Extract school data from a school card element
def extract_school_data(school, city):
    # Extract the school name
    name = school.find_element(By.CSS_SELECTOR, "a.name").text

    # Extract the address, if available
    try:
        address = school.find_element(By.CSS_SELECTOR, ".address").text.split("•")[0].strip()
    except NoSuchElementException:
        address = "N/A"
    
    # Extract the GreatSchools Rating
    try:
        gso_rating = school.find_element(By.CSS_SELECTOR, ".gs-rating .circle-rating--search-page").text.split('/')[0]
    except NoSuchElementException:
        gso_rating = "N/A"
    
    # Extract subratings like Academic Progress and Test Scores
    subratings = {}
    for subrating in school.find_elements(By.CSS_SELECTOR, ".subratings .subrating"):
        try:
            subrating_name = subrating.find_element(By.CSS_SELECTOR, ".name").text
            subrating_value = subrating.find_element(By.CSS_SELECTOR, ".circle-rating--xx-small").text
            subratings[subrating_name] = subrating_value
        except NoSuchElementException:
            subratings[subrating_name] = "N/A"
    
    # Extract School Types
    try:
        filter_chips = school.find_elements(By.CSS_SELECTOR, ".filter-chips .filter-chip")
        school_types = [chip.text for chip in filter_chips]
    except NoSuchElementException:
        school_types = []

    # Extract the star rating and review link
    try:
        star_rating = school.find_element(By.CSS_SELECTOR, ".user-rating .five-stars .rating-value").text
    except NoSuchElementException:
        star_rating = "N/A"
    
    try:
        review_link_element = school.find_element(By.XPATH, ".//a[contains(@href, '/reviews/')]")
        review_link = review_link_element.get_attribute('href')
    except NoSuchElementException:
        review_link = 'N/A'
    
    # Extract the school link
    try:
        school_link = school.find_element(By.CSS_SELECTOR, "div.header > a").get_attribute("href")
    except NoSuchElementException:
        school_link = "N/A"

    # Prepare the school data dictionary
    school_data = {
        "School Name": name,
        "Address": address,
        "GSO Rating": gso_rating,
        "Academic Progress": subratings.get("Academic Progress", "N/A"),
        "Test Scores": subratings.get("Test Scores", "N/A"),
        "Equity Scores": subratings.get("Equity", "N/A"),
        "School Types": school_types,
        "Star Rating": star_rating,
        "Review Link": review_link,
        "School Link": school_link,
        "City": city,
    }

    return school_data

# Navigate to the next page of schools
def navigate_to_next_page(driver):
    try:
        next_buttons = driver.find_elements(By.CSS_SELECTOR, "a.anchor-button:not(.disabled)")
        if next_buttons and "icon-chevron-right" in next_buttons[-1].get_attribute("innerHTML"):
            next_button = next_buttons[-1]
            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(SLEEP_TIME_AFTER_PAGE_LOAD)
            return True
    except Exception as e:
        logging.error(f"Error navigating to next page: {e}")
    return False

# Main function to orchestrate the scraping in parallel
def main():
    with open('cities.txt') as file:
        cities = file.read()

    cities = [city.strip() for city in cities.split('\n') if city.strip()]
    grades = {"e": "Elementary", "m": "Middle", "h": "High"}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(get_school_data_for_city, city, grades) for city in cities]
        all_schools_data = []
        for future in as_completed(futures):
            all_schools_data.extend(future.result())

    df_schools = pd.DataFrame(all_schools_data)
    df_schools.to_csv(SCHOOL_DATA_CSV_FILE, index=False)

if __name__ == "__main__":
    main()