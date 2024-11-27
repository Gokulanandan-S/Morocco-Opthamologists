import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def init_driver():
    service = Service(executable_path=driver_path)
    options = webdriver.ChromeOptions()
    options.add_argument("--lang=fr")
    # options.add_argument("--headless")
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def get_info(url, driver):
    try:
    
        driver.get(url)

        phone_button = driver.find_element(By.ID, 'telephoneFirme')
        phone_button.click()
        phone_element = WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.ID, 'telephoneFirmeDisplay'))
        )

        name_element = driver.find_element(By.CLASS_NAME, 'nomprenomclient')
        name = name_element.text
        address_element = driver.find_element(By.CLASS_NAME, 'adresseclient')
        address = address_element.text
        phone = phone_element.text

        return name, address, phone, url
    
    except Exception as e:
        logging.error(f"Error retrieving info from {url}: {e}")
        return None, None, None, url

def scrape_profile_urls(driver, url):
    driver.get(url)
    doctor_links = driver.find_elements(By.CSS_SELECTOR, '.nomprenomclient a')

    profile_pages = []
    
    for link in doctor_links:
        href = link.get_attribute('href')
        if href and not href.startswith("mailto:"):
            href = href.replace('https://www.medicalis.mahttps://www.medicalis.ma', 'https://www.medicalis.ma')
            profile_pages.append(href)
    return profile_pages

def worker(url):
    driver = init_driver()
    result = get_info(url, driver)
    driver.quit()
    return result

driver_path = "C:/Users/Anandan Suresh/Documents/Roche/morroco data/chromedriver.exe"  

url = 'https://www.medicalis.ma/liste?tags=Ophtalmologue+&ReferenceVille=080'


csv_file_path = "C:/Users/Anandan Suresh/Documents/Roche/morroco data/tanger2.csv"
with open(csv_file_path, 'w', newline='', encoding='utf-8-sig') as file:
    writer = csv.writer(file)
    writer.writerow(['Name', 'Address', 'Phone', 'Profile URL'])

    driver = init_driver()

    for _ in tqdm(range(3)):
        profile_pages = scrape_profile_urls(driver, url)
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(worker, profile_url) for profile_url in profile_pages]
            for future in as_completed(futures):
                name, address, phone, profile_url = future.result()
                if name and address and phone:
                    writer.writerow([name, address, phone, profile_url])
                else:
                    writer.writerow(['Unable to retrieve information', '', '', profile_url])

        time.sleep(3)

    driver.quit()
