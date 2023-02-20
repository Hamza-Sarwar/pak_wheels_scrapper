import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

engine = create_engine('postgresql://postgres:pass@localhost:5432/pak_wheels')


def page_urls():
    urls = []
    for x in range(1, 2301):
        url = f'https://www.pakwheels.com/used-cars/search/-/?page={x}'
        urls.append(url)
    return urls


def scrape_data(url):
    html_parsed = requests.get(url).content
    soup = BeautifulSoup(html_parsed, "html.parser")
    cars = []
    # Find the script tag containing the JSON-LD snippet
    page_data = soup.find_all("li", class_="classified-listing")
    for data in page_data:
        car_dict = json.loads(data.find_next('script').text)
        parts = car_dict['name'].split('for sale in')
        model_name = parts[0].strip()  # "Toyota Yaris"
        model_year = int(parts[0].split()[-1])  # "2022"
        city = parts[1].strip()  # "Lahore"
        # Create a dataframe for the car
        df = pd.DataFrame({
            'brand': [car_dict['brand']['name']],
            'model_name': [model_name],
            'model_year': [model_year],
            'city': [city],
            'mileage': [int(car_dict['mileageFromOdometer'][:-3].replace(',', ''))],
            'engine_cc': [car_dict['vehicleEngine']['engineDisplacement']],
            'price': [car_dict['offers']['price']],
            'currency': [car_dict['offers']['priceCurrency']],
            'fuel_type': [car_dict['fuelType']],
            'transmission': [car_dict['vehicleTransmission']],
            'image': [car_dict['image']],
            'url': [car_dict['offers']['url']]
        })
        # Append the dataframe to the list of car data
        cars.append(df)
    logging.info(f'{threading.current_thread()} added {df} to db')
    logging.info(f'----------------------------------------------')
    car_data = pd.concat(cars, ignore_index=True)
    try:

        car_data.to_sql('cars', con=engine, if_exists='append', index=False)
    except Exception as e:
        logging.info(e)
    return car_data


if __name__ == "__main__":
    start = time.perf_counter()
    urls = page_urls()
    with ThreadPoolExecutor() as executor:
        executor.map(scrape_data, urls)
    print(f'Total Time: {time.perf_counter() - start} seconds')
