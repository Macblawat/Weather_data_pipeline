import requests
import json
from datetime import datetime
import os
import logging
from dotenv import load_dotenv
import time
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

MAX_RETRIES = config["retries"]
RETRY_DELAY = config["retry_delay"]
cities = config["cities"]

load_dotenv()  
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("API key not found.")

os.makedirs("weather_data", exist_ok=True)

weather_data = {}

def scrap_weather_for_city(city, api_key,session):
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    url = f"{base_url}?q={city},PL&appid={api_key}&units=metric"
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            logging.info(f"Scrapped weather for {city}")
            return city, response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            logging.error(f"[{city}] attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                logging.info(f"Retrying {city} in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
    logging.error(f"Failed to fetch weather for {city} after {MAX_RETRIES} attempts")
    return city, None

with requests.Session() as session:
    with ThreadPoolExecutor(max_workers=5) as executor:  
        city_weathers = {executor.submit(scrap_weather_for_city, city, API_KEY,session): city for city in cities}
        for city_weather in as_completed(city_weathers):
            city, data = city_weather.result()
            if data:
                weather_data[city] = data
     
 # this is a code for inserting the cities sequentially and not parallel, the first version of the scrapper           
'''for city in cities:
  base_url = "http://api.openweathermap.org/data/2.5/weather"
  url = f"{base_url}?q={city},PL&appid={API_KEY}&units=metric"
  success=False
  for attempt in range(1, MAX_RETRIES + 1):   
    try:
        response = requests.get(url,timeout=10)
        response.raise_for_status()
        weather_data[city] = response.json()
        success=True
        logging.info(f"Scrapped weather for {city}")
        break
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting data for {city}: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding failed for {city}: {e}")
    
    if attempt < MAX_RETRIES:
        logging.info(f"Retrying {city} in {RETRY_DELAY} seconds...")
        time.sleep(RETRY_DELAY)
        
  if not success:
        logging.error(f"Failed to fetch weather for {city} after {MAX_RETRIES} attempts")   ''' 
    
if weather_data:
    filename = f"weather_data/weather_batch_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    logging.info(f"Saved batch weather data -> {filename} with {len(weather_data)} cities")
else:
    logging.warning("No weather data collected. Exiting.")
    exit(1) 