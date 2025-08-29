import requests
import json
from datetime import datetime
import os
import logging
from dotenv import load_dotenv
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

load_dotenv()  
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("API key not found.")

MAX_RETRIES = 3
RETRY_DELAY = 5

cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan",
          "Lodz", "Szczecin", "Lublin", "Katowice", "Bialystok"]

os.makedirs("weather_data", exist_ok=True)

weather_data = {}

for city in cities:
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
        logging.error(f"Failed to fetch weather for {city} after {MAX_RETRIES} attempts")    
    
if weather_data:
    filename = f"weather_data/weather_batch_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    logging.info(f"Saved batch weather data -> {filename}")