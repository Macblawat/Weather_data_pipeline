import requests
import json
from datetime import datetime
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

load_dotenv()  
API_KEY = os.getenv("OPENWEATHER_API_KEY")



if not API_KEY:
    raise ValueError("API key not found.")

cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan",
          "Lodz", "Szczecin", "Lublin", "Katowice", "Bialystok"]

os.makedirs("weather_data", exist_ok=True)

weather_data = {}

for city in cities:
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city},PL&appid={API_KEY}&units=metric"
    
    try:
        response = requests.get(url,timeout=10)
        response.raise_for_status()
        weather_data[city] = response.json()
        logging.info(f"Scrapped weather for {city}")
        
    except requests.exceptions.RequestException as e:
           logging.error(f"Error getting data for {city}: {e}")
           
if weather_data:
    filename = f"weather_data/weather_batch_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    logging.info(f"Saved batch weather data -> {filename}")