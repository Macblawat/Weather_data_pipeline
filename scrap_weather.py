import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()  
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("API key not found.")

cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan",
          "Lodz", "Szczecin", "Lublin", "Katowice", "Bialystok"]

for city in cities:
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city},PL&appid={API_KEY}&units=metric"
    response = requests.get(url)
    
    if response.status_code == 200:
        
        filename = f"weather_data/weather_{city}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(filename, "w", encoding="utf-8") as f:
         json.dump(response.json(), f, indent=4, ensure_ascii=False)

        print(f"Saved weather data for {city} -> {filename}")
    else:
        print(f"Error getting data for {city}: {response.status_code} - {response.text}")