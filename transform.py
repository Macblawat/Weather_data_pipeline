import os
import json
import pyodbc
from datetime import datetime,timezone
import pandas as pd
import logging
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

load_dotenv()  
database = os.getenv("DATABASE_NAME")
username=os.getenv("DATABASE_USERNAME")
password=os.getenv("DATABASE_PASSWORD")
server=os.getenv("SQL_SERVER_CONN")
driver = "{ODBC Driver 18 for SQL Server}"

conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()


json_folder = "weather_data"


for filename in os.listdir(json_folder):
    if  filename.endswith(".json"):
     filepath = os.path.join(json_folder, filename)
     with open(filepath, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except ValueError:
            logging.error(f"Invalid JSON: {filename}")
            continue
    
def get_or_create_city(city_data):
    cursor.execute("SELECT city_id FROM city WHERE api_city_id = ?", city_data['id'])
    row = cursor.fetchone()
    if row :
        return row.city_id
    cursor.execute("""
        INSERT INTO city (api_city_id, city_name, country_id, latitude, longitude)
        VALUES (?, ?, ?, ?, ?)""",
        city_data['id'], city_data['name'], city_data['sys']['country'],
        city_data['coord']['lat'], city_data['coord']['lon'])
    conn.commit()
    city_db_id = cursor.execute("SELECT city_id FROM city WHERE api_city_id = ?", city_data['id']).fetchone().city_id
    return city_db_id

def get_or_create_weather_info(weather):
    cursor.execute("SELECT weather_id FROM weather_info WHERE api_weather_id = ?", weather['id'])
    row = cursor.fetchone()
    if row:
        return row.weather_id
    cursor.execute("""
        INSERT INTO weather_info (api_weather_id, main, description)
        VALUES (?, ?, ?)""",
        weather['id'], weather['main'], weather['description'])
    conn.commit()
    weather_info_db_id=cursor.execute("SELECT weather_id FROM weather_info WHERE api_weather_id = ?", weather['id']).fetchone().weather_id
    return weather_info_db_id

def get_or_create_date(unix_timestamp):
    dt = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
    cursor.execute("SELECT date_id FROM date WHERE full_date = ?", dt.date())
    row = cursor.fetchone()
    if row:
        return row.date_id
    cursor.execute("""
        INSERT INTO date (full_date, year, month, day, weekday)
        VALUES (?, ?, ?, ?, ?)""",
        dt.date(), dt.year, dt.month, dt.day, dt.strftime('%A'))
    conn.commit()
    date_db_id= cursor.execute("SELECT date_id FROM date WHERE full_date = ?", dt.date()).fetchone().date_id
    return date_db_id

def get_or_create_time(unix_timestamp):
    dt = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

    cursor.execute("SELECT time_id FROM time WHERE hour = ? AND minute = ?", dt.hour, dt.minute)
    row = cursor.fetchone()
    if row:
        return row.time_id

    cursor.execute("""
        INSERT INTO time (hour, minute, time_str)
        VALUES (?, ?, ?)
    """, dt.hour, dt.minute, dt.strftime("%H:%M"))
    conn.commit()

    time_db_id = cursor.execute(
        "SELECT time_id FROM time WHERE hour = ? AND minute = ?", dt.hour, dt.minute
    ).fetchone().time_id
    return time_db_id

for city_name, city_data in data.items():
  
    try:
        city_id = get_or_create_city(city_data)
        weather_id = None
        if city_data.get('weather'):
            weather_id = get_or_create_weather_info(city_data['weather'][0])
        date_id = get_or_create_date(city_data['dt'])
        time_id=get_or_create_time(city_data['dt'])
        
        main = city_data.get('main', {})
        wind = city_data.get('wind', {})
        clouds = city_data.get('clouds', {})
        rain = city_data.get('rain', {})
        snow = city_data.get('snow', {})
        sys = city_data.get('sys', {})

        rain_1h = rain.get('1h', 0)
        snow_1h = snow.get('1h', 0)

        cursor.execute("""
            INSERT INTO weather (
                city_id, weather_id, date_id, time_id, temperature, feels_like, temp_min, temp_max,
                pressure, sea_level, ground_level, humidity, visibility, wind_speed, wind_deg, wind_gust,
                cloudiness, rain_1h, snow_1h
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        city_id,
        weather_id,
        date_id,
        time_id,
        main.get('temp'),
        main.get('feels_like'),
        main.get('temp_min'),
        main.get('temp_max'),
        main.get('pressure'),
        main.get('sea_level'),
        main.get('grnd_level'),
        main.get('humidity'),
        city_data.get('visibility'),
        wind.get('speed'),
        wind.get('deg'),
        wind.get('gust'),
        clouds.get('all'),
        rain_1h,
        snow_1h,

        )
        conn.commit()
        logging.info(f"Inserted weather for {city_data['name']}")
    except Exception as e:
        logging.error(f"Failed to insert {city_data.get('name','Unknown')}: {e}")

# Close connection
cursor.close()
conn.close()

