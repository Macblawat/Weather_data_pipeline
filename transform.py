import os
import json
import pyodbc
from datetime import datetime,timezone
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
cursor.fast_executemany = True

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
        OUTPUT INSERTED.city_id
        VALUES (?, ?, ?, ?, ?)""",
        city_data['id'], city_data['name'], city_data['sys']['country'],
        city_data['coord']['lat'], city_data['coord']['lon'])
    city_db_id = cursor.fetchone().city_id
    return city_db_id

def get_or_create_weather_info(weather):
    cursor.execute("SELECT weather_id FROM weather_info WHERE api_weather_id = ?", weather['id'])
    row = cursor.fetchone()
    if row:
        return row.weather_id
    cursor.execute("""
        INSERT INTO weather_info (api_weather_id, main, description)
        OUTPUT INSERTED.weather_id
        VALUES (?, ?, ?)""",
        weather['id'], weather['main'], weather['description'])
    weather_info_db_id=cursor.fetchone().weather_id
    return weather_info_db_id

def get_or_create_date(unix_timestamp):
    dt = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
    cursor.execute("SELECT date_id FROM date WHERE full_date = ?", dt.date())
    row = cursor.fetchone()
    if row:
        return row.date_id
    cursor.execute("""
        INSERT INTO date (full_date, year, month, day, weekday)
        OUTPUT INSERTED.date_id
        VALUES (?, ?, ?, ?, ?)""",
        dt.date(), dt.year, dt.month, dt.day, dt.strftime('%A'))
    date_db_id= cursor.fetchone().date_id
    return date_db_id

def get_or_create_time(unix_timestamp):
    dt = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

    cursor.execute("SELECT time_id FROM time WHERE hour = ? AND minute = ?", dt.hour, dt.minute)
    row = cursor.fetchone()
    if row:
        return row.time_id

    cursor.execute("""
        INSERT INTO time (hour, minute, time_str)
        OUTPUT INSERTED.time_id
        VALUES (?, ?, ?)
    """, dt.hour, dt.minute, dt.strftime("%H:%M"))

    time_db_id = cursor.fetchone().time_id
    return time_db_id

def validate_weather(main, wind, clouds, rain, snow):
    if not (-90 <= main.get('temp', 0) <= 60):
        raise ValueError(f"Temperature out of range: {main.get('temp')}")
    if not (-90 <= main.get('feels_like', 0) <= 60):
        raise ValueError(f"Feels_like out of range: {main.get('feels_like')}")
    if not (-90 <= main.get('temp_min', 0) <= 60):
        raise ValueError(f"Temp_min out of range: {main.get('temp_min')}")
    if not (-90 <= main.get('temp_max', 0) <= 60):
        raise ValueError(f"Temp_max out of range: {main.get('temp_max')}")
    if not (800 <= main.get('pressure', 0) <= 1100):
        raise ValueError(f"Pressure out of range: {main.get('pressure')}")
    if not (0 <= main.get('humidity', 0) <= 100):
        raise ValueError(f"Humidity out of range: {main.get('humidity')}")
    if not (0 <= wind.get('speed', 0) <= 150):
        raise ValueError(f"Wind speed out of range: {wind.get('speed')}")
    if not (0 <= clouds.get('all', 0) <= 100):
        raise ValueError(f"Cloudiness out of range: {clouds.get('all')}")
    if not (0 <= rain.get('1h', 0) <= 500):
        raise ValueError(f"Rain 1h out of range: {rain.get('1h', 0)}")
    if not (0 <= snow.get('1h', 0) <= 500):
        raise ValueError(f"Snow 1h out of range: {snow.get('1h', 0)}")

rows_to_insert = []
    
for city_name, city_data in data.items():
  
    try:
        city_id = get_or_create_city(city_data)
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
        validate_weather(main, wind, clouds, rain, snow)
       
        row=(city_id,
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
        rain.get('1h', 0),
        snow.get('1h', 0))
        
        rows_to_insert.append(row)
    except Exception as e:
        logging.error(f"Failed to process {city_name}: {e}")

if rows_to_insert:
    SQL_insert="""
            INSERT INTO weather (
                city_id, weather_id, date_id, time_id, temperature, feels_like, temp_min, temp_max,
                pressure, sea_level, ground_level, humidity, visibility, wind_speed, wind_deg, wind_gust,
                cloudiness, rain_1h, snow_1h
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    try:
        cursor.executemany(SQL_insert,rows_to_insert)
        conn.commit()
        logging.info(f"Inserted {len(rows_to_insert)} weather records from {filename}")
    except Exception as e:
        logging.error(f"Bulk insert failed for {filename}: {e}")
        conn.rollback()

cursor.close()
conn.close()


