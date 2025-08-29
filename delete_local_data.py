import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

local_folder = "weather_data"

for filename in os.listdir(local_folder):
    if filename.endswith(".json"):
        file_path = os.path.join(local_folder, filename)
        os.remove(file_path) 
        logging.info('f"Deleted file : {file_path}')