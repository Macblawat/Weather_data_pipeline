import os


local_folder = "weather_data"

for filename in os.listdir(local_folder):
    if filename.endswith(".json"):
        file_path = os.path.join(local_folder, filename)
        os.remove(file_path) 