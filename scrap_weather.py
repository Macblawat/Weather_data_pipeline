import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()  # loads from .env
API_KEY = os.getenv("OPENWEATHER_API_KEY")