import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import hopsworks

# --- Load API Keys from .env ---
load_dotenv()
openweather_api_key = os.getenv("OPENWEATHER_API")
hopsworks_api_key = os.getenv("HOPSWORKS_API_KEY")

# --- CONFIG ---
LAT = 24.8607
LON = 67.0011
FEATURE_GROUP_NAME = "karachi_aqi_raw"
FEATURE_GROUP_VERSION = 1

# --- Fetch AQI + weather from OpenWeather ---
def fetch_openweather_full(lat, lon, api_key):
    # Air quality
    air_url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
    air_data = requests.get(air_url).json()


    if "list" not in air_data or not air_data["list"]:
        raise Exception("Air Pollution data error")

    air = air_data["list"][0]["components"]
    timestamp = datetime.utcfromtimestamp(air_data["list"][0]["dt"])

    # Weather
    weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    weather_data = requests.get(weather_url).json()

    main = weather_data.get("main", {})
    wind = weather_data.get("wind", {})
    clouds = weather_data.get("clouds", {})

    return {
        "time": timestamp,
        "pm2_5": air.get("pm2_5"),
        "pm10": air.get("pm10"),
        "carbon_monoxide": air.get("co"),
        "nitrogen_dioxide": air.get("no2"),
        "ozone": air.get("o3"),
        "temperature": main.get("temp"),
        "humidity": main.get("humidity"),
        "pressure": main.get("pressure"),
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "cloud_coverage": clouds.get("all"),
        "hour": timestamp.hour,
        "day": timestamp.day,
        "month": timestamp.month,
        "weekday": timestamp.weekday()
    }

# --- Main function ---
def main():
    try:
        # Fetch the new row
        new_row = fetch_openweather_full(LAT, LON, openweather_api_key)
        # df = pd.DataFrame([new_row])
        # Force float where needed to match Hopsworks schema
        float_columns = ["humidity", "pressure", "wind_deg", "cloud_coverage"]
        for col in float_columns:
            if new_row[col] is not None:
                new_row[col] = float(new_row[col])

        df = pd.DataFrame([new_row])


        # Upload to Hopsworks
        project = hopsworks.login(api_key_value=hopsworks_api_key)
        fs = project.get_feature_store()
        fg = fs.get_feature_group(FEATURE_GROUP_NAME, version=FEATURE_GROUP_VERSION)

        fg.insert(df, write_options={"wait_for_job": True})
        print(f"✅ Row inserted for {new_row['time']} into Hopsworks.")
    except Exception as e:
        print("❌ Failed to fetch or insert:", e)

if __name__ == "__main__":
    main()
