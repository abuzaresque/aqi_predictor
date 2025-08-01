# import requests
# import pandas as pd
# from datetime import datetime
# import os
# from dotenv import load_dotenv

# # --- Load API Key ---
# load_dotenv()
# api_key = os.getenv("OPENWEATHER_API")

# # --- CONFIG ---
# API_KEY = "38c5bdf8424629e6d5016de1f1d0aeb1"  # Replace with your API key
# LAT = 24.8607
# LON = 67.0011
# CSV_PATH = "karachi_aqi_backfill_3.csv"

# # --- Fetch AQI + weather from OpenWeather ---
# def fetch_openweather_full(lat, lon, api_key):
#     # Air quality
#     air_url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
#     air_data = requests.get(air_url).json()

#     if "list" not in air_data or not air_data["list"]:
#         raise Exception("Air Pollution data error")

#     air = air_data["list"][0]["components"]
#     timestamp = datetime.utcfromtimestamp(air_data["list"][0]["dt"])

#     # Weather
#     weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
#     weather_data = requests.get(weather_url).json()

#     main = weather_data.get("main", {})
#     wind = weather_data.get("wind", {})
#     clouds = weather_data.get("clouds", {})

#     # Only include fields available in Open-Meteo too
#     return {
#         "time": timestamp,
#         "pm2_5": air.get("pm2_5"),
#         "pm10": air.get("pm10"),
#         "carbon_monoxide": air.get("co"),
#         "nitrogen_dioxide": air.get("no2"),
#         "ozone": air.get("o3"),
#         "temperature": main.get("temp"),
#         "humidity": main.get("humidity"),
#         "pressure": main.get("pressure"),
#         "wind_speed": wind.get("speed"),
#         "wind_deg": wind.get("deg"),
#         "cloud_coverage": clouds.get("all"),
#     }

# # --- Add time features ---
# def add_time_features(row):
#     ts = pd.to_datetime(row["time"])
#     row["hour"] = ts.hour
#     row["day"] = ts.day
#     row["month"] = ts.month
#     row["weekday"] = ts.weekday()
#     return row

# # --- Main function ---
# def main():
#     try:
#         existing_df = pd.read_csv(CSV_PATH)
#         existing_df["time"] = pd.to_datetime(existing_df["time"])
#     except FileNotFoundError:
#         print("📄 File not found. Creating new one.")
#         existing_df = pd.DataFrame()

#     try:
#         new_row = fetch_openweather_full(LAT, LON, API_KEY)
#         new_row = add_time_features(new_row)
#         new_df = pd.DataFrame([new_row])

#         if not existing_df.empty and new_df["time"].iloc[0] in existing_df["time"].values:
#             print("✅ Already recorded. Skipping.")
#         else:
#             updated_df = pd.concat([existing_df, new_df], ignore_index=True)
#             updated_df.to_csv(CSV_PATH, index=False)
#             print(f"✅ New row added for {new_df['time'].iloc[0]}")

#     except Exception as e:
#         print("❌ Failed to fetch or append:", e)

# if __name__ == "__main__":
#     main()



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
