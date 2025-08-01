# # import requests
# # import pandas as pd
# # from datetime import datetime, timedelta

# # # Karachi Coordinates
# # latitude = 24.8607
# # longitude = 67.0011

# # # Date range: last 30 days

# # end_date = datetime(2025,7,1)
# # start_date = end_date - timedelta(days=200)
# # # print(end_date,start_date)

# # # Open-Meteo Air Quality API URL
# # url = (
# #     f"https://air-quality-api.open-meteo.com/v1/air-quality"
# #     f"?latitude={latitude}&longitude={longitude}"
# #     f"&start_date={start_date}&end_date={end_date}"
# #     f"&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone"
# #     f"&timezone=auto"
# # )

# # # Fetch and parse JSON
# # response = requests.get(url)
# # data = response.json()

# # # Convert to DataFrame
# # df = pd.DataFrame(data["hourly"])
# # df["time"] = pd.to_datetime(df["time"])

# # # Add time-based features
# # df["hour"] = df["time"].dt.hour
# # df["day"] = df["time"].dt.day
# # df["month"] = df["time"].dt.month
# # df["weekday"] = df["time"].dt.weekday

# # # Save to CSV
# # df.to_csv("karachi_aqi_backfill.csv", index=False)

# # print("✅ Karachi AQI data saved: karachi_aqi_backfill.csv")


# # import requests
# # import pandas as pd
# # from datetime import datetime, timedelta

# # LAT = 24.8607
# # LON = 67.0011

# # # 230 days range
# # end_date = datetime.today().date()
# # # start_date = end_date - timedelta(days=230)
# # start_date = end_date - timedelta(days=7)


# # # Open-Meteo historical API
# # url = (
# #     "https://archive-api.open-meteo.com/v1/archive"
# #     f"?latitude={LAT}&longitude={LON}"
# #     f"&start_date={start_date}&end_date={end_date}"
# #     "&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,"
# #     "temperature_2m,relative_humidity_2m,dew_point_2m,precipitation,"
# #     "cloudcover,windspeed_10m,winddirection_10m,pressure_msl"
# #     "&timezone=auto"
# # )

# # # --- Request and parse ---
# # response = requests.get(url)
# # data = response.json()

# # # Debug: Print keys
# # print("📦 Response keys:", data.keys())
# # print("🔍 Sample:", str(data)[:500])

# # # --- Check and convert to DataFrame ---
# # if "hourly" not in data:
# #     raise Exception("❌ 'hourly' key missing in response. Check field names or API format.")

# # df = pd.DataFrame(data["hourly"])
# # df["time"] = pd.to_datetime(df["time"])

# # # Add time-based features
# # df["hour"] = df["time"].dt.hour
# # df["day"] = df["time"].dt.day
# # df["month"] = df["time"].dt.month
# # df["weekday"] = df["time"].dt.weekday

# # # Save
# # df.to_csv("karachi_aqi_backfill_2.csv", index=False)
# # print("✅ Saved 230 days of data to karachi_aqi_backfill.csv")



# import requests
# import pandas as pd
# from datetime import datetime, timedelta

# LAT = 24.8607
# LON = 67.0011
# DAYS = 230

# # Dates
# end_date = datetime.today().date()
# start_date = end_date - timedelta(days=DAYS)

# # -------- GET AIR POLLUTION DATA --------
# pollution_url = (
#     "https://air-quality-api.open-meteo.com/v1/air-quality"
#     f"?latitude={LAT}&longitude={LON}"
#     f"&start_date={start_date}&end_date={end_date}"
#     "&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone"
#     "&timezone=auto"
# )

# pollution = requests.get(pollution_url).json()
# if "hourly" not in pollution:
#     raise Exception("❌ Air pollution data not returned.")

# pollution_df = pd.DataFrame(pollution["hourly"])
# pollution_df["time"] = pd.to_datetime(pollution_df["time"])

# # -------- GET WEATHER DATA --------
# weather_url = (
#     "https://archive-api.open-meteo.com/v1/archive"
#     f"?latitude={LAT}&longitude={LON}"
#     f"&start_date={start_date}&end_date={end_date}"
#     "&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,"
#     "precipitation,cloudcover,windspeed_10m,winddirection_10m,pressure_msl"
#     "&timezone=auto"
# )

# weather = requests.get(weather_url).json()
# if "hourly" not in weather:
#     raise Exception("❌ Weather data not returned.")

# weather_df = pd.DataFrame(weather["hourly"])
# weather_df["time"] = pd.to_datetime(weather_df["time"])

# # -------- MERGE BOTH --------
# df = pd.merge(pollution_df, weather_df, on="time")

# # Add time features
# df["hour"] = df["time"].dt.hour
# df["day"] = df["time"].dt.day
# df["month"] = df["time"].dt.month
# df["weekday"] = df["time"].dt.weekday

# # Save
# df.to_csv("karachi_aqi_backfill_2.csv", index=False)
# print("✅ 230-day merged backfill saved to karachi_aqi_backfill.csv")



import requests
import pandas as pd
from datetime import datetime, timedelta

LAT = 24.8607
LON = 67.0011
DAYS = 230

# Dates
end_date = datetime.today().date()
start_date = end_date - timedelta(days=DAYS)

# -------- GET AIR POLLUTION DATA --------
pollution_url = (
    "https://air-quality-api.open-meteo.com/v1/air-quality"
    f"?latitude={LAT}&longitude={LON}"
    f"&start_date={start_date}&end_date={end_date}"
    "&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone"
    "&timezone=auto"
)

pollution = requests.get(pollution_url).json()
if "hourly" not in pollution:
    raise Exception("❌ Air pollution data not returned.")

pollution_df = pd.DataFrame(pollution["hourly"])
pollution_df["time"] = pd.to_datetime(pollution_df["time"])

# -------- GET WEATHER DATA --------
weather_url = (
    "https://archive-api.open-meteo.com/v1/archive"
    f"?latitude={LAT}&longitude={LON}"
    f"&start_date={start_date}&end_date={end_date}"
    "&hourly=temperature_2m,relative_humidity_2m,pressure_msl,windspeed_10m,winddirection_10m,cloudcover"
    "&timezone=auto"
)

weather = requests.get(weather_url).json()
if "hourly" not in weather:
    raise Exception("❌ Weather data not returned.")

weather_df = pd.DataFrame(weather["hourly"])
weather_df["time"] = pd.to_datetime(weather_df["time"])

# Rename Open-Meteo fields to match OpenWeather naming
weather_df = weather_df.rename(columns={
    "temperature_2m": "temperature",
    "relative_humidity_2m": "humidity",
    "pressure_msl": "pressure",
    "windspeed_10m": "wind_speed",
    "winddirection_10m": "wind_deg",
    "cloudcover": "cloud_coverage"
})

# -------- MERGE BOTH --------
df = pd.merge(pollution_df, weather_df, on="time")

# Add time features
df["hour"] = df["time"].dt.hour
df["day"] = df["time"].dt.day
df["month"] = df["time"].dt.month
df["weekday"] = df["time"].dt.weekday

# Save
df.to_csv("karachi_aqi_backfill_3.csv", index=False)
print("✅ 230-day merged backfill saved to karachi_aqi_backfill_2.csv")
