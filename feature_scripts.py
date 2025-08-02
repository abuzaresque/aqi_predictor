# import pandas as pd
# import numpy as np
# from datetime import timedelta
# import hopsworks
# import os
# from dotenv import load_dotenv

# # --- Load API Key ---
# load_dotenv()
# api_key = os.getenv("HOPSWORKS_API_KEY")
# # print(api_key)

# # --- Load Raw Data ---
# # df = pd.read_csv("karachi_aqi.csv")
# project = hopsworks.login(api_key_value=api_key)
# fs = project.get_feature_store()
# fg_raw = fs.get_feature_group("karachi_aqi_raw", version=1)
# df = fg_raw.read()
# df["time"] = pd.to_datetime(df["time"])
# df = df.sort_values("time")

# # --- Feature Engineering ---

# # Time-based
# df["is_weekend"] = df["weekday"].apply(lambda x: 1 if x >= 5 else 0)

# # Lag features
# for col in ["pm2_5", "pm10", "temperature"]:
#     df[f"{col}_lag1"] = df[col].shift(1)
#     df[f"{col}_lag3"] = df[col].shift(3)

# # Rolling statistics
# for col in ["pm2_5", "temperature"]:
#     df[f"{col}_roll_mean_3"] = df[col].rolling(window=3).mean()
#     df[f"{col}_roll_std_6"] = df[col].rolling(window=6).std()

# # Derived features
# df["pm_ratio"] = df["pm2_5"] / (df["pm10"] + 1e-3)
# df["temp_humidity_index"] = df["temperature"] * df["humidity"]

# # Target: PM2.5 24 hours later
# df["target_pm2_5_next_day"] = df["pm2_5"].shift(-24)

# # Drop NaNs from shifts/rolls
# df = df.dropna()

# # --- Upload to Hopsworks ---
# project = hopsworks.login(api_key_value=api_key)
# fs = project.get_feature_store()

# fg = fs.get_or_create_feature_group(
#     name="karachi_aqi_features",
#     version=1,
#     primary_key=["time"],
#     description="Engineered features for Karachi AQI forecasting",
#     event_time="time"
# )

# fg.insert(df, write_options={"wait_for_job": True})
# print("✅ Features uploaded to Hopsworks.")


# # import hopsworks
# # import os
# # from dotenv import load_dotenv

# # load_dotenv()
# # api_key = os.getenv("HOPSWORKS_API_KEY")

# # project = hopsworks.login(api_key_value=api_key)
# # print("✅ Successfully connected to Hopsworks project:", project.name)


import pandas as pd
import numpy as np
from datetime import timedelta
import hopsworks
import os
from dotenv import load_dotenv

# --- Load API Key ---
load_dotenv()
api_key = os.getenv("HOPSWORKS_API_KEY")

# --- Connect to Hopsworks and load raw data ---
project = hopsworks.login(api_key_value=api_key)
fs = project.get_feature_store()
fg_raw = fs.get_feature_group("karachi_aqi_raw", version=1)

df = fg_raw.read()
df["time"] = pd.to_datetime(df["time"])
df = df.sort_values("time").reset_index(drop=True)

# --- Feature Engineering ---

# Time-based
df["is_weekend"] = df["weekday"].apply(lambda x: 1 if x >= 5 else 0)

# Lag features
for col in ["pm2_5", "pm10", "temperature"]:
    df[f"{col}_lag1"] = df[col].shift(1)
    df[f"{col}_lag3"] = df[col].shift(3)

# Rolling stats
for col in ["pm2_5", "temperature"]:
    df[f"{col}_roll_mean_3"] = df[col].rolling(window=3).mean()
    df[f"{col}_roll_std_6"] = df[col].rolling(window=6).std()

# Derived
df["pm_ratio"] = df["pm2_5"] / (df["pm10"] + 1e-3)
df["temp_humidity_index"] = df["temperature"] * df["humidity"]

# Round timestamps to calendar days
df["date"] = df["time"].dt.floor("D")

# --- Create Calendar-Day Targets ---

# Step 1: Group PM2.5 by full day
daily_pm = df.groupby("date")["pm2_5"].mean().reset_index()
daily_pm.columns = ["date", "avg_pm2_5"]

# Step 2: Shift to create targets
daily_pm["target_pm2_5_avg_day1"] = daily_pm["avg_pm2_5"].shift(-1)
daily_pm["target_pm2_5_avg_day2"] = daily_pm["avg_pm2_5"].shift(-2)
daily_pm["target_pm2_5_avg_day3"] = daily_pm["avg_pm2_5"].shift(-3)

# Step 3: Merge targets back into hourly feature DataFrame
df = df.merge(
    daily_pm[["date", "target_pm2_5_avg_day1", "target_pm2_5_avg_day2", "target_pm2_5_avg_day3"]],
    on="date",
    how="left"
)

# Drop rows with any NaNs in features or targets
df = df.dropna().reset_index(drop=True)

# --- Upload to Hopsworks (v2) ---
fg = fs.get_or_create_feature_group(
    name="karachi_aqi_features",
    version=2,
    primary_key=["time"],
    description="Engineered features with calendar-day AQI targets",
    event_time="time"
)

fg.insert(df, write_options={"wait_for_job": True})
print("✅ Feature group v2 successfully created and populated.")
