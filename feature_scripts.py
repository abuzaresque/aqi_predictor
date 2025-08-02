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
import hopsworks
import os
from dotenv import load_dotenv

# --- Load API Key ---
load_dotenv()
api_key = os.getenv("HOPSWORKS_API_KEY")

# --- Connect to Hopsworks & Read Raw Data ---
project = hopsworks.login(api_key_value=api_key)
fs = project.get_feature_store()
fg_raw = fs.get_feature_group("karachi_aqi_raw", version=1)
df = fg_raw.read()

# --- Preprocessing ---
df["time"] = pd.to_datetime(df["time"])
df = df.sort_values("time").reset_index(drop=True)

# Time-based features
df["hour"] = df["time"].dt.hour
df["day"] = df["time"].dt.day
df["month"] = df["time"].dt.month
df["weekday"] = df["time"].dt.weekday
df["is_weekend"] = df["weekday"].apply(lambda x: 1 if x >= 5 else 0)
df["date"] = df["time"].dt.date  # Important for grouping

# Lag features
for col in ["pm2_5", "pm10", "temperature"]:
    df[f"{col}_lag1"] = df[col].shift(1)
    df[f"{col}_lag3"] = df[col].shift(3)

# Rolling statistics
for col in ["pm2_5", "temperature"]:
    df[f"{col}_roll_mean_3"] = df[col].rolling(window=3).mean()
    df[f"{col}_roll_std_6"] = df[col].rolling(window=6).std()

# Derived features
df["pm_ratio"] = df["pm2_5"] / (df["pm10"] + 1e-3)
df["temp_humidity_index"] = df["temperature"] * df["humidity"]

# --- ✅ Compute calendar-day PM2.5 average targets ---
daily_avg = df.groupby("date")["pm2_5"].mean().sort_index()

future_avg = pd.DataFrame({
    "target_pm2_5_avg_day1": daily_avg.shift(-1),
    "target_pm2_5_avg_day2": daily_avg.shift(-2),
    "target_pm2_5_avg_day3": daily_avg.shift(-3)
})

df = df.merge(future_avg, left_on="date", right_index=True)
df = df.dropna().reset_index(drop=True)

# --- Upload to Feature Store ---
fg = fs.get_or_create_feature_group(
    name="karachi_aqi_features",
    version=1,
    primary_key=["time"],
    description="Final features with correct average PM2.5 targets",
    event_time="time"
)

fg.insert(df, write_options={"wait_for_job": True})
print("✅ Features with calendar-day targets uploaded to Hopsworks.")
