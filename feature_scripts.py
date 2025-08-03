import pandas as pd
import numpy as np
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

# Step 1: Daily avg
daily_pm = df.groupby("date")["pm2_5"].mean().reset_index()
daily_pm.columns = ["date", "avg_pm2_5"]

# Step 2: Shift to get future targets
daily_pm["target_pm2_5_avg_day1"] = daily_pm["avg_pm2_5"].shift(-1)
daily_pm["target_pm2_5_avg_day2"] = daily_pm["avg_pm2_5"].shift(-2)
daily_pm["target_pm2_5_avg_day3"] = daily_pm["avg_pm2_5"].shift(-3)

# Step 3: Merge with hourly df
df = df.merge(
    daily_pm[["date", "target_pm2_5_avg_day1", "target_pm2_5_avg_day2", "target_pm2_5_avg_day3"]],
    on="date",
    how="left"
)

# Only drop rows where features are NaN (NOT target NaNs)
df = df.dropna(subset=[
    'carbon_monoxide', 'cloud_coverage', 'day', 'hour', 'humidity', 'is_weekend',
    'month', 'nitrogen_dioxide', 'ozone', 'pm_ratio', 'pm10', 'pm10_lag1', 'pm10_lag3',
    'pm2_5', 'pm2_5_lag1', 'pm2_5_lag3', 'pm2_5_roll_mean_3', 'pm2_5_roll_std_6',
    'pressure', 'temp_humidity_index', 'temperature', 'temperature_lag1', 'temperature_lag3',
    'temperature_roll_mean_3', 'temperature_roll_std_6', 'weekday', 'wind_deg', 'wind_speed'
]).reset_index(drop=True)
print(df[["time", "pm2_5"]].tail(60))

# --- Upload to Hopsworks ---
fg = fs.get_or_create_feature_group(
    name="karachi_aqi_features",
    version=2,
    primary_key=["time"],
    description="Includes recent data even if future targets are missing",
    event_time="time"
)

fg.insert(df, write_options={"wait_for_job": True})
print("✅ Feature group v2 created and populated.")
