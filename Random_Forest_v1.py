import hopsworks
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
import joblib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
from dotenv import load_dotenv

# --- Load API key ---
load_dotenv()
api_key = os.getenv("HOPSWORKS_API_KEY")

# --- AQI formula (NEQS) ---
def calculate_neqs_aqi_pm25(pm):
    breakpoints = [
        (0.0, 17.5, 0, 50),
        (17.6, 35.0, 51, 100),
        (35.1, 52.5, 101, 150),
        (52.6, 70.0, 151, 200),
        (70.1, 105.0, 201, 300),
        (105.1, 175.0, 301, 500)
    ]
    for c_lo, c_hi, i_lo, i_hi in breakpoints:
        if c_lo <= pm <= c_hi:
            return round(((i_hi - i_lo) / (c_hi - c_lo)) * (pm - c_lo) + i_lo)
    return None

# --- Connect to Hopsworks ---
project = hopsworks.login(api_key_value=api_key)
fs = project.get_feature_store()
fg = fs.get_feature_group("karachi_aqi_features", version=2)

# --- Load full dataset for training ---
df = fg.read()
df = df.sort_values("time").reset_index(drop=True)

# --- Feature columns ---
feature_cols = [
    'carbon_monoxide', 'cloud_coverage', 'day', 'hour', 'humidity', 'is_weekend',
    'month', 'nitrogen_dioxide', 'ozone', 'pm_ratio', 'pm10', 'pm10_lag1', 'pm10_lag3',
    'pm2_5', 'pm2_5_lag1', 'pm2_5_lag3', 'pm2_5_roll_mean_3', 'pm2_5_roll_std_6',
    'pressure', 'temp_humidity_index', 'temperature', 'temperature_lag1', 'temperature_lag3',
    'temperature_roll_mean_3', 'temperature_roll_std_6', 'weekday', 'wind_deg', 'wind_speed'
]

target_cols = {
    "day1": "target_pm2_5_avg_day1",
    "day2": "target_pm2_5_avg_day2",
    "day3": "target_pm2_5_avg_day3"
}

# --- Train and save models ---
param_grid = {
    "n_estimators": [100, 200, 300],          # Try deeper forests
    "max_depth": [5, 10, 15, None],           # Allow unlimited depth too
    "min_samples_leaf": [1, 2, 4],            # Try more flexible splits
    "max_features": ["sqrt", "log2", None],   # Let model explore more features
    "min_samples_split": [2, 5, 10]           # Control how splits are made
}

models = {}

# Only use rows where all 3 targets are present
df_train = df.dropna(subset=list(target_cols.values()))
X = df_train[feature_cols]

for name, target in target_cols.items():
    y = df_train[target]
    split = int(0.8 * len(df_train))
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    grid = GridSearchCV(RandomForestRegressor(random_state=42), param_grid, cv=3,
                        scoring="neg_root_mean_squared_error", n_jobs=-1, verbose=0)
    grid.fit(X_train, y_train)
    best_model = grid.best_estimator_

    models[name] = best_model
    joblib.dump(best_model, f"models/pm2_5_model_{name}_v2.pkl")
    print(f"✅ Trained model for {name}: Best Params = {grid.best_params_}")

# --- Predict Next 3 Days using today's latest feature row ---
today = datetime.now(ZoneInfo("Asia/Karachi")).date()

# Read online feature store
df_online = fg.read(read_options={"online": True})
df_online["time"] = pd.to_datetime(df_online["time"])
df_online["date"] = df_online["time"].dt.date


df_today = df_online[df_online["date"] == today]
if df_today.empty:
    raise ValueError(f"No feature data available for today ({today}) in online store.")

# Use latest hour from today
latest_input = df_today.sort_values("time")[feature_cols].iloc[[-1]]

print("\n📆 Predicted AQI for next 3 days:")
for i, name in enumerate(["day1", "day2", "day3"], 1):
    model = models[name]
    pred_pm25 = model.predict(latest_input)[0]
    pred_aqi = calculate_neqs_aqi_pm25(pred_pm25)
    future_date = today + timedelta(days=i)
    print(f"Day +{i} ({future_date}): PM2.5 = {pred_pm25:.2f} → AQI = {pred_aqi}")

# --- Print actual AQI for previous 3 recorded days ---
print("\n📊 Actual AQI for previous 3 recorded days:")
df["date"] = df["time"].dt.floor("D")
daily_summary = df.groupby("date")["pm2_5"].mean().dropna().sort_index()

# 🔧 Fix index type for safe comparison
daily_summary.index = pd.to_datetime(daily_summary.index).date

for offset in range(3, 0, -1):
    day = today - timedelta(days=offset)
    if day in daily_summary.index:
        avg_pm = daily_summary[day]
        aqi = calculate_neqs_aqi_pm25(avg_pm)
        print(f"Day -{offset} ({day}): PM2.5 = {avg_pm:.2f} → AQI = {aqi}")
    else:
        print(f"Day -{offset} ({day}): No data")

print("\n✅ Prediction complete and models saved.")
