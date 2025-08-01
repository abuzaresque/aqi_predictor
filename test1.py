import pandas as pd
import hopsworks
import os
from dotenv import load_dotenv

# --- Load environment ---
load_dotenv()
api_key = os.getenv("HOPSWORKS_API_KEY")

# --- Load raw CSV data ---
CSV_FILE = "karachi_aqi.csv"  # rename if yours is different
df = pd.read_csv(CSV_FILE)
df["time"] = pd.to_datetime(df["time"])

# --- Connect to Hopsworks ---
project = hopsworks.login(api_key_value=api_key)
fs = project.get_feature_store()

# --- Create or get 'karachi_aqi_raw' feature group ---
fg = fs.get_or_create_feature_group(
    name="karachi_aqi_raw",
    version=1,
    description="Raw hourly AQI + weather data for Karachi (backfilled)",
    primary_key=["time"],
    event_time="time"
)

# --- Upload to Hopsworks ---
fg.insert(df, write_options={"wait_for_job": True})
print(f"✅ Backfill inserted into Hopsworks feature group 'karachi_aqi_raw'")
