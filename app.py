import streamlit as st
import hopsworks
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()

# --- Cache Hopsworks project connection ---
@st.cache_resource
def get_hopsworks_project():
    api_key = os.getenv("HOPSWORKS_API_KEY")
    project = hopsworks.login(api_key_value=api_key)
    return project

project = get_hopsworks_project()
ms = project.get_model_serving()
st.success("Connected to Hopsworks!")

# --- NEQS AQI function ---
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

# --- UI ---
st.title("AQI Predictor – Next 3 Days")

model_choice = st.selectbox(
    "Choose model:",
    ["LSTM (3 days)", "Random Forest"]
)

feature_cols = [
    'carbon_monoxide', 'cloud_coverage', 'day', 'hour', 'humidity', 'is_weekend',
    'month', 'nitrogen_dioxide', 'ozone', 'pm_ratio', 'pm10', 'pm10_lag1', 'pm10_lag3',
    'pm2_5', 'pm2_5_lag1', 'pm2_5_lag3', 'pm2_5_roll_mean_3', 'pm2_5_roll_std_6',
    'pressure', 'temp_humidity_index', 'temperature', 'temperature_lag1', 'temperature_lag3',
    'temperature_roll_mean_3', 'temperature_roll_std_6', 'weekday', 'wind_deg', 'wind_speed'
]

if st.button("Predict AQI"):
    try:
        today = datetime.now(ZoneInfo("Asia/Karachi")).date()

        if model_choice.startswith("LSTM"):
            # Ensure RF deployments are stopped
            try:
                ms.get_deployment("randomforestday1").stop()
                ms.get_deployment("randomforestday2").stop()
                ms.get_deployment("randomforestday3").stop()
            except:
                pass  # already stopped

            deployment = ms.get_deployment("lstm3daypm25predictorv2")
            deployment.start()

            dummy_input = [[0] * len(feature_cols)]
            predictions = deployment.predict({"instances": dummy_input})
            st.write("Raw predictions:", predictions)

            pred_values = predictions["predictions"]
            pm2_5_pred = [
                pred_values["pm2_5_day1"],
                pred_values["pm2_5_day2"],
                pred_values["pm2_5_day3"]
            ]

        elif model_choice.startswith("Random"):
            pm2_5_pred = []

            try:
                ms.get_deployment("lstm3daypm25predictorv2").stop()
            except:
                pass  # already stopped

            for i in range(1, 4):
                deployment_name = f"randomforestday{i}"
                deployment = ms.get_deployment(deployment_name)
                deployment.start()

                dummy_input = [[0] * len(feature_cols)]
                predictions = deployment.predict({"instances": dummy_input})
                st.write(f"Raw predictions (Day {i}):", predictions)

                pred_value = predictions["predictions"]
                pm2_5_pred.append(pred_value[f"pm2_5_day{i}"])

        # --- Process predictions ---
        aqi_pred = [calculate_neqs_aqi_pm25(pm) for pm in pm2_5_pred]
        days = [today + timedelta(days=i+1) for i in range(len(pm2_5_pred))]

        st.subheader("Predicted PM2.5 & AQI")
        for i, day in enumerate(days):
            st.write(f"{day}: PM2.5 = {pm2_5_pred[i]:.2f}, AQI = {aqi_pred[i]}")

        # --- Plot ---
        fig, ax1 = plt.subplots()
        ax1.plot(days, pm2_5_pred, 'b-o', label='PM2.5')
        ax1.set_xlabel("Date")
        ax1.set_ylabel("PM2.5", color='b')
        ax2 = ax1.twinx()
        ax2.plot(days, aqi_pred, 'r--o', label='AQI')
        ax2.set_ylabel("AQI", color='r')
        fig.tight_layout()
        st.pyplot(fig)

    except Exception as e:
        st.error(f"Prediction failed: {e}")
