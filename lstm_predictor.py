import os
import joblib
import tensorflow as tf
import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime

class Predictor:
    def __init__(self, project, deployment, model):
        self.project = project
        self.deployment = deployment
        self.model_meta = model

        # Download model artifacts
        artifacts_path = model.download()
        print(artifacts_path)

        # Load model + scalers + feature columns
        self.model_obj = tf.keras.models.load_model(
            os.path.join(artifacts_path, "LSTM_3days.h5"),
            compile=False
        )
        self.x_scaler = joblib.load(os.path.join(artifacts_path, "x_scaler.pkl"))
        self.y_scaler = joblib.load(os.path.join(artifacts_path, "y_scaler.pkl"))
        self.feature_cols = [
            'carbon_monoxide', 'cloud_coverage', 'day', 'hour', 'humidity', 'is_weekend',
            'month', 'nitrogen_dioxide', 'ozone', 'pm_ratio', 'pm10', 'pm10_lag1', 'pm10_lag3',
            'pm2_5', 'pm2_5_lag1', 'pm2_5_lag3', 'pm2_5_roll_mean_3', 'pm2_5_roll_std_6',
            'pressure', 'temp_humidity_index', 'temperature', 'temperature_lag1', 'temperature_lag3',
            'temperature_roll_mean_3', 'temperature_roll_std_6', 'weekday', 'wind_deg', 'wind_speed'
        ]

        # Load feature store and feature group
        self.fs = project.get_feature_store()
        self.feature_group = self.fs.get_feature_group(name="karachi_aqi_features", version=2)

    def predict(self, x=None):
        df = self.feature_group.read()
        df["time"] = pd.to_datetime(df["time"])
        df = df.dropna(subset=self.feature_cols)
        df = df.sort_values("time", ascending=True)
        
        today = datetime.now(ZoneInfo("Asia/Karachi")).date()
        df_today = df[df["time"].dt.date == today]

        if df_today.empty:
            raise ValueError(f"No valid feature row for today ({today})")

        row = df_today[self.feature_cols].iloc[[-1]]

        X_scaled = self.x_scaler.transform(row.values)
        X_reshaped = X_scaled.reshape((X_scaled.shape[0], 1, X_scaled.shape[1]))

        y_pred_scaled = self.model_obj.predict(X_reshaped)
        y_pred = self.y_scaler.inverse_transform(y_pred_scaled)

        return {
            "pm2_5_day1": float(y_pred[0, 0]),
            "pm2_5_day2": float(y_pred[0, 1]),
            "pm2_5_day3": float(y_pred[0, 2])
        }
