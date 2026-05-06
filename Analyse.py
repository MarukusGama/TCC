import os
import joblib
import numpy as np
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# configuração do banco de dados (retirado para colocar no github)
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

model = joblib.load("random_forest_vehicle_model.pkl")
feature_cols = joblib.load("feature_cols.pkl")
sensor_cols = joblib.load("sensor_cols.pkl")
thresholds = joblib.load("thresholds.pkl")

QUERY = """
SELECT
    id,
    vehicle_id,
    date_time,
    rpm,
    speed,
    coolant_temp,
    engine_load,
    latitude,
    longitude
FROM vehicle_telemetry
ORDER BY vehicle_id, date_time;
"""

df = pd.read_sql(QUERY, engine)

if df.empty:
    print("Nenhum dado disponível.")
    exit()

df["date_time"] = pd.to_datetime(df["date_time"])
df = df.sort_values(["device_id", "date_time"])

df["hour"] = df["date_time"].dt.hour
df["dayofweek"] = df["date_time"].dt.dayofweek

for col in sensor_cols:
    df[f"{col}_lag1"] = df.groupby("device_id")[col].shift(1)
    df[f"{col}_lag2"] = df.groupby("device_id")[col].shift(2)
    df[f"{col}_rolling_mean_5"] = (
        df.groupby("device_id")[col]
        .rolling(window=5, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

df = df.dropna()

# Analisa apenas os últimos registros de cada veículo
latest = df.groupby("device_id").tail(1)

for _, row in latest.iterrows():
    X = row[feature_cols].to_frame().T
    real_values = row[sensor_cols].values.astype(float)

    predicted_values = model.predict(X)[0]
    residuals = np.abs(real_values - predicted_values)

    anomalies = []

    for i, col in enumerate(sensor_cols):
        threshold = thresholds[col]
        residual = residuals[i]

        if residual > threshold:
            anomalies.append({
                "variable": col,
                "real": real_values[i],
                "predicted": predicted_values[i],
                "residual": residual,
                "threshold": threshold
            })

    if anomalies:
        score = max(
            anomaly["residual"] / anomaly["threshold"]
            for anomaly in anomalies
        )

        messages = []

        for anomaly in anomalies:
            messages.append(
                f"{anomaly['variable']} anormal: "
                f"valor real={anomaly['real']:.2f}, "
                f"valor esperado={anomaly['predicted']:.2f}, "
                f"desvio={anomaly['residual']:.2f}"
            )

        alert_message = " | ".join(messages)

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO vehicle_alerts (
                        telemetry_id,
                        device_id,
                        alert_type,
                        alert_message,
                        anomaly_score
                    ) VALUES (
                        :telemetry_id,
                        :device_id,
                        :alert_type,
                        :alert_message,
                        :anomaly_score
                    )
                """),
                {
                    "telemetry_id": int(row["id"]),
                    "device_id": row["device_id"],
                    "alert_type": "ANOMALIA_RANDOM_FOREST",
                    "alert_message": alert_message,
                    "anomaly_score": float(score)
                }
            )

        print(f"ALERTA GERADO PARA {row['device_id']}: {alert_message}")

    else:
        print(f"Nenhuma anomalia detectada para {row['device_id']}.")