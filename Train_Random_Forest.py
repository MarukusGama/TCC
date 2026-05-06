import os
import joblib
import numpy as np
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

load_dotenv()

# configuração do banco de dados (retirado para colocar no github)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

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
WHERE is_valid = TRUE
  AND rpm IS NOT NULL
  AND speed IS NOT NULL
  AND coolant_temp IS NOT NULL
  AND engine_load IS NOT NULL
ORDER BY vehicle_id, date_time;
"""

df = pd.read_sql(QUERY, engine)

if df.empty:
    raise Exception("Nenhum dado encontrado para treinamento.")

df["date_time"] = pd.to_datetime(df["date_time"])
df = df.sort_values(["vehicle_id", "date_time"])

# Variáveis que serão analisadas
sensor_cols = [
    "rpm",
    "speed",
    "coolant_temp",
    "engine_load",
]

# Criação de variáveis temporais
df["hour"] = df["date_time"].dt.hour
df["dayofweek"] = df["date_time"].dt.dayofweek

# Criação de variáveis defasadas e médias móveis por veículo
for col in sensor_cols:
    df[f"{col}_lag1"] = df.groupby("vehicle_id")[col].shift(1)
    df[f"{col}_lag2"] = df.groupby("vehicle_id")[col].shift(2)
    df[f"{col}_rolling_mean_5"] = (
        df.groupby("vehicle_id")[col]
        .rolling(window=5, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

df = df.dropna()

feature_cols = []

for col in sensor_cols:
    feature_cols.append(f"{col}_lag1")
    feature_cols.append(f"{col}_lag2")
    feature_cols.append(f"{col}_rolling_mean_5")

feature_cols += [
    "hour",
    "dayofweek",
    "latitude",
    "longitude"
]

X = df[feature_cols]
y = df[sensor_cols]

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    shuffle=False
)

model = RandomForestRegressor(
    n_estimators=200,
    max_depth=15,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train, y_train)

y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred, multioutput="raw_values")

print("Erro médio absoluto por variável:")
for col, error in zip(sensor_cols, mae):
    print(f"{col}: {error:.2f}")

# Cálculo dos resíduos no conjunto de treino
train_pred = model.predict(X_train)
residuals = np.abs(y_train.values - train_pred)

# Limiar de anomalia por variável
# Percentil 99,5%: valores acima disso serão considerados incomuns
thresholds = np.quantile(residuals, 0.995, axis=0)

threshold_dict = {
    col: float(threshold)
    for col, threshold in zip(sensor_cols, thresholds)
}

print("\nLimiar de anomalia por variável:")
for col, threshold in threshold_dict.items():
    print(f"{col}: {threshold:.2f}")

joblib.dump(model, "random_forest_vehicle_model.pkl")
joblib.dump(feature_cols, "feature_cols.pkl")
joblib.dump(sensor_cols, "sensor_cols.pkl")
joblib.dump(threshold_dict, "thresholds.pkl")

print("\nModelo Random Forest treinado e salvo com sucesso.")