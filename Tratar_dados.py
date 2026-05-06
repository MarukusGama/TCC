import os
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =========================
# CONFIGURAÇÕES
# =========================

load_dotenv()

# configuração do banco de dados (retirado para colocar no github)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

# =========================
# LIMITES DE VALIDAÇÃO
# =========================

LIMITS = {
    "rpm_min": 0,
    "rpm_max": 9000,

    "speed_min": 0,
    "speed_max": 250,

    "coolant_temp_min": -40,
    "coolant_temp_max": 140,

    "engine_load_min": 0,
    "engine_load_max": 100,

    "latitude_min": -90,
    "latitude_max": 90,

    "longitude_min": -180,
    "longitude_max": 180
}


# =========================
# FUNÇÕES
# =========================

def get_invalid_reason(row):
    reasons = []

    required_fields = [
        "device_id",
        "date_time",
        "rpm",
        "speed",
        "coolant_temp",
        "engine_load",
        "latitude",
        "longitude"
    ]

    for field in required_fields:
        if pd.isna(row[field]):
            reasons.append(f"Campo obrigatório ausente: {field}")

    if reasons:
        return "; ".join(reasons)

    if row["rpm"] < LIMITS["rpm_min"] or row["rpm"] > LIMITS["rpm_max"]:
        reasons.append(f"RPM fora da faixa: {row['rpm']}")

    if row["speed"] < LIMITS["speed_min"] or row["speed"] > LIMITS["speed_max"]:
        reasons.append(f"Velocidade fora da faixa: {row['speed']}")

    if row["coolant_temp"] < LIMITS["coolant_temp_min"] or row["coolant_temp"] > LIMITS["coolant_temp_max"]:
        reasons.append(f"Temperatura do motor fora da faixa: {row['coolant_temp']}")

    if row["engine_load"] < LIMITS["engine_load_min"] or row["engine_load"] > LIMITS["engine_load_max"]:
        reasons.append(f"Carga do motor fora da faixa: {row['engine_load']}")

    if row["latitude"] < LIMITS["latitude_min"] or row["latitude"] > LIMITS["latitude_max"]:
        reasons.append(f"Latitude inválida: {row['latitude']}")

    if row["longitude"] < LIMITS["longitude_min"] or row["longitude"] > LIMITS["longitude_max"]:
        reasons.append(f"Longitude inválida: {row['longitude']}")

    return "; ".join(reasons) if reasons else None


def load_data():
    query = """
        SELECT
            id,
            device_id,
            date_time,
            rpm,
            speed,
            coolant_temp,
            engine_load,
            latitude,
            longitude,
            received_at
        FROM vehicle_telemetry
        ORDER BY device_id, date_time, id;
    """

    return pd.read_sql(query, engine)


def create_invalid_table():
    query = """
        CREATE TABLE IF NOT EXISTS vehicle_telemetry_invalid (
            id BIGSERIAL PRIMARY KEY,
            original_id BIGINT,
            device_id VARCHAR(50),
            date_time TIMESTAMP,
            rpm INTEGER,
            speed INTEGER,
            coolant_temp INTEGER,
            engine_load INTEGER,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            received_at TIMESTAMP,
            invalid_reason TEXT,
            removed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """

    with engine.begin() as conn:
        conn.execute(text(query))


def save_invalid_records(invalid_df):
    if invalid_df.empty:
        return

    insert_query = text("""
        INSERT INTO vehicle_telemetry_invalid (
            original_id,
            device_id,
            date_time,
            rpm,
            speed,
            coolant_temp,
            engine_load,
            latitude,
            longitude,
            received_at,
            invalid_reason
        ) VALUES (
            :original_id,
            :device_id,
            :date_time,
            :rpm,
            :speed,
            :coolant_temp,
            :engine_load,
            :latitude,
            :longitude,
            :received_at,
            :invalid_reason
        );
    """)

    with engine.begin() as conn:
        for _, row in invalid_df.iterrows():
            conn.execute(
                insert_query,
                {
                    "original_id": int(row["id"]),
                    "device_id": row["device_id"],
                    "date_time": row["date_time"],
                    "rpm": None if pd.isna(row["rpm"]) else int(row["rpm"]),
                    "speed": None if pd.isna(row["speed"]) else int(row["speed"]),
                    "coolant_temp": None if pd.isna(row["coolant_temp"]) else int(row["coolant_temp"]),
                    "engine_load": None if pd.isna(row["engine_load"]) else int(row["engine_load"]),
                    "latitude": None if pd.isna(row["latitude"]) else float(row["latitude"]),
                    "longitude": None if pd.isna(row["longitude"]) else float(row["longitude"]),
                    "received_at": row["received_at"],
                    "invalid_reason": row["invalid_reason"]
                }
            )


def delete_invalid_records(invalid_ids):
    if not invalid_ids:
        return

    delete_query = text("""
        DELETE FROM vehicle_telemetry
        WHERE id = ANY(:ids);
    """)

    with engine.begin() as conn:
        conn.execute(delete_query, {"ids": invalid_ids})


def remove_duplicates(df):
    duplicate_df = df[
        df.duplicated(
            subset=[
                "device_id",
                "date_time",
                "rpm",
                "speed",
                "coolant_temp",
                "engine_load",
                "latitude",
                "longitude"
            ],
            keep="first"
        )
    ].copy()

    if duplicate_df.empty:
        return []

    duplicate_df["invalid_reason"] = "Registro duplicado"

    save_invalid_records(duplicate_df)

    return duplicate_df["id"].astype(int).tolist()


def main():
    print("Iniciando tratamento dos dados...")

    create_invalid_table()

    df = load_data()

    if df.empty:
        print("Nenhum registro encontrado.")
        return

    print(f"Total de registros carregados: {len(df)}")

    df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")

    # Identifica registros inválidos
    df["invalid_reason"] = df.apply(get_invalid_reason, axis=1)

    invalid_df = df[df["invalid_reason"].notna()].copy()

    invalid_ids = invalid_df["id"].astype(int).tolist()

    print(f"Registros inválidos encontrados: {len(invalid_ids)}")

    # Salva inválidos na tabela de auditoria
    save_invalid_records(invalid_df)

    # Remove inválidos da tabela principal
    delete_invalid_records(invalid_ids)

    # Recarrega dados após primeira limpeza
    df_clean = load_data()

    # Remove duplicados
    duplicate_ids = remove_duplicates(df_clean)

    print(f"Registros duplicados encontrados: {len(duplicate_ids)}")

    delete_invalid_records(duplicate_ids)

    print("Tratamento concluído.")
    print(f"Total removido: {len(invalid_ids) + len(duplicate_ids)}")


if __name__ == "__main__":
    main()