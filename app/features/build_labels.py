from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.engine import URL


# =========================================================
# CONFIG
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "artifacts"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LABELS_FILE = OUTPUT_DIR / "labels.npy"
LABELS_CSV = OUTPUT_DIR / "labels_preview.csv"

# Either provide DATABASE_URL directly,
# or provide DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
DATABASE_URL = os.getenv("DATABASE_URL")

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

EXPLICIT_TABLE = os.getenv("MAKFLEET_EVENTS_TABLE")


# =========================================================
# DB
# =========================================================

def get_engine() -> Engine:
    if DATABASE_URL:
        return create_engine(DATABASE_URL, future=True)

    if not DB_PASSWORD:
        raise ValueError(
            "No database password found.\n"
            "Set DB_PASSWORD in PowerShell, for example:\n"
            '$env:DB_PASSWORD="your_real_password"'
        )

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME,
    )
    return create_engine(url, future=True)


def list_tables(engine: Engine) -> list[str]:
    inspector = inspect(engine)
    return inspector.get_table_names()


def get_table_columns(engine: Engine, table_name: str) -> list[str]:
    inspector = inspect(engine)
    return [col["name"] for col in inspector.get_columns(table_name)]


def choose_best_table(engine: Engine, explicit_table: Optional[str] = None) -> str:
    tables = list_tables(engine)

    if not tables:
        raise ValueError(
            "No tables were found in the connected PostgreSQL database."
        )

    print("Available tables:", tables)

    if explicit_table:
        if explicit_table in tables:
            print(f"Using table from MAKFLEET_EVENTS_TABLE: {explicit_table}")
            return explicit_table
        raise ValueError(f"Table '{explicit_table}' not found. Available tables: {tables}")

    likely_names = [
        "telemetry",
        "events",
        "telemetry_events",
        "vehicle_events",
        "vehicle_telemetry",
        "fleet_events",
        "sensor_data",
        "raw_telemetry",
        "trip_events",
        "trips",
    ]

    for name in likely_names:
        if name in tables:
            print(f"Auto-selected table by name: {name}")
            return name

    best_table = None
    best_score = -1

    for table in tables:
        cols = [c.lower() for c in get_table_columns(engine, table)]
        score = 0

        if any(c in cols for c in ["sensor_ts", "timestamp", "event_time", "created_at", "ts", "time"]):
            score += 5
        if any(c in cols for c in ["vehicle_id", "vehicle", "car_id", "unit_id", "asset_id"]):
            score += 3
        if any(c in cols for c in ["speed", "speed_kmh", "event_type", "is_anomaly", "lat", "lon", "latitude", "longitude"]):
            score += 2
        if any(word in table.lower() for word in ["telemetry", "event", "trip", "fleet", "sensor"]):
            score += 2

        if score > best_score:
            best_score = score
            best_table = table

    if best_table is None:
        raise ValueError(f"Could not determine the best source table. Available tables: {tables}")

    print(f"Auto-selected table by score: {best_table}")
    return best_table


def load_table(engine: Engine, table_name: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql_table(table_name, conn)


# =========================================================
# COLUMN DETECTION
# =========================================================

def find_timestamp_column(df: pd.DataFrame) -> str:
    candidates = [
        "sensor_ts",
        "timestamp",
        "event_time",
        "telemetry_ts",
        "created_at",
        "ts",
        "time",
    ]
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    raise KeyError(f"No timestamp column found. Available columns: {df.columns.tolist()}")


def find_vehicle_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "vehicle_id",
        "vehicle",
        "car_id",
        "unit_id",
        "asset_id",
        "driver_vehicle_id",
    ]
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def find_speed_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "speed",
        "speed_kmh",
        "vehicle_speed",
        "velocity",
        "kmh",
    ]
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


# =========================================================
# CLEANING
# =========================================================

def clean_events(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("The selected source table is empty.")

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    ts_col = find_timestamp_column(df)

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()

    df = df[df[ts_col].astype(str).str.lower() != ts_col.lower()].copy()
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df.dropna(subset=[ts_col]).copy()

    if df.empty:
        raise ValueError("After timestamp cleaning, no valid rows remained.")

    vehicle_col = find_vehicle_column(df)
    if vehicle_col:
        df = df.sort_values([vehicle_col, ts_col]).reset_index(drop=True)
    else:
        df = df.sort_values([ts_col]).reset_index(drop=True)

    return df


# =========================================================
# LABELS
# =========================================================

def create_labels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    lower_map = {col.lower(): col for col in df.columns}

    if "is_anomaly" in lower_map:
        col = lower_map["is_anomaly"]
        df["label"] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        df["label"] = df["label"].clip(0, 1)
        print("Labels built from is_anomaly column.")
        return df

    if "event_type" in lower_map:
        col = lower_map["event_type"]
        suspicious = {
            "harsh_braking",
            "overspeed",
            "collision",
            "geofence_violation",
            "engine_fault",
            "panic",
            "accident",
            "tamper",
            "unsafe",
            "anomaly",
            "alert",
        }
        df["label"] = df[col].astype(str).str.lower().isin(suspicious).astype(int)
        print("Labels built from event_type column.")
        return df

    speed_col = find_speed_column(df)
    if speed_col:
        speed = pd.to_numeric(df[speed_col], errors="coerce")
        threshold = speed.quantile(0.95)

        if pd.isna(threshold):
            df["label"] = 0
        else:
            df["label"] = (speed > threshold).fillna(False).astype(int)

        print(f"Labels built from speed threshold: {threshold}")
        return df

    df["label"] = 0
    print("No anomaly-related columns found. All labels set to 0.")
    return df


def prepare_output(df: pd.DataFrame) -> pd.DataFrame:
    ts_col = find_timestamp_column(df)
    vehicle_col = find_vehicle_column(df)

    cols = []
    if vehicle_col:
        cols.append(vehicle_col)
    cols.append(ts_col)
    cols.append("label")

    return df[cols].copy()


def save_outputs(labels_df: pd.DataFrame) -> None:
    labels = labels_df["label"].to_numpy(dtype=np.int64)
    np.save(LABELS_FILE, labels)
    labels_df.to_csv(LABELS_CSV, index=False)

    print(f"Saved labels to: {LABELS_FILE}")
    print(f"Saved preview CSV to: {LABELS_CSV}")
    print(f"Labels shape: {labels.shape}")
    print("Label distribution:")
    print(labels_df["label"].value_counts(dropna=False).sort_index())


# =========================================================
# MAIN
# =========================================================

def build_labels() -> pd.DataFrame:
    print("Connecting to PostgreSQL...")
    engine = get_engine()
    print(f"Using database: {engine.url.render_as_string(hide_password=True)}")

    table_name = choose_best_table(engine, EXPLICIT_TABLE)

    print(f"Loading data from table: {table_name}")
    df = load_table(engine, table_name)

    print(f"Loaded rows: {len(df)}")
    print("Columns:", df.columns.tolist())

    print("Cleaning data...")
    df = clean_events(df)
    print(f"Rows after cleaning: {len(df)}")

    print("Creating labels...")
    df = create_labels(df)

    labels_df = prepare_output(df)

    print("Preview:")
    print(labels_df.head(10))

    save_outputs(labels_df)
    return labels_df


if __name__ == "__main__":
    build_labels()