from __future__ import annotations

import os
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

SPEED_ALERT_THRESHOLD = float(os.getenv("SPEED_ALERT_THRESHOLD", "43"))
IDLE_SPEED_THRESHOLD = float(os.getenv("IDLE_SPEED_THRESHOLD", "2"))
IDLE_ALERT_MINUTES = int(os.getenv("IDLE_ALERT_MINUTES", "3"))
STALE_ALERT_MINUTES = int(os.getenv("STALE_ALERT_MINUTES", "5"))

if not DB_PASSWORD:
    raise ValueError("DB_PASSWORD is not set.")

DATABASE_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=int(DB_PORT),
    database=DB_NAME,
)

engine = create_engine(DATABASE_URL, future=True)

app = FastAPI(title="MakFleet API", version="1.4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

RAW_TABLE = "raw_telemetry"
ENRICHED_TABLE = "enriched_events"


def read_sql_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def get_columns(table_name: str) -> set[str]:
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


RAW_COLS = get_columns(RAW_TABLE)
ENRICHED_COLS = get_columns(ENRICHED_TABLE)

BIKE_COL = "bike_id" if "bike_id" in RAW_COLS else None
DRIVER_COL = "driver_id" if "driver_id" in RAW_COLS else None
TRIP_COL = "trip_id" if "trip_id" in RAW_COLS else None
ENGINE_COL = "engine_state" if "engine_state" in RAW_COLS else None
SPEED_COL = "speed_kmh" if "speed_kmh" in RAW_COLS else None
TS_COL = "sensor_ts" if "sensor_ts" in RAW_COLS else None
LAT_COL = "latitude" if "latitude" in RAW_COLS else None
LON_COL = "longitude" if "longitude" in RAW_COLS else None

REQUIRED = [BIKE_COL, SPEED_COL, TS_COL]
if any(col is None for col in REQUIRED):
    raise RuntimeError(
        f"Missing required columns in {RAW_TABLE}. Found: {sorted(RAW_COLS)}"
    )

ZONE_COL_CANDIDATES = ["zone", "zone_name", "campus_zone", "zone_label", "name"]
ENRICHED_ZONE_COL = next(
    (c for c in ZONE_COL_CANDIDATES if c in ENRICHED_COLS),
    None,
)


def zone_parts(rt_alias: str = "rt", ee_alias: str = "ee") -> tuple[str, str]:
    if ENRICHED_ZONE_COL and "event_id" in RAW_COLS and "event_id" in ENRICHED_COLS:
        zone_select = f"COALESCE({ee_alias}.{ENRICHED_ZONE_COL}::text, 'Unknown') AS zone"
        zone_join = (
            f"LEFT JOIN {ENRICHED_TABLE} {ee_alias} "
            f"ON {rt_alias}.event_id = {ee_alias}.event_id"
        )
        return zone_select, zone_join

    return "'Unknown' AS zone", ""


def status_case(rt_alias: str = "rt") -> str:
    engine_expr = (
        f"UPPER({rt_alias}.{ENGINE_COL}::text) = 'IDLE'"
        if ENGINE_COL
        else "FALSE"
    )
    return f"""
        CASE
            WHEN {rt_alias}.{SPEED_COL} >= :speed_threshold THEN 'Overspeed'
            WHEN {engine_expr} THEN 'Idle'
            WHEN {rt_alias}.{SPEED_COL} >= (:speed_threshold * 0.8) THEN 'Watch'
            ELSE 'Normal'
        END
    """


@app.get("/")
def root():
    return {"message": "MakFleet API is running"}


@app.get("/api/kpis")
def get_kpis():
    engine_idle_expr = (
        f"UPPER(latest.engine) = 'IDLE'"
        if ENGINE_COL
        else "FALSE"
    )

    trip_select = f"rt.{TRIP_COL}::text AS trip," if TRIP_COL else "'N/A' AS trip,"
    engine_select = (
        f"rt.{ENGINE_COL}::text AS engine," if ENGINE_COL else "'UNKNOWN' AS engine,"
    )

    sql = f"""
        WITH latest_per_bike AS (
            SELECT DISTINCT ON (rt.{BIKE_COL})
                rt.{BIKE_COL}::text AS bike,
                {trip_select}
                {engine_select}
                ROUND(rt.{SPEED_COL}::numeric, 1) AS speed,
                rt.{TS_COL} AS ts
            FROM {RAW_TABLE} rt
            ORDER BY rt.{BIKE_COL}, rt.{TS_COL} DESC
        )
        SELECT
            COUNT(*) AS active_bikes,
            SUM(
                CASE
                    WHEN {engine_idle_expr}
                     AND COALESCE(latest.speed, 0) <= :idle_speed_threshold
                    THEN 1 ELSE 0
                END
            ) AS idle_bikes,
            SUM(
                CASE
                    WHEN EXTRACT(EPOCH FROM (NOW() - latest.ts)) / 60.0 >= :stale_alert_minutes
                    THEN 1 ELSE 0
                END
            ) AS stale_bikes,
            SUM(
                CASE
                    WHEN COALESCE(latest.speed, 0) >= :speed_threshold THEN 1
                    WHEN {engine_idle_expr}
                     AND COALESCE(latest.speed, 0) <= :idle_speed_threshold
                     AND EXTRACT(EPOCH FROM (NOW() - latest.ts)) / 60.0 >= :idle_alert_minutes
                    THEN 1
                    WHEN EXTRACT(EPOCH FROM (NOW() - latest.ts)) / 60.0 >= :stale_alert_minutes
                    THEN 1
                    ELSE 0
                END
            ) AS critical_alerts
        FROM latest_per_bike latest
    """

    params = {
        "speed_threshold": SPEED_ALERT_THRESHOLD,
        "idle_speed_threshold": IDLE_SPEED_THRESHOLD,
        "idle_alert_minutes": IDLE_ALERT_MINUTES,
        "stale_alert_minutes": STALE_ALERT_MINUTES,
    }

    df = read_sql_df(sql, params)
    row = df.iloc[0]

    return [
        {
            "label": "Active Bikes Now",
            "value": int(row["active_bikes"] or 0),
            "hint": "Latest reporting bikes",
        },
        {
            "label": "Idle Bikes Now",
            "value": int(row["idle_bikes"] or 0),
            "hint": f"Engine idle and speed ≤ {IDLE_SPEED_THRESHOLD} km/h",
        },
        {
            "label": "Stale Bikes",
            "value": int(row["stale_bikes"] or 0),
            "hint": f"No fresh telemetry in {STALE_ALERT_MINUTES}+ min",
        },
        {
            "label": "Critical Alerts Now",
            "value": int(row["critical_alerts"] or 0),
            "hint": "Overspeed, long idle, or stale",
        },
    ]


@app.get("/api/telemetry")
def get_telemetry(limit: int = 100):
    zone_select, zone_join = zone_parts()

    driver_expr = (
        f"rt.{DRIVER_COL}::text AS driver," if DRIVER_COL else "'N/A' AS driver,"
    )
    trip_expr = f"rt.{TRIP_COL}::text AS trip," if TRIP_COL else "'N/A' AS trip,"
    engine_expr = (
        f"rt.{ENGINE_COL}::text AS engine," if ENGINE_COL else "'UNKNOWN' AS engine,"
    )

    sql = f"""
        SELECT
            rt.{BIKE_COL}::text AS bike,
            {driver_expr}
            {trip_expr}
            ROUND(rt.{SPEED_COL}::numeric, 1) AS speed,
            {engine_expr}
            {zone_select},
            {status_case("rt")} AS status,
            rt.{TS_COL} AS ts
        FROM {RAW_TABLE} rt
        {zone_join}
        ORDER BY rt.{TS_COL} DESC
        LIMIT :limit
    """
    df = read_sql_df(
        sql, {"limit": limit, "speed_threshold": SPEED_ALERT_THRESHOLD}
    )

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return df.fillna("N/A").to_dict(orient="records")


@app.get("/api/alerts")
def get_alerts(limit: int = 20):
    zone_select, zone_join = zone_parts("rt", "ee")

    driver_expr = (
        f"rt.{DRIVER_COL}::text AS driver" if DRIVER_COL else "'N/A' AS driver"
    )
    trip_expr = f"rt.{TRIP_COL}::text AS trip" if TRIP_COL else "'N/A' AS trip"
    engine_expr = (
        f"rt.{ENGINE_COL}::text AS engine" if ENGINE_COL else "'UNKNOWN' AS engine"
    )
    idle_condition = (
        f"UPPER(rt.{ENGINE_COL}::text) = 'IDLE' AND COALESCE(rt.{SPEED_COL}, 0) <= :idle_speed_threshold"
        if ENGINE_COL
        else f"COALESCE(rt.{SPEED_COL}, 0) <= :idle_speed_threshold"
    )

    sql = f"""
        WITH latest_per_bike AS (
            SELECT DISTINCT ON (rt.{BIKE_COL})
                rt.{BIKE_COL}::text AS bike,
                {driver_expr},
                {trip_expr},
                ROUND(rt.{SPEED_COL}::numeric, 1) AS speed,
                {engine_expr},
                {zone_select},
                rt.{TS_COL} AS ts
            FROM {RAW_TABLE} rt
            {zone_join}
            ORDER BY rt.{BIKE_COL}, rt.{TS_COL} DESC
        ),
        overspeed_alerts AS (
            SELECT
                'OS-' || bike AS id,
                'Overspeed' AS type,
                CASE
                    WHEN speed >= (:speed_threshold * 1.20) THEN 'High'
                    ELSE 'Medium'
                END AS severity,
                bike,
                zone AS location,
                ts AS event_time,
                CONCAT(
                    'Bike reached ',
                    speed,
                    ' km/h, exceeding the configured threshold of ',
                    :speed_threshold,
                    ' km/h.'
                ) AS note
            FROM latest_per_bike
            WHERE speed >= :speed_threshold
        ),
        idle_stats AS (
            SELECT
                rt.{BIKE_COL}::text AS bike,
                MIN(rt.{TS_COL}) AS idle_start,
                MAX(rt.{TS_COL}) AS idle_end,
                COUNT(*) AS idle_points
            FROM {RAW_TABLE} rt
            WHERE {idle_condition}
              AND rt.{TS_COL} >= NOW() - (:idle_window_minutes * INTERVAL '1 minute')
            GROUP BY rt.{BIKE_COL}
        ),
        idle_alerts AS (
            SELECT
                'ID-' || lb.bike AS id,
                'Long Idle' AS type,
                CASE
                    WHEN EXTRACT(EPOCH FROM (i.idle_end - i.idle_start)) / 60.0 >= (:idle_alert_minutes * 2)
                        THEN 'High'
                    ELSE 'Medium'
                END AS severity,
                lb.bike,
                lb.zone AS location,
                lb.ts AS event_time,
                CONCAT(
                    'Bike has remained idle for about ',
                    ROUND(EXTRACT(EPOCH FROM (i.idle_end - i.idle_start)) / 60.0, 1),
                    ' minutes.'
                ) AS note
            FROM idle_stats i
            JOIN latest_per_bike lb
              ON lb.bike = i.bike
            WHERE EXTRACT(EPOCH FROM (i.idle_end - i.idle_start)) / 60.0 >= :idle_alert_minutes
              AND UPPER(lb.engine) = 'IDLE'
              AND COALESCE(lb.speed, 0) <= :idle_speed_threshold
        ),
        stale_alerts AS (
            SELECT
                'ST-' || bike AS id,
                'Stale Telemetry' AS type,
                CASE
                    WHEN EXTRACT(EPOCH FROM (NOW() - ts)) / 60.0 >= (:stale_alert_minutes * 2)
                        THEN 'High'
                    ELSE 'Medium'
                END AS severity,
                bike,
                zone AS location,
                ts AS event_time,
                CONCAT(
                    'No fresh telemetry for about ',
                    ROUND(EXTRACT(EPOCH FROM (NOW() - ts)) / 60.0, 1),
                    ' minutes.'
                ) AS note
            FROM latest_per_bike
            WHERE EXTRACT(EPOCH FROM (NOW() - ts)) / 60.0 >= :stale_alert_minutes
        )
        SELECT *
        FROM (
            SELECT * FROM overspeed_alerts
            UNION ALL
            SELECT * FROM idle_alerts
            UNION ALL
            SELECT * FROM stale_alerts
        ) all_alerts
        ORDER BY event_time DESC NULLS LAST
        LIMIT :limit
    """

    params = {
        "limit": limit,
        "speed_threshold": SPEED_ALERT_THRESHOLD,
        "idle_speed_threshold": IDLE_SPEED_THRESHOLD,
        "idle_alert_minutes": IDLE_ALERT_MINUTES,
        "idle_window_minutes": max(IDLE_ALERT_MINUTES * 3, 10),
        "stale_alert_minutes": STALE_ALERT_MINUTES,
    }

    df = read_sql_df(sql, params)

    if df.empty:
        return []

    df["time"] = pd.to_datetime(df["event_time"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    df = df.drop(columns=["event_time"])
    return df.fillna("N/A").to_dict(orient="records")


@app.get("/api/speed-series")
def get_speed_series(limit: int = 20):
    sql = f"""
        SELECT
            {TS_COL} AS time,
            ROUND({SPEED_COL}::numeric, 1) AS speed
        FROM {RAW_TABLE}
        ORDER BY {TS_COL} DESC
        LIMIT :limit
    """
    df = read_sql_df(sql, {"limit": limit})
    df = df.sort_values("time")
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.strftime("%H:%M:%S")
    return df.to_dict(orient="records")


@app.get("/api/zone-traffic")
def get_zone_traffic():
    zone_select, zone_join = zone_parts()

    sql = f"""
        SELECT
            zone,
            COUNT(*) AS trips
        FROM (
            SELECT
                {zone_select}
            FROM {RAW_TABLE} rt
            {zone_join}
        ) z
        GROUP BY zone
        ORDER BY trips DESC
        LIMIT 10
    """
    df = read_sql_df(sql)
    return df.fillna("Unknown").to_dict(orient="records")


@app.get("/api/latest-positions")
def get_latest_positions():
    if not LAT_COL or not LON_COL:
        return []

    zone_select, zone_join = zone_parts()

    driver_expr = (
        f"rt.{DRIVER_COL}::text AS driver," if DRIVER_COL else "'N/A' AS driver,"
    )
    trip_expr = f"rt.{TRIP_COL}::text AS trip," if TRIP_COL else "'N/A' AS trip,"
    engine_expr = (
        f"rt.{ENGINE_COL}::text AS engine," if ENGINE_COL else "'UNKNOWN' AS engine,"
    )

    sql = f"""
        SELECT DISTINCT ON (rt.{BIKE_COL})
            rt.{BIKE_COL}::text AS bike,
            {driver_expr}
            {trip_expr}
            ROUND(rt.{SPEED_COL}::numeric, 1) AS speed,
            {engine_expr}
            rt.{LAT_COL} AS latitude,
            rt.{LON_COL} AS longitude,
            {zone_select},
            {status_case("rt")} AS status,
            rt.{TS_COL} AS ts
        FROM {RAW_TABLE} rt
        {zone_join}
        WHERE rt.{LAT_COL} IS NOT NULL
          AND rt.{LON_COL} IS NOT NULL
        ORDER BY rt.{BIKE_COL}, rt.{TS_COL} DESC
    """
    df = read_sql_df(sql, {"speed_threshold": SPEED_ALERT_THRESHOLD})
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return df.fillna("N/A").to_dict(orient="records")


@app.get("/api/bike-history/{bike_id}")
def get_bike_history(bike_id: str, limit: int = 30):
    if not LAT_COL or not LON_COL:
        return []

    zone_select, zone_join = zone_parts()

    driver_expr = (
        f"rt.{DRIVER_COL}::text AS driver," if DRIVER_COL else "'N/A' AS driver,"
    )
    trip_expr = f"rt.{TRIP_COL}::text AS trip," if TRIP_COL else "'N/A' AS trip,"
    engine_expr = (
        f"rt.{ENGINE_COL}::text AS engine," if ENGINE_COL else "'UNKNOWN' AS engine,"
    )

    sql = f"""
        SELECT
            rt.{BIKE_COL}::text AS bike,
            {driver_expr}
            {trip_expr}
            ROUND(rt.{SPEED_COL}::numeric, 1) AS speed,
            {engine_expr}
            rt.{LAT_COL} AS latitude,
            rt.{LON_COL} AS longitude,
            {zone_select},
            {status_case("rt")} AS status,
            rt.{TS_COL} AS ts
        FROM {RAW_TABLE} rt
        {zone_join}
        WHERE rt.{BIKE_COL}::text = :bike_id
          AND rt.{LAT_COL} IS NOT NULL
          AND rt.{LON_COL} IS NOT NULL
        ORDER BY rt.{TS_COL} DESC
        LIMIT :limit
    """

    df = read_sql_df(
        sql,
        {
            "bike_id": bike_id,
            "limit": limit,
            "speed_threshold": SPEED_ALERT_THRESHOLD,
        },
    )

    if df.empty:
        return []

    df = df.sort_values("ts")
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return df.fillna("N/A").to_dict(orient="records")