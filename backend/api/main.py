from __future__ import annotations

import os
from datetime import datetime, timezone

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
RESTRICTED_ZONES = [
    zone.strip()
    for zone in os.getenv("RESTRICTED_ZONES", "").split(",")
    if zone.strip()
]

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

app = FastAPI(title="MakFleet API", version="1.5.0")

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


def get_latest_per_bike(include_coords: bool = False) -> pd.DataFrame:
    zone_select, zone_join = zone_parts()

    driver_expr = (
        f"rt.{DRIVER_COL}::text AS driver," if DRIVER_COL else "'N/A' AS driver,"
    )
    trip_expr = f"rt.{TRIP_COL}::text AS trip," if TRIP_COL else "'N/A' AS trip,"
    engine_expr = (
        f"rt.{ENGINE_COL}::text AS engine," if ENGINE_COL else "'UNKNOWN' AS engine,"
    )

    coords_expr = ""
    coords_filter = ""
    if include_coords and LAT_COL and LON_COL:
        coords_expr = f"""
            rt.{LAT_COL} AS latitude,
            rt.{LON_COL} AS longitude,
        """
        coords_filter = f"""
            AND rt.{LAT_COL} IS NOT NULL
            AND rt.{LON_COL} IS NOT NULL
        """

    sql = f"""
        SELECT DISTINCT ON (rt.{BIKE_COL})
            rt.{BIKE_COL}::text AS bike,
            {driver_expr}
            {trip_expr}
            ROUND(rt.{SPEED_COL}::numeric, 1) AS speed,
            {engine_expr}
            {coords_expr}
            {zone_select},
            {status_case("rt")} AS status,
            rt.{TS_COL} AS ts
        FROM {RAW_TABLE} rt
        {zone_join}
        WHERE 1=1
        {coords_filter}
        ORDER BY rt.{BIKE_COL}, rt.{TS_COL} DESC
    """
    df = read_sql_df(sql, {"speed_threshold": SPEED_ALERT_THRESHOLD})
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    return df


def get_idle_stats() -> pd.DataFrame:
    idle_condition = (
        f"UPPER(rt.{ENGINE_COL}::text) = 'IDLE' AND COALESCE(rt.{SPEED_COL}, 0) <= :idle_speed_threshold"
        if ENGINE_COL
        else f"COALESCE(rt.{SPEED_COL}, 0) <= :idle_speed_threshold"
    )

    sql = f"""
        SELECT
            rt.{BIKE_COL}::text AS bike,
            MIN(rt.{TS_COL}) AS idle_start,
            MAX(rt.{TS_COL}) AS idle_end,
            COUNT(*) AS idle_points
        FROM {RAW_TABLE} rt
        WHERE {idle_condition}
          AND rt.{TS_COL} >= NOW() - (:idle_window_minutes * INTERVAL '1 minute')
        GROUP BY rt.{BIKE_COL}
    """
    df = read_sql_df(
        sql,
        {
            "idle_speed_threshold": IDLE_SPEED_THRESHOLD,
            "idle_window_minutes": max(IDLE_ALERT_MINUTES * 3, 10),
        },
    )
    if not df.empty:
        df["idle_start"] = pd.to_datetime(df["idle_start"], errors="coerce")
        df["idle_end"] = pd.to_datetime(df["idle_end"], errors="coerce")
    return df


@app.get("/")
def root():
    return {"message": "MakFleet API is running"}


@app.get("/api/kpis")
def get_kpis():
    latest_df = get_latest_per_bike(include_coords=False)

    if latest_df.empty:
        return [
            {"label": "Active Bikes Now", "value": 0, "hint": "Latest reporting bikes"},
            {"label": "Idle Bikes Now", "value": 0, "hint": "Engine idle and slow"},
            {"label": "Stale Bikes", "value": 0, "hint": "No fresh telemetry"},
            {"label": "Critical Alerts Now", "value": 0, "hint": "Overspeed, long idle, stale, restricted"},
        ]

    now_utc = datetime.now(timezone.utc)
    ts_utc = pd.to_datetime(latest_df["ts"], utc=True, errors="coerce")

    idle_mask = (
        latest_df["engine"].astype(str).str.upper().eq("IDLE")
        & (pd.to_numeric(latest_df["speed"], errors="coerce").fillna(0) <= IDLE_SPEED_THRESHOLD)
    )
    stale_mask = ((now_utc - ts_utc).dt.total_seconds() / 60.0) >= STALE_ALERT_MINUTES
    overspeed_mask = pd.to_numeric(latest_df["speed"], errors="coerce").fillna(0) >= SPEED_ALERT_THRESHOLD
    restricted_mask = latest_df["zone"].astype(str).isin(RESTRICTED_ZONES) if RESTRICTED_ZONES else pd.Series(False, index=latest_df.index)

    critical_mask = overspeed_mask | stale_mask | restricted_mask | idle_mask

    return [
        {
            "label": "Active Bikes Now",
            "value": int(len(latest_df)),
            "hint": "Latest reporting bikes",
        },
        {
            "label": "Idle Bikes Now",
            "value": int(idle_mask.sum()),
            "hint": f"Engine idle and speed ≤ {IDLE_SPEED_THRESHOLD} km/h",
        },
        {
            "label": "Stale Bikes",
            "value": int(stale_mask.sum()),
            "hint": f"No fresh telemetry in {STALE_ALERT_MINUTES}+ min",
        },
        {
            "label": "Critical Alerts Now",
            "value": int(critical_mask.sum()),
            "hint": "Overspeed, long idle, stale, or restricted",
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
    latest_df = get_latest_per_bike(include_coords=False)
    idle_stats_df = get_idle_stats()

    alerts = []

    if not latest_df.empty:
        latest_df = latest_df.copy()
        latest_df["speed_num"] = pd.to_numeric(latest_df["speed"], errors="coerce").fillna(0)
        latest_df["ts"] = pd.to_datetime(latest_df["ts"], utc=True, errors="coerce")

        now_utc = datetime.now(timezone.utc)

        overspeed_df = latest_df[latest_df["speed_num"] >= SPEED_ALERT_THRESHOLD]
        for _, row in overspeed_df.iterrows():
            alerts.append(
                {
                    "id": f"OS-{row['bike']}",
                    "type": "Overspeed",
                    "severity": "High" if row["speed_num"] >= SPEED_ALERT_THRESHOLD * 1.2 else "Medium",
                    "bike": row["bike"],
                    "location": row["zone"],
                    "event_time": row["ts"],
                    "note": f"Bike reached {row['speed_num']:.1f} km/h, exceeding the configured threshold of {SPEED_ALERT_THRESHOLD} km/h.",
                }
            )

        stale_df = latest_df[
            ((now_utc - latest_df["ts"]).dt.total_seconds() / 60.0) >= STALE_ALERT_MINUTES
        ]
        for _, row in stale_df.iterrows():
            age_minutes = (now_utc - row["ts"]).total_seconds() / 60.0
            alerts.append(
                {
                    "id": f"ST-{row['bike']}",
                    "type": "Stale Telemetry",
                    "severity": "High" if age_minutes >= STALE_ALERT_MINUTES * 2 else "Medium",
                    "bike": row["bike"],
                    "location": row["zone"],
                    "event_time": row["ts"],
                    "note": f"No fresh telemetry for about {age_minutes:.1f} minutes.",
                }
            )

        if RESTRICTED_ZONES:
            restricted_df = latest_df[latest_df["zone"].astype(str).isin(RESTRICTED_ZONES)]
            for _, row in restricted_df.iterrows():
                alerts.append(
                    {
                        "id": f"RZ-{row['bike']}",
                        "type": "Restricted Zone",
                        "severity": "High",
                        "bike": row["bike"],
                        "location": row["zone"],
                        "event_time": row["ts"],
                        "note": f"Bike is currently inside restricted zone '{row['zone']}'.",
                    }
                )

    if not idle_stats_df.empty and not latest_df.empty:
        merged = idle_stats_df.merge(latest_df, on="bike", how="left")
        merged["idle_minutes"] = (
            (merged["idle_end"] - merged["idle_start"]).dt.total_seconds() / 60.0
        )

        merged = merged[
            (merged["idle_minutes"] >= IDLE_ALERT_MINUTES)
            & (merged["engine"].astype(str).str.upper() == "IDLE")
            & (pd.to_numeric(merged["speed"], errors="coerce").fillna(0) <= IDLE_SPEED_THRESHOLD)
        ]

        for _, row in merged.iterrows():
            alerts.append(
                {
                    "id": f"ID-{row['bike']}",
                    "type": "Long Idle",
                    "severity": "High" if row["idle_minutes"] >= IDLE_ALERT_MINUTES * 2 else "Medium",
                    "bike": row["bike"],
                    "location": row["zone"],
                    "event_time": row["ts"],
                    "note": f"Bike has remained idle for about {row['idle_minutes']:.1f} minutes.",
                }
            )

    if not alerts:
        return []

    alerts_df = pd.DataFrame(alerts).sort_values("event_time", ascending=False).head(limit)
    alerts_df["time"] = pd.to_datetime(alerts_df["event_time"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    alerts_df = alerts_df.drop(columns=["event_time"])
    return alerts_df.fillna("N/A").to_dict(orient="records")


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

    df = get_latest_per_bike(include_coords=True)
    if df.empty:
        return []

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


@app.get("/api/zone-events")
def get_zone_events(limit: int = 20):
    zone_select, zone_join = zone_parts("rt", "ee")

    sql = f"""
        WITH zone_stream AS (
            SELECT
                rt.{BIKE_COL}::text AS bike,
                {zone_select},
                rt.{TS_COL} AS ts,
                LAG(COALESCE({ 'ee.' + ENRICHED_ZONE_COL + '::text' if ENRICHED_ZONE_COL and 'event_id' in RAW_COLS and 'event_id' in ENRICHED_COLS else "'Unknown'" }, 'Unknown'))
                    OVER (PARTITION BY rt.{BIKE_COL} ORDER BY rt.{TS_COL}) AS previous_zone
            FROM {RAW_TABLE} rt
            {zone_join}
        )
        SELECT
            bike,
            previous_zone,
            zone AS current_zone,
            ts
        FROM zone_stream
        WHERE previous_zone IS NOT NULL
          AND zone <> previous_zone
        ORDER BY ts DESC
        LIMIT :limit
    """

    df = read_sql_df(sql, {"limit": limit})

    if df.empty:
        return []

    if RESTRICTED_ZONES:
        df["restricted_entry"] = df["current_zone"].astype(str).isin(RESTRICTED_ZONES)
    else:
        df["restricted_entry"] = False

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    return df.fillna("N/A").to_dict(orient="records")


@app.get("/api/bike-zone-history/{bike_id}")
def get_bike_zone_history(bike_id: str, limit: int = 20):
    zone_select, zone_join = zone_parts("rt", "ee")

    sql = f"""
        WITH zone_stream AS (
            SELECT
                rt.{BIKE_COL}::text AS bike,
                {zone_select},
                rt.{TS_COL} AS ts,
                LAG(COALESCE({ 'ee.' + ENRICHED_ZONE_COL + '::text' if ENRICHED_ZONE_COL and 'event_id' in RAW_COLS and 'event_id' in ENRICHED_COLS else "'Unknown'" }, 'Unknown'))
                    OVER (PARTITION BY rt.{BIKE_COL} ORDER BY rt.{TS_COL}) AS previous_zone
            FROM {RAW_TABLE} rt
            {zone_join}
            WHERE rt.{BIKE_COL}::text = :bike_id
        )
        SELECT
            bike,
            previous_zone,
            zone AS current_zone,
            ts
        FROM zone_stream
        WHERE previous_zone IS NOT NULL
          AND zone <> previous_zone
        ORDER BY ts DESC
        LIMIT :limit
    """

    df = read_sql_df(sql, {"bike_id": bike_id, "limit": limit})

    if df.empty:
        return []

    df = df.sort_values("ts")
    if RESTRICTED_ZONES:
        df["restricted_entry"] = df["current_zone"].astype(str).isin(RESTRICTED_ZONES)
    else:
        df["restricted_entry"] = False

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    return df.fillna("N/A").to_dict(orient="records")