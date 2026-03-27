import pandas as pd
from app.db.postgres import get_pg_connection


REQUIRED_COLUMNS = [
    "bike_id", "driver_id", "trip_id", "sensor_ts",
    "latitude", "longitude", "speed_kmh",
    "heading", "accel_x", "accel_y", "accel_z", "engine_state"
]


def load_csv_to_postgres(csv_path: str) -> int:
    df = pd.read_csv(csv_path)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    conn = get_pg_connection()
    inserted = 0

    try:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO raw_telemetry (
                        bike_id, driver_id, trip_id, sensor_ts,
                        latitude, longitude, speed_kmh, heading,
                        accel_x, accel_y, accel_z, engine_state, geom
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                    )
                    """,
                    (
                        row["bike_id"],
                        row["driver_id"],
                        row["trip_id"],
                        row["sensor_ts"],
                        row["latitude"],
                        row["longitude"],
                        row["speed_kmh"],
                        row["heading"],
                        row["accel_x"],
                        row["accel_y"],
                        row["accel_z"],
                        row["engine_state"],
                        row["longitude"],
                        row["latitude"],
                    ),
                )
                inserted += 1

        conn.commit()
    finally:
        conn.close()

    return inserted


if __name__ == "__main__":
    count = load_csv_to_postgres("data/raw/sample_telemetry.csv")
    print(f"Loaded {count} telemetry rows into raw_telemetry.")