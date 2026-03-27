import os
import numpy as np
import pandas as pd
from app.db.postgres import get_pg_connection
from app.config import WINDOW_MINUTES

OUTPUT_DIR = "data/processed"


def load_node_map():
    path = os.path.join(OUTPUT_DIR, "node_map.csv")
    if not os.path.exists(path):
        raise FileNotFoundError("node_map.csv not found. Run build_graph first.")
    return pd.read_csv(path)


def fetch_raw():
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    rt.sensor_ts,
                    ee.snapped_node_id,
                    rt.bike_id,
                    COALESCE(rt.speed_kmh, 0) AS speed_kmh
                FROM raw_telemetry rt
                JOIN enriched_events ee
                  ON ee.raw_event_id = rt.event_id
                WHERE ee.snapped_node_id IS NOT NULL
                ORDER BY rt.sensor_ts
            """)
            rows = cur.fetchall()
            return pd.DataFrame(rows)
    finally:
        conn.close()


def fetch_events():
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    sensor_ts,
                    snapped_node_id,
                    event_type
                FROM enriched_events
                WHERE snapped_node_id IS NOT NULL
                ORDER BY sensor_ts
            """)
            rows = cur.fetchall()
            return pd.DataFrame(rows)
    finally:
        conn.close()


def build_features():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    node_map = load_node_map()
    raw_df = fetch_raw()
    events_df = fetch_events()

    if raw_df.empty:
        raise ValueError("No raw/enriched joined data found.")
    if events_df.empty:
        raise ValueError("No enriched events found.")

    raw_df["sensor_ts"] = pd.to_datetime(raw_df["sensor_ts"])
    events_df["sensor_ts"] = pd.to_datetime(events_df["sensor_ts"])

    raw_df["window_start"] = raw_df["sensor_ts"].dt.floor(f"{WINDOW_MINUTES}min")
    events_df["window_start"] = events_df["sensor_ts"].dt.floor(f"{WINDOW_MINUTES}min")

    all_windows = sorted(raw_df["window_start"].dropna().unique())
    num_nodes = len(node_map)

    node_id_to_idx = dict(zip(node_map["node_id"], node_map["node_idx"]))
    feature_list = []

    for window in all_windows:
        X = np.zeros((num_nodes, 7), dtype=np.float32)

        raw_w = raw_df[raw_df["window_start"] == window]
        ev_w = events_df[events_df["window_start"] == window]

        for node_id, group in raw_w.groupby("snapped_node_id"):
            if pd.isna(node_id) or node_id not in node_id_to_idx:
                continue
            idx = node_id_to_idx[node_id]
            X[idx, 0] = group["bike_id"].nunique()     # bike count
            X[idx, 1] = group["speed_kmh"].mean()      # average speed

        for node_id, group in ev_w.groupby("snapped_node_id"):
            if pd.isna(node_id) or node_id not in node_id_to_idx:
                continue
            idx = node_id_to_idx[node_id]
            X[idx, 2] = (group["event_type"] == "harsh_braking").sum()
            X[idx, 3] = (group["event_type"] == "speeding").sum()
            X[idx, 4] = (group["event_type"] == "off_path").sum()

        ts = pd.Timestamp(window)
        X[:, 5] = ts.hour
        X[:, 6] = 1 if ts.weekday() < 5 else 0

        feature_list.append(X)

    features = np.array(feature_list, dtype=np.float32)
    np.save(os.path.join(OUTPUT_DIR, "features.npy"), features)

    print(f"Saved features.npy with shape {features.shape}")


if __name__ == "__main__":
    build_features()