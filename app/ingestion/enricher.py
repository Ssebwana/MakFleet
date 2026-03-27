from app.db.postgres import get_pg_connection
from app.ingestion.map_matcher import (
    snap_to_nearest_node,
    snap_to_nearest_edge,
    find_zone,
)


def classify_event(speed_kmh: float, accel_x: float, accel_y: float, edge, zone):
    if accel_x < -2.5 or accel_y < -2.5:
        return "harsh_braking", 0.90, "Detected from accelerometer threshold"

    if edge and edge["speed_limit_kmh"] and speed_kmh > edge["speed_limit_kmh"]:
        return "speeding", 0.75, f"Exceeded edge speed limit on {edge['edge_name']}"

    if edge and edge["edge_type"] == "footpath":
        return "off_path", 0.85, "Movement mapped to footpath"

    if zone and zone["zone_type"] == "pedestrian_only":
        return "policy_zone_entry", 0.95, "Entered pedestrian-only zone"

    return "normal", 0.10, "No risk rule triggered"


def enrich_unprocessed_raw(limit: int = 500) -> int:
    conn = get_pg_connection()
    inserted = 0

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rt.*
                FROM raw_telemetry rt
                LEFT JOIN enriched_events ee
                  ON ee.raw_event_id = rt.event_id
                WHERE ee.raw_event_id IS NULL
                ORDER BY rt.sensor_ts ASC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

            for row in rows:
                node = snap_to_nearest_node(row["latitude"], row["longitude"])
                edge = snap_to_nearest_edge(row["latitude"], row["longitude"])
                zone = find_zone(row["latitude"], row["longitude"])

                event_type, severity, notes = classify_event(
                    row["speed_kmh"] or 0,
                    row["accel_x"] or 0,
                    row["accel_y"] or 0,
                    edge,
                    zone,
                )

                cur.execute(
                    """
                    INSERT INTO enriched_events (
                        raw_event_id, bike_id, driver_id, trip_id, sensor_ts,
                        event_type, severity, snapped_node_id, snapped_edge_id,
                        zone_id, notes, geom
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                    )
                    """,
                    (
                        row["event_id"],
                        row["bike_id"],
                        row["driver_id"],
                        row["trip_id"],
                        row["sensor_ts"],
                        event_type,
                        severity,
                        node["node_id"] if node else None,
                        edge["edge_id"] if edge else None,
                        zone["zone_id"] if zone else None,
                        notes,
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
    count = enrich_unprocessed_raw()
    print(f"Enriched {count} events.")