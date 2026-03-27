from db import pg_conn


def snap_point(latitude: float, longitude: float) -> dict | None:
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    edge_id,
                    edge_name,
                    edge_type,
                    speed_limit_kmh,
                    ST_Distance(
                        geom::geography,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ) AS distance_m
                FROM campus_edges
                ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                LIMIT 1
                """,
                (longitude, latitude, longitude, latitude),
            )
            row = cur.fetchone()

            if not row:
                return None

            return {
                "edge_id": row[0],
                "edge_name": row[1],
                "edge_type": row[2],
                "speed_limit_kmh": row[3],
                "distance_m": float(row[4]) if row[4] is not None else None,
            }
    finally:
        conn.close()


def find_zone(latitude: float, longitude: float) -> dict | None:
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT zone_id, zone_name, zone_type, priority_level
                FROM campus_zones
                WHERE ST_Contains(
                    geom,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                )
                ORDER BY priority_level DESC
                LIMIT 1
                """,
                (longitude, latitude),
            )
            row = cur.fetchone()

            if not row:
                return None

            return {
                "zone_id": row[0],
                "zone_name": row[1],
                "zone_type": row[2],
                "priority_level": row[3],
            }
    finally:
        conn.close()