from app.db.postgres import get_pg_connection


def snap_to_nearest_node(latitude: float, longitude: float):
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    node_id,
                    node_name,
                    node_type,
                    ST_Distance(
                        geom::geography,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ) AS distance_m
                FROM campus_nodes
                ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                LIMIT 1
                """,
                (longitude, latitude, longitude, latitude),
            )
            return cur.fetchone()
    finally:
        conn.close()


def snap_to_nearest_edge(latitude: float, longitude: float):
    conn = get_pg_connection()
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
            return cur.fetchone()
    finally:
        conn.close()


def find_zone(latitude: float, longitude: float):
    conn = get_pg_connection()
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
            return cur.fetchone()
    finally:
        conn.close()