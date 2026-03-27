import json
import os
import psycopg2

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "makfleet")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def load_edges(file_path: str, edge_type_default: str = "road"):
    conn = get_conn()
    inserted = 0
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            gj = json.load(f)

        with conn.cursor() as cur:
            for feat in gj["features"]:
                props = feat.get("properties", {})
                geom = json.dumps(feat["geometry"])
                edge_name = props.get("name", "Unnamed Edge")
                edge_type = props.get("edge_type", edge_type_default)
                speed_limit = props.get("speed_limit_kmh", 20)

                cur.execute(
                    """
                    INSERT INTO campus_edges (edge_name, edge_type, speed_limit_kmh, geom)
                    VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                    """,
                    (edge_name, edge_type, speed_limit, geom),
                )
                inserted += 1

        conn.commit()
    finally:
        conn.close()

    return inserted


def load_zones(file_path: str, zone_type_default: str = "general"):
    conn = get_conn()
    inserted = 0
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            gj = json.load(f)

        with conn.cursor() as cur:
            for feat in gj["features"]:
                props = feat.get("properties", {})
                geom = json.dumps(feat["geometry"])
                zone_name = props.get("name", "Unnamed Zone")
                zone_type = props.get("zone_type", zone_type_default)
                priority = props.get("priority_level", 1)

                cur.execute(
                    """
                    INSERT INTO campus_zones (zone_name, zone_type, priority_level, geom)
                    VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                    """,
                    (zone_name, zone_type, priority, geom),
                )
                inserted += 1

        conn.commit()
    finally:
        conn.close()

    return inserted


if __name__ == "__main__":
    edges_file = "data/gis/campus_edges.geojson"
    zones_file = "data/gis/campus_zones.geojson"

    if os.path.exists(edges_file):
        print(f"Loaded {load_edges(edges_file)} edges")
    else:
        print(f"Missing: {edges_file}")

    if os.path.exists(zones_file):
        print(f"Loaded {load_zones(zones_file)} zones")
    else:
        print(f"Missing: {zones_file}")