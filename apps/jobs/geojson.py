import json
import psycopg2

FILES = [
    ("data/gis/campus_roads.geojson", "campus_edges", "road"),
    ("data/gis/campus_footpaths.geojson", "campus_edges", "footpath"),
]

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="makfleet", user="postgres", password="postgres"
)
cur = conn.cursor()

for path, table, edge_type in FILES:
    with open(path, "r", encoding="utf-8") as f:
        gj = json.load(f)
    for feat in gj["features"]:
        geom = json.dumps(feat["geometry"])
        name = feat["properties"].get("name")
        cur.execute("""
            INSERT INTO campus_edges (edge_name, edge_type, geom)
            VALUES (%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
        """, (name, edge_type, geom))

conn.commit()
cur.close()
conn.close()