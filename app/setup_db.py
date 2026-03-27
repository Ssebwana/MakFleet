from app.db.postgres import get_pg_connection

SQL = """
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS raw_telemetry (
    event_id SERIAL PRIMARY KEY,
    bike_id TEXT NOT NULL,
    driver_id TEXT,
    trip_id TEXT,
    sensor_ts TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed_kmh DOUBLE PRECISION,
    heading DOUBLE PRECISION,
    accel_x DOUBLE PRECISION,
    accel_y DOUBLE PRECISION,
    accel_z DOUBLE PRECISION,
    engine_state TEXT,
    geom geometry(Point, 4326)
);

CREATE TABLE IF NOT EXISTS campus_nodes (
    node_id SERIAL PRIMARY KEY,
    node_name TEXT NOT NULL,
    node_type TEXT NOT NULL,
    geom geometry(Point, 4326)
);

CREATE TABLE IF NOT EXISTS campus_edges (
    edge_id SERIAL PRIMARY KEY,
    source_node INTEGER REFERENCES campus_nodes(node_id),
    target_node INTEGER REFERENCES campus_nodes(node_id),
    edge_name TEXT,
    edge_type TEXT NOT NULL,
    speed_limit_kmh DOUBLE PRECISION,
    geom geometry(LineString, 4326)
);

CREATE TABLE IF NOT EXISTS campus_zones (
    zone_id SERIAL PRIMARY KEY,
    zone_name TEXT NOT NULL,
    zone_type TEXT NOT NULL,
    priority_level INTEGER DEFAULT 1,
    geom geometry(Polygon, 4326)
);

CREATE TABLE IF NOT EXISTS enriched_events (
    enriched_id SERIAL PRIMARY KEY,
    raw_event_id INTEGER REFERENCES raw_telemetry(event_id),
    bike_id TEXT NOT NULL,
    driver_id TEXT,
    trip_id TEXT,
    sensor_ts TIMESTAMP NOT NULL,
    event_type TEXT NOT NULL,
    severity DOUBLE PRECISION,
    snapped_node_id INTEGER REFERENCES campus_nodes(node_id),
    snapped_edge_id INTEGER REFERENCES campus_edges(edge_id),
    zone_id INTEGER REFERENCES campus_zones(zone_id),
    notes TEXT,
    geom geometry(Point, 4326)
);

CREATE INDEX IF NOT EXISTS idx_raw_telemetry_geom
ON raw_telemetry USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_campus_nodes_geom
ON campus_nodes USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_campus_edges_geom
ON campus_edges USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_campus_zones_geom
ON campus_zones USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_enriched_events_geom
ON enriched_events USING GIST (geom);
"""

SEED = """
INSERT INTO campus_nodes (node_name, node_type, geom)
VALUES
('Main Gate', 'gate', ST_SetSRID(ST_MakePoint(32.5680, 0.3344), 4326)),
('Library Junction', 'junction', ST_SetSRID(ST_MakePoint(32.5685, 0.3348), 4326)),
('Hostel Area', 'hostel', ST_SetSRID(ST_MakePoint(32.5690, 0.3352), 4326))
ON CONFLICT DO NOTHING;

INSERT INTO campus_edges (source_node, target_node, edge_name, edge_type, speed_limit_kmh, geom)
VALUES
(
  1, 2, 'Gate to Library', 'road', 25,
  ST_SetSRID(ST_GeomFromText('LINESTRING(32.5680 0.3344, 32.5685 0.3348)'), 4326)
),
(
  2, 3, 'Library to Hostel', 'road', 20,
  ST_SetSRID(ST_GeomFromText('LINESTRING(32.5685 0.3348, 32.5690 0.3352)'), 4326)
),
(
  2, 3, 'Library Footpath', 'footpath', 10,
  ST_SetSRID(ST_GeomFromText('LINESTRING(32.5683 0.3346, 32.5687 0.3349)'), 4326)
)
ON CONFLICT DO NOTHING;

INSERT INTO campus_zones (zone_name, zone_type, priority_level, geom)
VALUES
(
  'Main Library Zone',
  'library',
  2,
  ST_SetSRID(
    ST_GeomFromText('POLYGON((32.5683 0.3346, 32.5687 0.3346, 32.5687 0.3350, 32.5683 0.3350, 32.5683 0.3346))'),
    4326
  )
),
(
  'Pedestrian Zone A',
  'pedestrian_only',
  3,
  ST_SetSRID(
    ST_GeomFromText('POLYGON((32.5682 0.3345, 32.5684 0.3345, 32.5684 0.3348, 32.5682 0.3348, 32.5682 0.3345))'),
    4326
  )
)
ON CONFLICT DO NOTHING;
"""

def main():
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL)
            cur.execute(SEED)
        conn.commit()
        print("Database tables and seed data created successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()