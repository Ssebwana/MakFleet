CREATE TABLE IF NOT EXISTS campus_edges (
    edge_id SERIAL PRIMARY KEY,
    edge_name TEXT,
    edge_type TEXT NOT NULL, -- road | footpath | restricted_path
    speed_limit_kmh DOUBLE PRECISION,
    geom geometry(LineString, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS campus_zones (
    zone_id SERIAL PRIMARY KEY,
    zone_name TEXT NOT NULL,
    zone_type TEXT NOT NULL, -- library | hostel | pedestrian_only | gate
    priority_level INTEGER DEFAULT 1,
    geom geometry(Polygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS enriched_events (
    enriched_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_event_id UUID REFERENCES raw_telemetry(event_id),
    bike_id TEXT NOT NULL,
    driver_id TEXT,
    trip_id TEXT,
    sensor_ts TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL, -- harsh_braking | speeding | off_path | normal
    severity DOUBLE PRECISION,
    snapped_edge_id INTEGER REFERENCES campus_edges(edge_id),
    zone_id INTEGER REFERENCES campus_zones(zone_id),
    notes TEXT,
    geom geometry(Point, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_campus_edges_geom
ON campus_edges USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_campus_zones_geom
ON campus_zones USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_enriched_events_geom
ON enriched_events USING GIST (geom);
