CREATE TABLE raw_telemetry (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bike_id TEXT NOT NULL,
    driver_id TEXT,
    trip_id TEXT,
    sensor_ts TIMESTAMPTZ NOT NULL,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed_kmh DOUBLE PRECISION,
    heading DOUBLE PRECISION,
    accel_x DOUBLE PRECISION,
    accel_y DOUBLE PRECISION,
    accel_z DOUBLE PRECISION,
    engine_state TEXT,
    payload_hash TEXT NOT NULL,
    geom geometry(Point, 4326) NOT NULL
);

CREATE TABLE campus_edges (
    edge_id SERIAL PRIMARY KEY,
    edge_name TEXT,
    edge_type TEXT NOT NULL, -- road | footpath | restricted_path
    speed_limit_kmh DOUBLE PRECISION,
    geom geometry(LineString, 4326) NOT NULL
);

CREATE TABLE campus_zones (
    zone_id SERIAL PRIMARY KEY,
    zone_name TEXT NOT NULL,
    zone_type TEXT NOT NULL, -- library | hostel | pedestrian_only | gate
    priority_level INTEGER DEFAULT 1,
    geom geometry(Polygon, 4326) NOT NULL
);

CREATE TABLE enriched_events (
    enriched_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_event_id UUID REFERENCES raw_telemetry(event_id),
    bike_id TEXT NOT NULL,
    driver_id TEXT,
    trip_id TEXT,
    sensor_ts TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL, -- harsh_braking | speeding | off_path
    severity DOUBLE PRECISION,
    snapped_edge_id INTEGER REFERENCES campus_edges(edge_id),
    zone_id INTEGER REFERENCES campus_zones(zone_id),
    notes TEXT,
    geom geometry(Point, 4326) NOT NULL
);

CREATE TABLE trips (
    trip_id TEXT PRIMARY KEY,
    bike_id TEXT NOT NULL,
    driver_id TEXT,
    start_ts TIMESTAMPTZ,
    end_ts TIMESTAMPTZ,
    start_geom geometry(Point, 4326),
    end_geom geometry(Point, 4326),
    total_distance_m DOUBLE PRECISION,
    total_duration_s DOUBLE PRECISION,
    anomaly_score DOUBLE PRECISION DEFAULT 0
);