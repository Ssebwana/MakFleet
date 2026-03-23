CREATE INDEX idx_raw_telemetry_geom ON raw_telemetry USING GIST (geom);
CREATE INDEX idx_campus_edges_geom ON campus_edges USING GIST (geom);
CREATE INDEX idx_campus_zones_geom ON campus_zones USING GIST (geom);
CREATE INDEX idx_enriched_events_geom ON enriched_events USING GIST (geom);

CREATE INDEX idx_raw_telemetry_bike_ts ON raw_telemetry (bike_id, sensor_ts);
CREATE INDEX idx_enriched_events_type_ts ON enriched_events (event_type, sensor_ts);