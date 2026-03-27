import json
import os
import hashlib
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from db import pg_conn
from map_matcher import snap_point, find_zone

consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC_RAW", "makfleet.telemetry.raw"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
    auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password123")),
)


def payload_hash(payload):
    return hashlib.sha256(str(sorted(payload.items())).encode()).hexdigest()


def classify_event(payload, snapped, zone):
    speed = payload.get("speed_kmh") or 0
    ax = payload.get("accel_x") or 0
    ay = payload.get("accel_y") or 0

    if ax < -2.5 or ay < -2.5:
        return "harsh_braking", 0.90, "Detected from accelerometer threshold"

    if snapped and snapped.get("speed_limit_kmh") and speed > snapped["speed_limit_kmh"]:
        return "speeding", 0.75, f"Exceeded edge speed limit on {snapped.get('edge_name')}"

    if snapped and snapped.get("edge_type") == "footpath":
        return "off_path", 0.85, "Movement mapped to footpath"

    if zone and zone.get("zone_type") == "pedestrian_only":
        return "policy_zone_entry", 0.95, "Entered pedestrian-only zone"

    return "normal", 0.10, "No risk rule triggered"


for msg in consumer:
    payload = msg.value
    h = payload_hash(payload)

    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            # Insert raw telemetry
            cur.execute(
                """
                INSERT INTO raw_telemetry (
                    bike_id, driver_id, trip_id, sensor_ts,
                    latitude, longitude, speed_kmh, heading,
                    accel_x, accel_y, accel_z, engine_state,
                    payload_hash, geom
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                )
                RETURNING event_id
                """,
                (
                    payload["bike_id"],
                    payload.get("driver_id"),
                    payload.get("trip_id"),
                    payload["sensor_ts"],
                    payload["latitude"],
                    payload["longitude"],
                    payload.get("speed_kmh"),
                    payload.get("heading"),
                    payload.get("accel_x"),
                    payload.get("accel_y"),
                    payload.get("accel_z"),
                    payload.get("engine_state"),
                    h,
                    payload["longitude"],
                    payload["latitude"],
                ),
            )
            raw_event_id = cur.fetchone()[0]

            snapped = snap_point(payload["latitude"], payload["longitude"])
            zone = find_zone(payload["latitude"], payload["longitude"])

            event_type, severity, notes = classify_event(payload, snapped, zone)

            cur.execute(
                """
                INSERT INTO enriched_events (
                    raw_event_id, bike_id, driver_id, trip_id, sensor_ts,
                    event_type, severity, snapped_edge_id, zone_id, notes, geom
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                )
                RETURNING enriched_id
                """,
                (
                    raw_event_id,
                    payload["bike_id"],
                    payload.get("driver_id"),
                    payload.get("trip_id"),
                    payload["sensor_ts"],
                    event_type,
                    severity,
                    snapped["edge_id"] if snapped else None,
                    zone["zone_id"] if zone else None,
                    notes,
                    payload["longitude"],
                    payload["latitude"],
                ),
            )
            enriched_id = cur.fetchone()[0]

        conn.commit()
    finally:
        conn.close()

    with neo4j_driver.session() as session:
        session.run(
            """
            MERGE (b:Bike {bike_id: $bike_id})
            MERGE (d:Driver {driver_id: $driver_id})
            MERGE (t:Trip {trip_id: $trip_id})
            MERGE (e:Event {event_id: $event_id})
            SET e.event_type = $event_type,
                e.severity = $severity,
                e.sensor_ts = $sensor_ts,
                e.notes = $notes

            MERGE (d)-[:DRIVES]->(b)
            MERGE (b)-[:MADE_TRIP]->(t)
            MERGE (t)-[:HAS_EVENT]->(e)
            """,
            bike_id=payload["bike_id"],
            driver_id=payload.get("driver_id", "UNKNOWN_DRIVER"),
            trip_id=payload.get("trip_id", "UNKNOWN_TRIP"),
            event_id=str(enriched_id),
            event_type=event_type,
            severity=severity,
            sensor_ts=payload["sensor_ts"],
            notes=notes,
        )

        if snapped:
            session.run(
                """
                MERGE (ce:CampusEdge {edge_id: $edge_id})
                SET ce.edge_name = $edge_name,
                    ce.edge_type = $edge_type,
                    ce.speed_limit_kmh = $speed_limit_kmh
                MATCH (e:Event {event_id: $event_id})
                MERGE (e)-[:OCCURRED_ON]->(ce)
                """,
                edge_id=snapped["edge_id"],
                edge_name=snapped["edge_name"],
                edge_type=snapped["edge_type"],
                speed_limit_kmh=snapped["speed_limit_kmh"],
                event_id=str(enriched_id),
            )

        if zone:
            session.run(
                """
                MERGE (z:Zone {zone_id: $zone_id})
                SET z.zone_name = $zone_name,
                    z.zone_type = $zone_type,
                    z.priority_level = $priority_level
                MATCH (e:Event {event_id: $event_id})
                MERGE (e)-[:WITHIN]->(z)
                """,
                zone_id=zone["zone_id"],
                zone_name=zone["zone_name"],
                zone_type=zone["zone_type"],
                priority_level=zone["priority_level"],
                event_id=str(enriched_id),
            )

    print(f"processed raw={raw_event_id} enriched={enriched_id} type={event_type}")