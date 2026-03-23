import json
import os
from datetime import datetime
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from db import pg_conn
from enricher import payload_hash
from map_matcher import classify_event

consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC_RAW", "makfleet.telemetry.raw"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
    auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password123"))
)

def insert_pg(payload):
    conn = pg_conn()
    cur = conn.cursor()

    h = payload_hash(payload)
    cur.execute("""
        INSERT INTO raw_telemetry (
            bike_id, driver_id, trip_id, sensor_ts, latitude, longitude,
            speed_kmh, heading, accel_x, accel_y, accel_z, engine_state,
            payload_hash, geom
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        )
        RETURNING event_id
    """, (
        payload["bike_id"], payload.get("driver_id"), payload.get("trip_id"),
        payload["sensor_ts"], payload["latitude"], payload["longitude"],
        payload.get("speed_kmh"), payload.get("heading"),
        payload.get("accel_x"), payload.get("accel_y"), payload.get("accel_z"),
        payload.get("engine_state"), h,
        payload["longitude"], payload["latitude"]
    ))
    event_id = cur.fetchone()[0]

    event_type, severity = classify_event(payload)

    cur.execute("""
        INSERT INTO enriched_events (
            raw_event_id, bike_id, driver_id, trip_id, sensor_ts,
            event_type, severity, geom
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        )
    """, (
        event_id, payload["bike_id"], payload.get("driver_id"), payload.get("trip_id"),
        payload["sensor_ts"], event_type, severity,
        payload["longitude"], payload["latitude"]
    ))

    conn.commit()
    cur.close()
    conn.close()
    return str(event_id), event_type, severity

def sync_neo4j(payload, event_id, event_type, severity):
    with neo4j_driver.session() as session:
        session.run("""
            MERGE (b:Bike {bike_id: $bike_id})
            MERGE (d:Driver {driver_id: $driver_id})
            MERGE (t:Trip {trip_id: $trip_id})
            MERGE (d)-[:DRIVES]->(b)
            MERGE (b)-[:MADE_TRIP]->(t)
            MERGE (e:Event {event_id: $event_id})
            SET e.event_type = $event_type,
                e.severity = $severity,
                e.sensor_ts = $sensor_ts
            MERGE (t)-[:HAS_EVENT]->(e)
        """, {
            "bike_id": payload["bike_id"],
            "driver_id": payload.get("driver_id", "UNKNOWN"),
            "trip_id": payload.get("trip_id", "UNKNOWN"),
            "event_id": event_id,
            "event_type": event_type,
            "severity": severity,
            "sensor_ts": payload["sensor_ts"]
        })

for msg in consumer:
    payload = msg.value
    event_id, event_type, severity = insert_pg(payload)
    sync_neo4j(payload, event_id, event_type, severity)
    print("Processed:", event_id, event_type)