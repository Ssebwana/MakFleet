import os
import psycopg2
from fastapi import FastAPI
from neo4j import GraphDatabase

app = FastAPI(title="MakFleet API", version="0.1.0")

def pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "makfleet"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )

neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
    auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password123"))
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/anomalies/recent")
def recent_anomalies(limit: int = 20):
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT bike_id, trip_id, sensor_ts, event_type, severity
        FROM enriched_events
        WHERE event_type <> 'normal'
        ORDER BY sensor_ts DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {
            "bike_id": r[0],
            "trip_id": r[1],
            "sensor_ts": r[2],
            "event_type": r[3],
            "severity": r[4]
        } for r in rows
    ]

@app.get("/graph/trip/{trip_id}")
def trip_graph(trip_id: str):
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (t:Trip {trip_id: $trip_id})-[:HAS_EVENT]->(e:Event)
            RETURN t.trip_id AS trip_id, collect({
                event_id: e.event_id,
                event_type: e.event_type,
                severity: e.severity,
                sensor_ts: e.sensor_ts
            }) AS events
        """, {"trip_id": trip_id})
        record = result.single()
        return dict(record) if record else {"trip_id": trip_id, "events": []}