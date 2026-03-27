import json
import os
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "makfleet.telemetry.raw")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

bikes = ["MK001", "MK002", "MK003"]
drivers = ["DRV01", "DRV02", "DRV03"]

# Rough Makerere-ish coordinates for prototype only
base_lat = 0.334
base_lon = 32.568

while True:
    payload = {
        "bike_id": random.choice(bikes),
        "driver_id": random.choice(drivers),
        "trip_id": f"TRIP-{random.randint(1,20)}",
        "sensor_ts": datetime.now(timezone.utc).isoformat(),
        "latitude": base_lat + random.uniform(-0.003, 0.003),
        "longitude": base_lon + random.uniform(-0.003, 0.003),
        "speed_kmh": round(random.uniform(0, 55), 2),
        "heading": round(random.uniform(0, 360), 2),
        "accel_x": round(random.uniform(-4, 4), 3),
        "accel_y": round(random.uniform(-4, 4), 3),
        "accel_z": round(random.uniform(-1, 1), 3),
        "engine_state": random.choice(["on", "idle"])
    }
    producer.send(TOPIC, payload)
    producer.flush()
    time.sleep(1)