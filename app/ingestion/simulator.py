import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


OUTPUT_FILE = Path("data/raw/sample_telemetry.csv")


BIKES = ["MK001", "MK002", "MK003", "MK004"]
DRIVERS = ["DRV01", "DRV02", "DRV03", "DRV04"]


# Rough Makerere-like coordinates for simulation
BASE_POINTS = [
    (0.3344, 32.5680),  # Main Gate area
    (0.3348, 32.5685),  # Library Junction
    (0.3352, 32.5690),  # Hostel area
    (0.3346, 32.5683),  # Pedestrian zone edge
]


def generate_row(start_time: datetime, step: int):
    bike_id = random.choice(BIKES)
    driver_id = random.choice(DRIVERS)
    trip_id = f"TRIP{random.randint(1, 20):03d}"

    base_lat, base_lon = random.choice(BASE_POINTS)

    latitude = round(base_lat + random.uniform(-0.0004, 0.0004), 6)
    longitude = round(base_lon + random.uniform(-0.0004, 0.0004), 6)

    speed_kmh = round(random.uniform(5, 45), 2)
    heading = round(random.uniform(0, 360), 2)

    # occasional hard braking patterns
    if random.random() < 0.15:
        accel_x = round(random.uniform(-3.5, -2.6), 3)
        accel_y = round(random.uniform(-1.0, -0.2), 3)
    else:
        accel_x = round(random.uniform(-0.5, 0.5), 3)
        accel_y = round(random.uniform(-0.5, 0.5), 3)

    accel_z = round(random.uniform(-0.2, 0.2), 3)
    engine_state = random.choice(["on", "idle"])

    sensor_ts = (start_time + timedelta(seconds=step * 5)).strftime("%Y-%m-%d %H:%M:%S")

    return [
        bike_id,
        driver_id,
        trip_id,
        sensor_ts,
        latitude,
        longitude,
        speed_kmh,
        heading,
        accel_x,
        accel_y,
        accel_z,
        engine_state,
    ]


def generate_csv(rows: int = 100):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "bike_id",
        "driver_id",
        "trip_id",
        "sensor_ts",
        "latitude",
        "longitude",
        "speed_kmh",
        "heading",
        "accel_x",
        "accel_y",
        "accel_z",
        "engine_state",
    ]

    start_time = datetime.now()

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(rows):
            writer.writerow(generate_row(start_time, i))

    print(f"Generated {rows} simulated telemetry rows in {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_csv(rows=200)