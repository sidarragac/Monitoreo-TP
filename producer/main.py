import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# -----------------------------
# Args
# -----------------------------

parser = argparse.ArgumentParser()

parser.add_argument("--num_buses", type=int, default=5)
parser.add_argument("--interval", type=float, default=1.0)

args = parser.parse_args()

NUM_BUSES = args.num_buses
INTERVAL = args.interval
DATA_FILE = "./data/coords.json"

# -----------------------------
# Kafka Config
# -----------------------------

KAFKA_TOPIC = "bus_raw_events"
KAFKA_SERVER = "localhost:9092"

# -----------------------------
# Load Route Coordinates
# -----------------------------

with open(DATA_FILE, "r", encoding="utf-8") as f:
    ROUTE_POINTS = json.load(f)

print(f"Loaded {len(ROUTE_POINTS)} route points")

# -----------------------------
# Kafka Producer
# -----------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Connected to Kafka")

# -----------------------------
# Event Generator
# -----------------------------

def generate_event(bus_id, route_id, driver_id, point, prev_speed):

    # velocidad más realista
    speed = max(0, prev_speed + random.uniform(-12, 12))

    # aceleración aproximada
    acceleration = (speed - prev_speed) / random.uniform(1, 3)

    now = datetime.now(timezone.utc)

    event = {
        "event_id": str(uuid.uuid4()),

        "bus_id": bus_id,
        "route_id": route_id,
        "driver_id": driver_id,

        "station_name": point["name"],
        "station_order": point["order"],

        "timestamp": now.isoformat(),

        "lat": round(point["lat"], 6),
        "lon": round(point["lon"], 6),

        "speed_kmh": round(speed, 2),
        "acceleration_ms2": round(acceleration, 2),

        # RAW telemetry only
        "ingestion_ts": datetime.now(timezone.utc).isoformat()
    }

    return event, speed

# -----------------------------
# Main Simulation
# -----------------------------

def run():

    buses = {}

    for bus_id in range(1, NUM_BUSES + 1):

        buses[bus_id] = {
            "route_id": f"R{random.randint(100, 500)}",
            "driver_id": random.randint(1, 20),

            "prev_speed": random.uniform(20, 40),

            # posición inicial en la ruta
            "route_index": random.randint(0, len(ROUTE_POINTS) - 1)
        }

    while True:

        for bus_id, state in buses.items():

            point = ROUTE_POINTS[state["route_index"]]

            point["order"] = state["route_index"] + 1

            event, new_speed = generate_event(
                bus_id,
                state["route_id"],
                state["driver_id"],
                point,
                state["prev_speed"]
            )

            state["prev_speed"] = new_speed

            # avanzar en la ruta
            state["route_index"] += 1

            # reiniciar ruta
            if state["route_index"] >= len(ROUTE_POINTS):
                state["route_index"] = 0

            # -----------------------------
            # Send to Kafka
            # -----------------------------

            producer.send(KAFKA_TOPIC, event)

        producer.flush()

        time.sleep(INTERVAL)

# -----------------------------
# Main
# -----------------------------

if __name__ == "__main__":
    run()