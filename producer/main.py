import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from random import random, randint, uniform

from kafka import KafkaProducer

parser = argparse.ArgumentParser()

parser.add_argument("--num_buses", type=int, default=5)
parser.add_argument("--interval", type=float, default=1.0)

args = parser.parse_args()

NUM_BUSES = args.num_buses
INTERVAL = args.interval
DATA_FILE = "../data/coords.json"

KAFKA_TOPIC = "bus_raw_events"
KAFKA_SERVER = "localhost:9092"

with open(DATA_FILE, "r", encoding="utf-8") as f:
    ROUTE_POINTS = json.load(f)

print(f"Loaded {len(ROUTE_POINTS)} route points")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Connected to Kafka")

def generate_event(bus_id, driver_id, station_number, point, prev_speed):
    speed = max(0, prev_speed + uniform(-12, 12))
    acceleration = (speed - prev_speed) / uniform(1, 3)
    now = datetime.now(timezone.utc)

    lat = round(point["lat"], 6)
    lon = round(point["lon"], 6)
    
    # 1 de cada 10 eventos sale fuera de la ruta esperada
    if random() < 0.1:
        lat += 0.05 if random() < 0.5 else -0.05
        lon += 0.05 if random() < 0.5 else -0.05

    event = {
        "event_id": str(uuid.uuid4()),
        "bus_id": bus_id,
        "driver_id": driver_id,
        "next_station": station_number,
        "timestamp": now.isoformat(),
        "lat": lat,
        "lon": lon,
        "speed_kmh": round(speed, 2),
        "acceleration_ms2": round(acceleration, 2),

        "ingestion_ts": datetime.now(timezone.utc).isoformat()
    }

    return event, speed

def run():
    buses = {}

    for bus_id in range(1, NUM_BUSES + 1):
        route_index = randint(0, len(ROUTE_POINTS) - 1)
        buses[bus_id] = {
            "driver_id": randint(1, 20),
            "prev_speed": uniform(20, 40),
            "route_index": route_index,
            "previous_point": 0
        }

    while True:
        for bus_id, state in buses.items():
            station = ROUTE_POINTS[state["route_index"]]

            is_station = False
            point = {}

            if state["previous_point"] == (len(station["previous"]) -1):
                is_station = True
                point = {
                    "lat": station["lat"],
                    "lon": station["lon"]
                }
            else:
                point = station["previous"][state["previous_point"]]

            event, new_speed = generate_event(
                bus_id,
                state["driver_id"],
                state["route_index"],
                point,
                state["prev_speed"]
            )

            state["prev_speed"] = new_speed

            if is_station:
                state["route_index"] += 1
                state["previous_point"] = 0
            else:
                state["previous_point"] += 1

            if state["route_index"] >= len(ROUTE_POINTS):
                state["route_index"] = 0
                state["previous_point"] = 0

            producer.send(KAFKA_TOPIC, event)

        producer.flush()

        time.sleep(INTERVAL)

if __name__ == "__main__":
    run()