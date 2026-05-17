import argparse
import json
import random
import time
import uuid
import os
from datetime import datetime, timezone
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--num_buses", type=int, default=5)
parser.add_argument("--interval", type=float, default=1.0)
args = parser.parse_args()

NUM_BUSES = args.num_buses
INTERVAL = args.interval

KAFKA_TOPIC = "bus_events"
KAFKA_SERVER = "localhost:9092"

BRONZE_DIR = "bronze_data"
BATCH_SIZE = 100

os.makedirs(BRONZE_DIR, exist_ok=True)

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

def generate_event(bus_id, route_id, driver_id, lat, lon, prev_speed):
    speed = random.uniform(20, 90)
    acceleration = (speed - prev_speed) / random.uniform(1, 3)

    if speed > 75:
        event_type = "OVERSPEED"
    elif acceleration < -3:
        event_type = "HARSH_BRAKE"
    else:
        event_type = "NORMAL"

    now = datetime.now(timezone.utc)

    event = {
        "event_id": str(uuid.uuid4()),
        "bus_id": bus_id,
        "route_id": route_id,
        "driver_id": driver_id,
        "timestamp": now.isoformat(),
        "lat": lat,
        "lon": lon,
        "speed_kmh": round(speed, 2),
        "acceleration_ms2": round(acceleration, 2),
        "event_type": event_type,
        "ingestion_ts": datetime.now(timezone.utc).isoformat()
    }

    return event, speed


# -----------------------------
# Main Simulation
# -----------------------------

def run():
    buses = {
        bus_id: {
            "route_id": f"R{random.randint(100, 500)}",
            "driver_id": random.randint(1, 20),
            "prev_speed": 0
        }
        for bus_id in range(1, NUM_BUSES + 1)
    }

    batch_buffer = []

    while True:
        for bus_id, state in buses.items():

            lat = round(random.uniform(6.20, 6.30), 6)
            lon = round(random.uniform(-75.65, -75.55), 6)

            event, new_speed = generate_event(
                bus_id,
                state["route_id"],
                state["driver_id"],
                lat,
                lon,
                state["prev_speed"]
            )

            state["prev_speed"] = new_speed

            # ------------- STREAMING -------------
            producer.send(KAFKA_TOPIC, event)

            # ------------- BATCH -------------
            batch_buffer.append(event)

        # Flush batch to file
        if len(batch_buffer) >= BATCH_SIZE:
            filename = f"{BRONZE_DIR}/bronze_{int(time.time())}.json"
            with open(filename, "w", encoding="utf-8") as f:
                for e in batch_buffer:
                    f.write(json.dumps(e) + "\n")

            print(f"Batch file written: {filename}")
            batch_buffer = []

        time.sleep(INTERVAL)


if __name__ == "__main__":
    run()