import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from math import radians, cos, sin, asin, sqrt

parser = argparse.ArgumentParser()
parser.add_argument("--num_buses", type=int, default=5)
parser.add_argument("--stream", action="store_true", help="Enable streaming mode")
parser.add_argument("--interval", type=float, default=1.0, help="Seconds between events")
args = parser.parse_args()

NUM_BUSES = args.num_buses
STREAM_MODE = args.stream
INTERVAL = args.interval


# -----------------------------
# Utilities
# -----------------------------

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371 * c


def load_stops():
    with open("coords.json", "r", encoding="utf-8") as f:
        return json.load(f)


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
    stops = load_stops()

    buses = {
        bus_id: {
            "route_id": f"R{random.randint(100, 500)}",
            "driver_id": random.randint(1, 20),
            "current_stop": 0,
            "prev_speed": 0
        }
        for bus_id in range(1, NUM_BUSES + 1)
    }

    batch_buffer = []

    while True:
        for bus_id, state in buses.items():
            stop = stops[state["current_stop"]]

            event, new_speed = generate_event(
                bus_id,
                state["route_id"],
                state["driver_id"],
                stop["lat"],
                stop["lng"],
                state["prev_speed"]
            )

            state["prev_speed"] = new_speed
            state["current_stop"] = (state["current_stop"] + 1) % len(stops)

            # STREAM OUTPUT (Kafka-ready)
            print(json.dumps(event))

            # BATCH BUFFER
            batch_buffer.append(event)

        # Write batch file every 50 events
        if not STREAM_MODE and len(batch_buffer) >= 50:
            filename = f"bronze_{int(time.time())}.json"
            with open(filename, "w", encoding="utf-8") as f:
                for e in batch_buffer:
                    f.write(json.dumps(e) + "\n")
            print(f"Batch file written: {filename}")
            batch_buffer = []

        time.sleep(INTERVAL)


if __name__ == "__main__":
    run()