from random import random

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

import json
from cassandra.cluster import Cluster
from datetime import datetime

DATA_FILE = '../data/coords.json'

with open(DATA_FILE, "r", encoding="utf-8") as f:
    ROUTE_POINTS = json.load(f)

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10000)
env.set_parallelism(1)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:29092")
    .set_topics("bus_raw_events")
    .set_group_id("flink-consumer")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

ds = env.from_source(
    source,
    WatermarkStrategy.for_monotonous_timestamps(),
    "Kafka Source"
)

def parse_event(event):
    event = json.loads(event)

    speed = float(event["speed_kmh"])
    acceleration = float(event["acceleration_ms2"])

    station_number = event["next_station"]
    lat = round(event["lat"], 6)
    lon = round(event["lon"], 6)
    normal_coords = [
        {
            "lat": round(point["lat"], 6),
            "lon": round(point["lon"], 6),
        }
        for point in [ROUTE_POINTS[station_number]] + ROUTE_POINTS[station_number]["previous"]
    ]
    bus_coords = {
        "lat": lat,
        "lon": lon
    }

    if bus_coords not in normal_coords:
        event_type = "OFF_ROUTE"
    elif speed > 75:
        event_type = "OVERSPEED"
    elif acceleration < -3:
        event_type = "HARSH_BRAKE"
    else:
        event_type = "NORMAL"

    return (
        int(event["bus_id"]),
        int(event["driver_id"]),
        lat,
        lon,
        speed,
        acceleration,
        event_type,
        event["timestamp"]
    )


parsed = ds.map(
    parse_event,
    output_type=Types.TUPLE([
        Types.INT(),      # bus_id
        Types.INT(),      # driver_id
        Types.FLOAT(),    # lat
        Types.FLOAT(),    # lon
        Types.FLOAT(),    # speed
        Types.FLOAT(),    # acceleration
        Types.STRING(),   # event_type
        Types.STRING()    # timestamp
    ])
)

class CassandraSink(MapFunction):
    def __init__(self):
        self.cluster = None
        self.session = None

    def open(self, runtime_context):

        print("Connecting to Cassandra...")

        self.cluster = Cluster(["cassandra"])

        self.session = self.cluster.connect("transport")

        print("Connected to Cassandra")

    def map(self, event):
        (
            bus_id,
            driver_id,
            lat,
            lon,
            speed,
            acceleration,
            event_type,
            ts
        ) = event

        self.session.execute("""
            INSERT INTO bus_realtime_status (
                bus_id,
                event_ts,
                driver_id,
                lat,
                lon,
                speed_kmh,
                acceleration_ms2,
                event_type
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            bus_id,
            datetime.fromisoformat(
                ts.replace("Z", "+00:00")
            ),
            driver_id,
            lat,
            lon,
            speed,
            acceleration,
            event_type
        ))

        return event

    def close(self):
        print("Closing Cassandra connection")
        if self.cluster:
            self.cluster.shutdown()

parsed.map(
    CassandraSink(),
    output_type=Types.TUPLE([
        Types.INT(),
        Types.INT(),
        Types.FLOAT(),
        Types.FLOAT(),
        Types.FLOAT(),
        Types.FLOAT(),
        Types.STRING(),
        Types.STRING()
    ])
)

env.execute("Bus Monitoring Streaming Job")