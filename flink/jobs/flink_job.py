from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

import json
from cassandra.cluster import Cluster
from datetime import datetime


# =====================================================
# FLINK ENVIRONMENT
# =====================================================

env = StreamExecutionEnvironment.get_execution_environment()

env.enable_checkpointing(10000)

# Opcional: limitar paralelismo para entorno local
env.set_parallelism(1)


# =====================================================
# KAFKA SOURCE
# =====================================================

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:29092")
    .set_topics("bus_events")
    .set_group_id("flink-consumer")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

ds = env.from_source(
    source,
    WatermarkStrategy.for_monotonous_timestamps(),
    "Kafka Source"
)


# =====================================================
# PARSE JSON
# =====================================================

def parse_event(event):

    e = json.loads(event)

    return (
        e["route_id"],
        e["bus_id"],
        e["driver_id"],
        float(e["lat"]),
        float(e["lon"]),
        float(e["speed_kmh"]),
        e["event_type"],
        e["timestamp"]
    )


parsed = ds.map(
    parse_event,
    output_type=Types.TUPLE([
        Types.STRING(),   # route_id
        Types.INT(),      # bus_id
        Types.INT(),      # driver_id
        Types.FLOAT(),    # lat
        Types.FLOAT(),    # lon
        Types.FLOAT(),    # speed
        Types.STRING(),   # event_type
        Types.STRING()    # timestamp
    ])
)


# =====================================================
# CASSANDRA SINK
# =====================================================

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

        route_id, bus_id, driver_id, lat, lon, speed, event_type, ts = event

        self.session.execute("""
            INSERT INTO bus_realtime_status (
                bus_id,
                event_ts,
                route_id,
                driver_id,
                lat,
                lon,
                speed_kmh,
                event_type
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            bus_id,
            datetime.fromisoformat(ts.replace("Z", "+00:00")),
            route_id,
            driver_id,
            lat,
            lon,
            speed,
            event_type
        ))

        print(f"Inserted bus_id={bus_id}")

        return event

    def close(self):

        print("Closing Cassandra connection")

        if self.cluster:
            self.cluster.shutdown()


# =====================================================
# EXECUTE SINK
# =====================================================

parsed.map(
    CassandraSink(),
    output_type=Types.TUPLE([
        Types.STRING(),
        Types.INT(),
        Types.INT(),
        Types.FLOAT(),
        Types.FLOAT(),
        Types.FLOAT(),
        Types.STRING(),
        Types.STRING()
    ])
)


# =====================================================
# EXECUTE JOB
# =====================================================

env.execute("Bus Monitoring Streaming Job")