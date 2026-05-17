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

# entorno local
env.set_parallelism(1)

# =====================================================
# KAFKA SOURCE
# =====================================================

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

# =====================================================
# PARSE + EVENT DETECTION
# =====================================================

def parse_event(event):

    e = json.loads(event)

    speed = float(e["speed_kmh"])
    acceleration = float(e["acceleration_ms2"])

    # -----------------------------
    # Realtime Event Detection
    # -----------------------------

    if speed > 75:
        event_type = "OVERSPEED"

    elif acceleration < -3:
        event_type = "HARSH_BRAKE"

    else:
        event_type = "NORMAL"

    return (
        e["route_id"],
        int(e["bus_id"]),
        int(e["driver_id"]),

        float(e["lat"]),
        float(e["lon"]),

        speed,
        acceleration,

        event_type,

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
        Types.FLOAT(),    # acceleration
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

        (
            route_id,
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
                route_id,
                driver_id,
                lat,
                lon,
                speed_kmh,
                acceleration_ms2,
                event_type
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            bus_id,

            datetime.fromisoformat(
                ts.replace("Z", "+00:00")
            ),

            route_id,
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
        Types.FLOAT(),
        Types.STRING(),
        Types.STRING()
    ])
)

# =====================================================
# EXECUTE JOB
# =====================================================

env.execute("Bus Monitoring Streaming Job")