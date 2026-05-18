from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time

import json
from cassandra.cluster import Cluster
from datetime import datetime

# Cargar datos de la ruta desde el archivo JSON
DATA_FILE = 'data/coords.json'
with open(DATA_FILE, "r", encoding="utf-8") as f:
    ROUTE_POINTS = json.load(f)

# Configuración del entorno de ejecución de Flink
env = StreamExecutionEnvironment.get_execution_environment()

# Checkpointing configurado para garantizar al menos una vez (at-least-once) de procesamiento
env.enable_checkpointing(10000)

env.set_parallelism(1)

# Configuración del Kafka Source para consumir eventos de bus
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

# Función para procesar cada evento de bus y determinar su tipo (NORMAL, OFF_ROUTE, OVERSPEED, HARSH_BRAKE)
def parse_event(event):
    event = json.loads(event)

    speed = float(event["speed_kmh"])
    acceleration = float(event["acceleration_ms2"])

    station_number = event["next_station"]
    lat = round(event["lat"], 6)
    lon = round(event["lon"], 6)

    # Crear una lista de coordenadas normales para la estación actual y las anteriores
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

    # Determinar el tipo de evento basado en la posición, velocidad y aceleración del bus
    if bus_coords not in normal_coords:
        event_type = "OFF_ROUTE"
    elif speed > ROUTE_POINTS[station_number]["max_speed"]:
        event_type = "OVERSPEED"
    elif acceleration < -3:
        event_type = "HARSH_BRAKE"
    else:
        event_type = "NORMAL"

    # Devolver una tupla con los datos almacenados del evento
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


# Aplicar la función de procesamiento a cada evento del stream y definir el tipo de salida
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

class CassandraRealtimeSink(MapFunction):
    def __init__(self):
        self.cluster = None
        self.session = None

    # Método para establecer la conexión con Cassandra al abrir el sink
    def open(self, runtime_context):
        print("Connecting to Cassandra...")

        self.cluster = Cluster(["cassandra"])
        self.session = self.cluster.connect("transport")

        print("Connected to Cassandra")

    # Método para procesar cada evento y almacenarlo en la tabla bus_realtime_status de Cassandra
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

        self.session.execute_async("""
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

# Aplicar el CassandraSink para almacenar los eventos procesados en Cassandra
parsed.map(
    CassandraRealtimeSink(),
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

# Función para procesar los eventos dentro de cada ventana y calcular las métricas agregadas por bus
class MetricsWindowFunction(ProcessWindowFunction):

    def process(self, key, context, elements):
        events = list(elements)
        bus_id = key
        avg_speed = sum(event[4] for event in events) / len(events)

        # Contar el número de eventos de cada tipo (OVERSPEED, HARSH_BRAKE, OFF_ROUTE) dentro de la ventana
        overspeed_count = len([
            event for event in events
            if event[6] == "OVERSPEED"
        ])

        harsh_brake_count = len([
            event for event in events
            if event[6] == "HARSH_BRAKE"
        ])

        off_route_count = len([
            event for event in events
            if event[6] == "OFF_ROUTE"
        ])

        yield (
            bus_id,
            round(avg_speed, 2),
            overspeed_count,
            harsh_brake_count,
            off_route_count,
            len(events),
            datetime.utcnow().isoformat()
        )

# Aplicar una ventana de tumbling de 30 segundos para calcular las métricas agregadas por bus
windowed_metrics = (
    parsed
    .key_by(lambda x: x[0])  # bus_id
    .window(
        TumblingProcessingTimeWindows.of(
            Time.seconds(30)
        )
    )
    .process(
        MetricsWindowFunction(),
        output_type=Types.TUPLE([
            Types.INT(),      # bus_id
            Types.FLOAT(),    # avg_speed
            Types.INT(),      # overspeed_count
            Types.INT(),      # harsh_brake_count
            Types.INT(),      # off_route_count
            Types.INT(),      # total_events
            Types.STRING()    # window_ts
        ])
    )
)


class CassandraMetricsSink(MapFunction):
    def __init__(self):
        self.cluster = None
        self.session = None

    def open(self, runtime_context):
        self.cluster = Cluster(["cassandra"])
        self.session = self.cluster.connect("transport")

    # Método para procesar cada métrica agregada por bus y almacenarla en la tabla bus_window_metrics de Cassandra
    def map(self, metric):
        (
            bus_id,
            avg_speed,
            overspeed_count,
            harsh_brake_count,
            off_route_count,
            total_events,
            window_ts
        ) = metric

        self.session.execute_async("""
            INSERT INTO bus_window_metrics (
                bus_id,
                window_ts,
                avg_speed,
                overspeed_count,
                harsh_brake_count,
                off_route_count,
                total_events
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            bus_id,
            datetime.fromisoformat(
                window_ts.replace("Z", "+00:00")
            ),
            avg_speed,
            overspeed_count,
            harsh_brake_count,
            off_route_count,
            total_events
        ))

        return metric

    def close(self):

        if self.cluster:
            self.cluster.shutdown()

# Aplicar el CassandraSink para almacenar las métricas agregadas por bus en Cassandra
windowed_metrics.map(
    CassandraMetricsSink(),
    output_type=Types.TUPLE([
        Types.INT(),
        Types.FLOAT(),
        Types.INT(),
        Types.INT(),
        Types.INT(),
        Types.INT(),
        Types.STRING()
    ])
)

env.execute("Bus Monitoring Streaming Job")