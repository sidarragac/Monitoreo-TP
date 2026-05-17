from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Silver Layer") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.table("demo.transport.bronze_bus_events")

# -----------------------------
# CLEANING
# -----------------------------

df = df.filter(col("lat").isNotNull()) \
       .filter(col("lon").isNotNull()) \
       .filter(col("speed_kmh") > 0)

# -----------------------------
# CAST TIMESTAMP
# -----------------------------

df = df.withColumn(
    "event_time",
    to_timestamp("timestamp")
)

# -----------------------------
# CALCULATED COLUMNS
# -----------------------------

df = df.withColumn(
    "risk_level",
    when(col("speed_kmh") > 80, "HIGH")
    .when(col("speed_kmh") > 60, "MEDIUM")
    .otherwise("LOW")
)

# -----------------------------
# DIMENSION TABLE
# -----------------------------

routes = [
    ("R101", "North"),
    ("R202", "South"),
    ("R303", "Center")
]

routes_df = spark.createDataFrame(
    routes,
    ["route_id", "zone"]
)

df = df.join(routes_df, on="route_id", how="left")

# -----------------------------
# WRITE SILVER
# -----------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.transport.silver_bus_events (
    route_id STRING,
    zone STRING,
    event_id STRING,
    bus_id INT,
    driver_id INT,
    event_time TIMESTAMP,
    lat DOUBLE,
    lon DOUBLE,
    speed_kmh DOUBLE,
    acceleration_ms2 DOUBLE,
    event_type STRING,
    risk_level STRING
)
USING iceberg
""")

df.writeTo("demo.transport.silver_bus_events").overwritePartitions()

print("Silver layer ready")