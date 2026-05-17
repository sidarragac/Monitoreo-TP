from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder \
    .appName("Bronze Ingestion") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# -----------------------------
# READ RAW JSON
# -----------------------------

df = spark.read.json("/opt/spark/jobs/bronze_data/*.json")

# METADATA
df = df.withColumn("ingestion_time", current_timestamp()) \
       .withColumn("source_file", input_file_name())

# -----------------------------
# CREATE TABLE
# -----------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.transport.bronze_bus_events (
    event_id STRING,
    bus_id INT,
    route_id STRING,
    driver_id INT,
    timestamp STRING,
    lat DOUBLE,
    lon DOUBLE,
    speed_kmh DOUBLE,
    acceleration_ms2 DOUBLE,
    event_type STRING,
    ingestion_ts STRING,
    ingestion_time TIMESTAMP,
    source_file STRING
)
USING iceberg
""")

# -----------------------------
# APPEND
# -----------------------------

df.writeTo("demo.transport.bronze_bus_events").append()

print("Bronze loaded")