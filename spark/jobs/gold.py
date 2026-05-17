from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Gold Layer") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.table("demo.transport.silver_bus_events")

gold = (
    df.groupBy(
        "route_id",
        "zone",
        "risk_level"
    )
    .agg(
        avg("speed_kmh").alias("avg_speed"),
        count("*").alias("total_events"),
        sum(
            when(col("event_type") == "OVERSPEED", 1)
            .otherwise(0)
        ).alias("overspeed_events"),

        sum(
            when(col("event_type") == "HARSH_BRAKE", 1)
            .otherwise(0)
        ).alias("harsh_brakes")
    )
)

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.transport.gold_route_metrics (
    route_id STRING,
    zone STRING,
    risk_level STRING,
    avg_speed DOUBLE,
    total_events BIGINT,
    overspeed_events BIGINT,
    harsh_brakes BIGINT
)
USING iceberg
""")

gold.writeTo("demo.transport.gold_route_metrics").overwritePartitions()

print("Gold layer ready")