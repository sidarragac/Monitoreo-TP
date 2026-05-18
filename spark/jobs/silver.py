from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# ==========================================
# Spark Session
# ==========================================

spark = SparkSession.builder \
    .appName("Bronze Ingestion") \
    .config("spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://monitoreo-tp-iceberg-rest-1:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.s3.endpoint", "http://monitoreo-tp-minio-1:9000") \
    .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
    .config("spark.sql.catalog.demo.s3.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://monitoreo-tp-minio-1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .getOrCreate()

# ==========================================
# Schema del JSON
# ==========================================

schema = StructType([
    StructField("event_id",          StringType(),  True),
    StructField("bus_id",            IntegerType(), True),
    StructField("driver_id",         IntegerType(), True),
    StructField("next_station",      IntegerType(), True),
    StructField("timestamp",         StringType(),  True),
    StructField("lat",               DoubleType(),  True),
    StructField("lon",               DoubleType(),  True),
    StructField("speed_kmh",         DoubleType(),  True),
    StructField("acceleration_ms2",  DoubleType(),  True),
    StructField("ingestion_ts",      StringType(),  True),
])

# ==========================================
# Leer Bronze
# ==========================================

bronze_df = spark.read.table("demo.bronze.transactions")

# ==========================================
# Parsear JSON + castear tipos
# ==========================================

parsed_df = bronze_df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_ts",    to_timestamp(col("timestamp"))) \
    .withColumn("ingestion_ts", to_timestamp(col("ingestion_ts"))) \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("source_topic", lit("bus_raw_events")) \
    .drop("timestamp")  # reemplazada por event_ts tipada

# ==========================================
# Validación — descartar filas corruptas
# ==========================================

clean_df = parsed_df.filter(
    col("event_id").isNotNull() &
    col("bus_id").isNotNull() &
    col("lat").isNotNull() &
    col("lon").isNotNull() &
    col("event_ts").isNotNull() &
    col("lat").between(-90, 90) &
    col("lon").between(-180, 180) &
    (col("speed_kmh") >= lit(0)) 
)

# ==========================================
# Crear namespace y escribir Silver
# ==========================================

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")

clean_df.writeTo("demo.silver.bus_events") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()