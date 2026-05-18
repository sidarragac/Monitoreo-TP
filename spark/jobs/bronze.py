from pyspark.sql import SparkSession

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

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "bus_raw_events") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr(
    "CAST(key AS STRING) as key",
    "CAST(value AS STRING) as value",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")

query = df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://warehouse/checkpoints/bronze-transactions") \
    .trigger(availableNow=True) \
    .toTable("demo.bronze.transactions")

query.awaitTermination()