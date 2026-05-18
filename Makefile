# =========================================
# Kafka
# =========================================

kafka-topic-create:
	docker exec -it monitoreo-tp-kafka-1 kafka-topics \
		--create \
		--if-not-exists \
		--topic bus_raw_events \
		--bootstrap-server localhost:29092 \
		--partitions 2 \
		--replication-factor 1

kafka-list-topics:
	docker exec -it monitoreo-tp-kafka-1 kafka-topics \
		--list \
		--bootstrap-server localhost:29092


# =========================================
# Producer
# =========================================

producer:
	python producer/main.py \
		--num_buses 10


# =========================================
# Flink
# =========================================

flink-run:
	docker exec -it flink-jobmanager \
		flink run -py /opt/flink/jobs/flink_job.py

flink-list:
	docker exec -it flink-jobmanager \
		flink list

flink-logs:
	docker logs -f flink-jobmanager \

flink-cancel:
	docker exec -it flink-jobmanager \
		flink cancel $(JOB_ID)

# =========================================
# Cassandra
# =========================================
cassandra-init:
	echo "Initializing Cassandra..."
	docker exec -i monitoreo-tp-cassandra-1 cqlsh -e " \
	CREATE KEYSPACE IF NOT EXISTS transport \
	WITH replication = {'class':'SimpleStrategy','replication_factor':1}; \
	USE transport; \
	CREATE TABLE bus_realtime_status ( \
		bus_id INT, \
		event_ts TIMESTAMP, \
		driver_id INT, \
		lat FLOAT, \
		lon FLOAT, \
		speed_kmh FLOAT, \
		acceleration_ms2 FLOAT, \
		event_type TEXT, \
		PRIMARY KEY (bus_id, event_ts) \
	) WITH CLUSTERING ORDER BY (event_ts DESC);"


# =========================================
# Spark Bronze
# =========================================

bronze:
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
		spark/bronze_job.py


# =========================================
# Spark Silver
# =========================================

silver:
	spark-submit spark/silver_job.py


# =========================================
# Spark Gold
# =========================================

gold:
	spark-submit spark/gold_job.py


# =========================================
# Docker
# =========================================

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f