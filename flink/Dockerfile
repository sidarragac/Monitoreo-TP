FROM flink:1.18.1-scala_2.12

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    pip3 install apache-flink cassandra-driver kafka-python

RUN ln -s /usr/bin/python3 /usr/bin/python

# Kafka connector
RUN wget -P /opt/flink/lib \
https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar

# Kafka clients
RUN wget -P /opt/flink/lib \
https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

ENV PYTHON=python3