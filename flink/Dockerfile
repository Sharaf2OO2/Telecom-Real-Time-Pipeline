FROM flink:2.0.0-scala_2.12

# download jars (example using Maven central patterns)
# NOTE: the exact artifact path depends on ICEBERG_VERSION & FLINK_MAJOR.
# The Iceberg docs show the pattern to download the correct runtime jar for the Flink major you use.
# (You may update ICEBERG_VERSION to the latest compatible one.)
RUN wget -O /opt/flink/lib/flink-connector-kafka-4.0.0-2.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar \
 && wget -O /opt/flink/lib/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
 && wget -O /opt/flink/lib/streampark-flink-connector-clickhouse_2.12-2.0.0.jar \
    https://repo1.maven.org/maven2/org/apache/streampark/streampark-flink-connector-clickhouse_2.12/2.0.0/streampark-flink-connector-clickhouse_2.12-2.0.0.jar \
 && wget -O /opt/flink/lib/flink-connector-jdbc-3.2.0-1.19.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar \
 && wget -O /opt/flink/lib/clickhouse-jdbc-0.6.0-all.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar
    
# (Optional) copy local config files if you want to bake them into the image
# COPY ./conf/flink-conf.yml /opt/flink/conf/config.yaml

# Install Python and PyFlink
USER root
RUN apt-get update && apt-get install -y python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && pip3 install apache-flink

RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Keep default entrypoint from base image
