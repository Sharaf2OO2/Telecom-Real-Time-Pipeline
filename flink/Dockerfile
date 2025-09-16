FROM flink:2.0.0-scala_2.12

# Args you can tweak
# pick an Iceberg release that you want (or 1.9.1/1.10.x); match docs
ENV FLINK_MAJOR=1.17
ENV ICEBERG_VERSION=1.5.2

# Create plugins folder for S3 and iceberg runtime
RUN mkdir -p /opt/flink/plugins/iceberg

# download jars (example using Maven central patterns)
# NOTE: the exact artifact path depends on ICEBERG_VERSION & FLINK_MAJOR.
# The Iceberg docs show the pattern to download the correct runtime jar for the Flink major you use.
# (You may update ICEBERG_VERSION to the latest compatible one.)
RUN wget -q -P /opt/flink/plugins/iceberg \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_MAJOR}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_MAJOR}-${ICEBERG_VERSION}.jar \
 && wget -q -P /opt/flink/plugins/iceberg \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-presto/${FLINK_MAJOR}.1/flink-s3-fs-presto-${FLINK_MAJOR}.1.jar \
 && wget -q -P /opt/flink/plugins/iceberg \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_MAJOR}.1/flink-sql-connector-kafka-${FLINK_MAJOR}.1.jar

    
# (Optional) copy local config files if you want to bake them into the image
COPY ./conf/flink-conf.yml /opt/flink/conf/config.yaml

USER root
RUN apt-get update && apt-get install -y python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && pip3 install apache-flink

RUN apt-get clean && rm -rf /var/lib/apt/lists/*
# Keep default entrypoint from base image
