FROM quay.io/strimzi/kafka:0.50.0-kafka-4.1.1
USER root:root

ENV FILENAME=debezium-connector-postgres-3.4.0.CR1-plugin.tar.gz

RUN curl --output ./${FILENAME} https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.4.0.CR1/${FILENAME}
RUN mv ./${FILENAME} /opt/kafka/plugins/
USER 1001