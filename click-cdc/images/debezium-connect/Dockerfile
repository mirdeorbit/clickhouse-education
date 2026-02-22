FROM quay.io/strimzi/kafka:0.50.0-kafka-4.1.1
USER root:root

ENV VERSION=3.4.0
ENV FILENAME=debezium-connector-postgres-${VERSION}.CR1-plugin.tar.gz
ENV PLUGIN_DIR=debezium-connector-postgres

RUN curl --output ./${FILENAME} https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${VERSION}.CR1/${FILENAME}
RUN mkdir -p ./plugins/${PLUGIN_DIR}
RUN tar -xvzf ./${FILENAME} -C ./plugins/${PLUGIN_DIR}
RUN rm ${FILENAME}

ENV CLICK_PLUGIN_VERSION=1.3.5
ENV CLICK_PLUGIN_FILENAME=clickhouse-kafka-connect-v${CLICK_PLUGIN_VERSION}.zip
ENV CLICK_PLUGIN_DIR=debezium-connector-clickhouse

RUN curl -OL https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v${CLICK_PLUGIN_VERSION}/${CLICK_PLUGIN_FILENAME}
RUN mkdir -p ./plugins/${CLICK_PLUGIN_DIR}
RUN unzip ./${CLICK_PLUGIN_FILENAME} -d ./plugins/${CLICK_PLUGIN_DIR}
RUN rm ${CLICK_PLUGIN_FILENAME}

USER 1001