#!/usr/bin/env bash

echo "Deploying Postgres & Kafka..."

./increment_version.sh 2

helm repo add bitnami https://charts.bitnami.com/bitnami
helm search repo bitnami/postgres

VERSION=$(cat ../version.txt)

helm dependency build ../templates/postgres
./update_chart_version.sh ../templates/postgres
helm upgrade --install postgres ../templates/postgres -f ../values/postgres/config.yaml

./update_chart_version.sh ../templates/debezium
helm upgrade --install debezium ../templates/debezium -f ../values/kafka/config.yaml