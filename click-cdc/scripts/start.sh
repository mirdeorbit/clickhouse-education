#!/usr/bin/env bash

echo "Deploying Postgres & Kafka..."

helm uninstall postgres
helm upgrade --install postgres oci://registry-1.docker.io/bitnamicharts/postgresql -f ../values/postgres/config.yaml

helm uninstall debezium
helm upgrade --install debezium ../templates/debezium -f ../values/kafka/config.yaml