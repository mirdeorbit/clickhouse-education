#!/usr/bin/env bash
set -e

echo "Exec query..."

QUERY=$@

echo "Replica 0: "

echo "$QUERY" | kubectl exec -i click-cdc-clickhouse-shard0-0 -- bash -c \
"clickhouse-client -u click_admin --password click_admin_password123"

printf '\n'

echo "Replica 1: "

printf '\n'

echo "$QUERY" | kubectl exec -i click-cdc-clickhouse-shard0-1 -- bash -c \
"clickhouse-client -u click_admin --password click_admin_password123"