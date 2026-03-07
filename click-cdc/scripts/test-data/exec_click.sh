#!/usr/bin/env bash
set -euo pipefail

echo "Exec query..."

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage:"
  echo "  $0 \"SQL_QUERY\" [0|1]"
  exit 1
fi

QUERY="$1"
TARGET="${2:-}"

run_query() {
  local replica=$1
  local pod="click-cdc-clickhouse-shard0-${replica}"

  echo "Replica ${replica}:"
  echo "$QUERY" | kubectl exec -i "$pod" -- bash -c \
    "clickhouse-client -u click_admin --password click_admin_password123"
  printf '\n'
}

# Если передана конкретная реплика
if [[ -n "$TARGET" ]]; then
  if [[ "$TARGET" != "0" && "$TARGET" != "1" ]]; then
    echo "Replica must be 0 or 1"
    exit 1
  fi

  run_query "$TARGET"
else
  # Иначе выполняем на обеих
  run_query 0
  run_query 1
fi