#!/usr/bin/env bash
set -e

echo "Exec query..."

QUERY=$@

echo "$QUERY" | kubectl exec -i postgres-0 -- bash -c \
"PGPASSWORD='pass123' psql -v ON_ERROR_STOP=1 -U postgres -d delivery"