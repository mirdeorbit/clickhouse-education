SCRIPT="
SELECT * FROM cdc.postgres_public_polygons;
"
echo "REPLICA 0 data :::"

echo "$SCRIPT" | kubectl exec -i click-cdc-clickhouse-shard0-0 -- bash -c \
"clickhouse-client -u click_admin --password click_admin_password123"

echo "REPLICA 1 data :::"

echo "$SCRIPT" | kubectl exec -i click-cdc-clickhouse-shard0-1 -- bash -c \
"clickhouse-client -u click_admin --password click_admin_password123"