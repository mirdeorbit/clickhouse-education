#!/usr/bin/env bash
set -e

echo "Uninstalling components..."

ALL_SERVICES=("postgres" "click-cdc" "debezium")
TARGETS=($@)

prev=""

if [ ${#TARGETS} -eq 0 ]; then
  TARGETS=("${ALL_SERVICES[@]}")
fi

uninstall_postgres() {
  helm delete postgres | true
  kubectl delete pvc data-postgres-0 | true
}

uninstall_cdc() {
  helm delete click-cdc | true
  kubectl delete pvc data-click-cdc-clickhouse-keeper-0 | true
  kubectl delete pvc data-click-cdc-clickhouse-shard0-0 | true
  kubectl delete pvc data-click-cdc-clickhouse-shard0-1 | true
}

uninstall_operator() {
  helm delete strimzi-cluster-operator | true 
}

uninstall_debezium() {
  helm delete debezium | true
}

for target in "${TARGETS[@]}"; do
  case "$target" in
    postgres) uninstall_postgres ;;
    click-cdc) uninstall_cdc ;;
    debezium) uninstall_debezium ;;
    operator) uninstall_operator ;;
    *)
      echo "Unknown target: $target"
      exit 1
      ;;
  esac
done

echo "Done."