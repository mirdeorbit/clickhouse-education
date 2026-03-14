#!/usr/bin/env bash
set -e

echo "Uninstalling components..."

ALL_SERVICES=("postgres" "click-cdc" "debezium")
TARGETS=($@)

prev=""

if [ ${#TARGETS} -eq 0 ]; then
  TARGETS=("${ALL_SERVICES[@]}")
fi

KAFKA_DIRS=(
  /tmp/minikube/kafka/replica0
  /tmp/minikube/kafka/replica1
  /tmp/minikube/kafka/replica2
)

uninstall_postgres() {
  helm delete postgres | true
  kubectl delete pvc data-postgres-0 | true
}

uninstall_cdc() {
  helm delete click-cdc | true
  kubectl delete pvc data-click-cdc-clickhouse-shard0-0 | true
  kubectl delete pvc data-click-cdc-clickhouse-shard0-1 | true
  kubectl delete pv data-click-cdc-clickhouse-shard0-0-pv | true
  kubectl delete pv data-click-cdc-clickhouse-shard0-1-pv | true
}

uninstall_operator() {
  helm delete strimzi-cluster-operator | true 
}

uninstall_debezium() {
  helm delete debezium | true
  kubectl delete pvc data-0-kafka-debezium-cluster-kafka-debezium-node-pool-0-pv | true
  kubectl delete pvc data-0-kafka-debezium-cluster-kafka-debezium-node-pool-1-pv | true
  kubectl delete pvc data-0-kafka-debezium-cluster-kafka-debezium-node-pool-2-pv | true
  kubectl delete pv data-0-kafka-debezium-cluster-kafka-debezium-node-pool-0 | true
  kubectl delete pv data-0-kafka-debezium-cluster-kafka-debezium-node-pool-1 | true
  kubectl delete pv data-0-kafka-debezium-cluster-kafka-debezium-node-pool-2 | true

  for DIR in "${KAFKA_DIRS[@]}"; do
    echo "Cleaning contents of $DIR"
    
    rm -rf "${DIR:?}/"*
  done
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