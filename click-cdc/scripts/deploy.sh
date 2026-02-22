#!/usr/bin/env bash
set -e

echo "Deploying components..."

ALL_SERVICES=("postgres" "click-cdc" "debezium")
TARGETS=()
UNINSTALL_ALL=false
UNINSTALL_LIST=()

PG_VALUES="-f ../values/postgres/config.yaml"
CLICK_VALUES="-f ../values/click-cdc/config.yaml"
KAFKA_VALUES="-f ../values/debezium/config.yaml"

prev=""

for arg in "$@"; do
  if [[ "$arg" == "uninstall" ]]; then
    if [[ -z "$prev" ]]; then
      UNINSTALL_ALL=true
    else
      UNINSTALL_LIST+=("$prev")
    fi
  else
    TARGETS+=("$arg")
    prev="$arg"
  fi
done

if [ ${#TARGETS[@]} -eq 0 ]; then
  TARGETS=("${ALL_SERVICES[@]}")
fi


retry() {
  local attempts=$1
  local delay=$2
  shift 2

  local count=1
  until "$@"; do
    if [ $count -ge $attempts ]; then
      echo "❌ Command failed after $count attempts: $*"
      return 1
    fi

    echo "⚠️ Attempt $count failed. Retrying in ${delay}s..."
    sleep "$delay"
    count=$((count + 1))
  done
}

contains() {
  local match=$1
  shift
  for item in "$@"; do
    [[ "$item" == "$match" ]] && return 0
  done
  return 1
}

uninstall_release() {
  local name=$1
  echo "Uninstalling $name..."
  helm uninstall "$name" --no-hooks || true
}

echo "Incrementing version..."
./increment_version.sh 2

helm repo add bitnami https://charts.bitnami.com/bitnami
helm search repo bitnami/postgres
helm search repo bitnami/clickhouse

install_postgres() {
  ./update_chart_version.sh ../templates/postgres
  retry 3 5 helm dependency build ../templates/postgres
  retry 3 5 helm upgrade --install postgres ../templates/postgres $PG_VALUES
}

install_cdc() {
  ./update_chart_version.sh ../templates/click-cdc
  retry 3 5 helm dependency build ../templates/click-cdc
  retry 3 5 helm upgrade --install click-cdc ../templates/click-cdc $CLICK_VALUES
}

install_debezium() {
  ./update_chart_version.sh ../templates/debezium
  retry 3 5 helm upgrade --install debezium ../templates/debezium $PG_VALUES $CLICK_VALUES $KAFKA_VALUES
}

if $UNINSTALL_ALL; then
  echo "Global uninstall requested..."
  for svc in "${ALL_SERVICES[@]}"; do
    uninstall_release "$svc"
  done
fi

for target in "${TARGETS[@]}"; do
  if contains "$target" "${UNINSTALL_LIST[@]}"; then
    uninstall_release "$target"
  fi

  case "$target" in
    postgres) install_postgres ;;
    click-cdc) install_cdc ;;
    debezium) install_debezium ;;
    *)
      echo "Unknown target: $target"
      exit 1
      ;;
  esac
done

echo "Done."