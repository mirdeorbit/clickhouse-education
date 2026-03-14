#!/usr/bin/env bash
set -e

ARCH="arm64"
OS="darwin"
HELM_VERSION="v4.1.0"

# -------- HELM --------
if command -v helm >/dev/null 2>&1; then
  echo "Helm already installed"
else
  echo "Install Helm..."
  curl https://get.helm.sh/helm-${HELM_VERSION}-${OS}-${ARCH}.tar.gz -o helm.tar.gz
  tar -zxvf helm.tar.gz
  mv ${OS}-${ARCH}/helm /usr/local/bin/helm
  chmod +x /usr/local/bin/helm
  rm -rf helm.tar.gz ${OS}-${ARCH}
  echo "Helm installed"
fi

if command -v minikube >/dev/null 2>&1; then
  echo "Minikube already installed"
else
  echo "Install Minikube..."
  curl -LO https://github.com/kubernetes/minikube/releases/latest/download/minikube-${OS}-${ARCH}
  install minikube-darwin-arm64 /usr/local/bin/minikube
  rm minikube
  echo "Minikube installed"
fi

if ! docker ps &> /dev/null; then
    echo "Error: Docker is not running!"
    exit 1
fi

PARENT_DIR_NAME=$(basename $(dirname $PWD))
MINIKUBE_HOME=${PARENT_DIR_NAME}/config/kube

DIRS=(
  /tmp/minikube/clickhouse/shard0
  /tmp/minikube/clickhouse/shard1
  /tmp/minikube/kafka/replica0
  /tmp/minikube/kafka/replica1
  /tmp/minikube/kafka/replica2
)

for DIR in "${DIRS[@]}"; do
  if [ ! -d "$DIR" ]; then
    echo "Creating $DIR"
    mkdir -p "$DIR"
  else
    echo "$DIR already exists"
  fi

  echo "Cleaning contents of $DIR"
  
  rm -rf "${DIR:?}/"*

  echo "Setting permissions on $DIR"
  chmod -Rf 777 "$DIR"
done

echo "Done creation directories..."

echo "Starting cluster..."

minikube start --memory 16384 --mount-string="/tmp/minikube:/tmp/minikube" --mount

STRIMZI_VERSION="0.51.0"
KAFKA_VERSION="4.1.1"

echo "Pulling clickhouse images..."
minikube image load docker.io/bitnamilegacy/clickhouse:25.7.5-debian-12-r0
minikube image load docker.io/bitnamilegacy/clickhouse-keeper:25.7.5-debian-12-r0

echo "Pulling postgres images..."
minikube image load registry-1.docker.io/mirdeorbit/postgres-custom:1.0

echo "Pulling debezium images..."
minikube image load quay.io/strimzi/operator:${STRIMZI_VERSION}
minikube image load quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}
minikube image load docker.io/mirdeorbit/debezium-connect:1.7.10


echo "Upgrading strimzi operator..."
helm delete strimzi-cluster-operator | true
sleep 7
helm upgrade --install strimzi-cluster-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator --set defaultImageTag=${STRIMZI_VERSION} --version ${STRIMZI_VERSION}