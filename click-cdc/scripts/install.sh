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

echo "Removing cluster..."

minikube delete

echo "Starting cluster..."

minikube start

helm install strimzi-cluster-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator