#!/bin/sh
set -e

# REGISTRY="docker.io"
# NAMESPACE="mirdeorbit"
# IMAGE_NAME="postgres-custom"
# TAG="1.0"

# FULL_IMAGE_NAME="$REGISTRY/$NAMESPACE/$IMAGE_NAME:$TAG"

# echo "****" | docker login $REGISTRY -u $NAMESPACE --password-stdin

# echo "Building image: $FULL_IMAGE_NAME"
# docker build -t "$FULL_IMAGE_NAME" ./postgres

# echo "Pushing image to registry"
# docker push "$FULL_IMAGE_NAME"

# echo "Done"

REGISTRY="docker.io"
NAMESPACE="mirdeorbit"
IMAGE_NAME_DEBEZIUM="debezium-connect"
TAG="1.4"

FULL_IMAGE_NAME_DEBEZIUM="$REGISTRY/$NAMESPACE/$IMAGE_NAME_DEBEZIUM:$TAG"

echo "Building image: $FULL_IMAGE_NAME_DEBEZIUM"
docker build -t "$FULL_IMAGE_NAME_DEBEZIUM" ./debezium-connect --no-cache --progress=plain

echo "Pushing image to registry"
docker push "$FULL_IMAGE_NAME_DEBEZIUM"

echo "Done"