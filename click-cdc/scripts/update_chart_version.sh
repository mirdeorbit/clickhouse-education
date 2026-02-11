#!/bin/bash

set -e

VERSION_FILE="../version.txt"

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <path-to-chart>"
  exit 1
fi

CHART_DIR="$1"
CHART_FILE="$CHART_DIR/Chart.yaml"

if [[ ! -f "$VERSION_FILE" ]]; then
  echo "Error: $VERSION_FILE not found in current directory"
  exit 1
fi

if [[ ! -f "$CHART_FILE" ]]; then
  echo "Error: Chart.yaml not found in $CHART_DIR"
  exit 1
fi

VERSION=$(tr -d '[:space:]' < "$VERSION_FILE")

if [[ -z "$VERSION" ]]; then
  echo "Error: version is empty"
  exit 1
fi

echo "Updating $CHART_FILE to version: $VERSION"

if sed --version >/dev/null 2>&1; then
  sed -i "s/^version:.*/version: $VERSION/" "$CHART_FILE"
  sed -i "s/^appVersion:.*/appVersion: \"$VERSION\"/" "$CHART_FILE"
else
  sed -i '' "s/^version:.*/version: $VERSION/" "$CHART_FILE"
  sed -i '' "s/^appVersion:.*/appVersion: \"$VERSION\"/" "$CHART_FILE"
fi

echo "Chart.yaml updated successfully"
