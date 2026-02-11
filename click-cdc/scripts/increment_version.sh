#!/bin/bash

FILE="../version.txt"

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 {0|1|2}"
  exit 1
fi

PART=$1

if [[ "$PART" != "0" && "$PART" != "1" && "$PART" != "2" ]]; then
  echo "Error: argument must be 0 (major), 1 (minor) or 2 (patch)"
  exit 1
fi

if [[ ! -f "$FILE" ]]; then
  echo "Error: $FILE not found"
  exit 1
fi

VERSION=$(cat "$FILE")

IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION"

if [[ -z "$MAJOR" || -z "$MINOR" || -z "$PATCH" ]]; then
  echo "Error: invalid version format"
  exit 1
fi

case "$PART" in
  0)
    ((MAJOR++))
    MINOR=0
    PATCH=0
    ;;
  1)
    ((MINOR++))
    PATCH=0
    ;;
  2)
    ((PATCH++))
    ;;
esac

NEW_VERSION="$MAJOR.$MINOR.$PATCH"

echo "$NEW_VERSION" > "$FILE"

echo "Version updated: $VERSION → $NEW_VERSION"
