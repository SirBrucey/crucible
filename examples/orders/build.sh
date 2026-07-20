#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

docker build -f api/Dockerfile -t crucible-example/orders-api:0.1 ..
docker build -f inventory/Dockerfile -t crucible-example/orders-inventory:0.1 ..
