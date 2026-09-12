#!/usr/bin/env bash
set -euo pipefail

# Built from the examples workspace, which is what the services are members of.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

docker build -f orders/1_base/api/Dockerfile -t crucible-example/orders-base-api:0.1 .
docker build -f orders/1_base/inventory/Dockerfile -t crucible-example/orders-base-inventory:0.1 .
