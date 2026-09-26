#!/usr/bin/env bash
set -euo pipefail

# Built from the examples workspace, which is what the services are members of.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

docker build -f orders/4_reconnect/api/Dockerfile -t crucible-example/orders-reconnect-api:0.1 .
docker build -f orders/4_reconnect/inventory/Dockerfile -t crucible-example/orders-reconnect-inventory:0.1 .
