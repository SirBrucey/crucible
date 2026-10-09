#!/usr/bin/env bash
set -euo pipefail

# Built from the examples workspace, which is what the services are members of.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

docker build -f orders/6_confirm/api/Dockerfile -t crucible-example/orders-confirm-api:0.1 .
docker build -f orders/6_confirm/inventory/Dockerfile -t crucible-example/orders-confirm-inventory:0.1 .
