#!/usr/bin/env bash
set -euo pipefail

# Built from the examples workspace, which is what the services are members of.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

docker build -f orders/5_ack_on_success/api/Dockerfile -t crucible-example/orders-ack-api:0.1 .
docker build -f orders/5_ack_on_success/inventory/Dockerfile -t crucible-example/orders-ack-inventory:0.1 .
