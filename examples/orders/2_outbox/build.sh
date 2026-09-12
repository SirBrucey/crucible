#!/usr/bin/env bash
set -euo pipefail

# Built from the examples workspace, which is what the services are members of.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

docker build -f orders/2_outbox/api/Dockerfile -t crucible-example/orders-outbox-api:0.1 .
docker build -f orders/2_outbox/inventory/Dockerfile -t crucible-example/orders-outbox-inventory:0.1 .
