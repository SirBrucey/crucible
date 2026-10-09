#!/usr/bin/env bash
set -euo pipefail

# Built from the examples workspace, which is what the services are members of.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

docker build -f orders/3_inbox/api/Dockerfile -t crucible-example/orders-inbox-api:0.1 .
docker build -f orders/3_inbox/inventory/Dockerfile -t crucible-example/orders-inbox-inventory:0.1 .
