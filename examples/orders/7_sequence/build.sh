#!/usr/bin/env bash
set -euo pipefail

# Built from the examples workspace, which is what the services are members of.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

docker build -f orders/7_sequence/api/Dockerfile -t crucible-example/orders-sequence-api:0.1 .
docker build -f orders/7_sequence/inventory/Dockerfile -t crucible-example/orders-sequence-inventory:0.1 .
