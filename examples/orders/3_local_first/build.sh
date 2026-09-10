#!/usr/bin/env bash
set -euo pipefail

# Built from the repository root: the API carries crucible's span adapter, so
# the build context has to hold it as well as the example.
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."

docker build -f examples/orders/3_local_first/api/Dockerfile -t crucible-example/orders-local-api:0.1 .
docker build -f examples/orders/3_local_first/inventory/Dockerfile -t crucible-example/orders-local-inventory:0.1 .
