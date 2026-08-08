#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

docker build -f api/Dockerfile -t crucible-example/pdns-api:0.1 ..
docker build -t crucible-example/pdns-db:0.1 db/
docker build -t crucible-example/pdns-ns:0.1 ns/
