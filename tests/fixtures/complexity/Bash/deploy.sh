#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

./scripts/build.sh
kubectl apply -f manifests/
helm upgrade --install web ./charts/web
