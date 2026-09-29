#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

case "$*" in
-h | --help)
    cat <<'EOF'
Usage: bash scripts/ci/test-frontend.sh

Install the locked frontend dependencies, run Vitest, and build the production
UI. Requires Node.js 22+ and npm; CI tests Node.js 22 and 24.
EOF
    exit 0
    ;;
"") ;;
*)
    echo "test-frontend.sh: unexpected arguments '$*'. Please consult --help." >&2
    exit 2
    ;;
esac

cd "$ROOT/graph-ui"
npm ci
npm test
npm run build