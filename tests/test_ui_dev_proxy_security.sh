#!/usr/bin/env bash
# Static contract for the Vite-only loopback proxy security boundary.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
config="$ROOT/graph-ui/vite.config.ts"

for required in \
    'strictPort: true' \
    'bypass(req, res)' \
    '!uiDevOrigins.has(origin)' \
    'res.writeHead(403' \
    'headers: { Origin: uiBackendOrigin }'; do
    if ! grep -Fq "$required" "$config"; then
        echo "FAIL: Vite proxy security contract missing: $required" >&2
        exit 1
    fi
done

for origin in 'http://127.0.0.1:5173' 'http://localhost:5173'; do
    if ! grep -Fq "$origin" "$config"; then
        echo "FAIL: Vite proxy local development origin missing: $origin" >&2
        exit 1
    fi
done

echo "UI development proxy security contract passed"
