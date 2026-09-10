#!/usr/bin/env bash
# The CodeAtlasWeb (graph-ui) leg: one script, every venue.
#
# CI's `test-ui` job calls exactly this, and so does anyone checking the
# frontend locally, so a green here means the same thing in both places:
#
#   1. npm ci with the same browser-download skip the product build uses
#      (the dev dependencies include playwright for the frontend's local
#      browser proofs; nothing in this leg launches a browser);
#   2. the frontend's vitest suite (render tests under jsdom);
#   3. its hardcoded-string scan and its promise scan;
#   4. its portable frozen acceptance checks (`test:acceptance`: every
#      frozen check except the two release-binding files, which tie the
#      recorded release report to the commits of the frontend's original
#      repository and cannot pass anywhere else);
#   5. a production build, the one the product embeds.
#
# Usage: scripts/ci/test-ui.sh          (from the repository root or anywhere)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/graph-ui"

echo "=== test-ui: install (no browser download) ==="
PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 npm ci

echo "=== test-ui: unit tests (vitest) ==="
npm run test:unit

echo "=== test-ui: style gate ==="
npm run check:style

echo "=== test-ui: promise scan ==="
npm run check:promises

echo "=== test-ui: frozen acceptance checks (portable set) ==="
npm run test:acceptance

echo "=== test-ui: production build ==="
npm run build

echo "=== test-ui: green ==="
