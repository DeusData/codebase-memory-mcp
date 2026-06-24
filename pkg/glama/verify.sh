#!/usr/bin/env bash
# Build the Glama check image and confirm the MCP stdio server starts and
# answers initialize + tools/list with no project indexed — exactly what
# Glama's directory check does. Used locally and by the CI smoke suite.
#
# Env:
#   IMAGE        image tag to build (default: cbm-glama-check)
#   DOCKER_BUILD_ARGS  extra args for `docker build` (e.g. --platform linux/amd64)
set -euo pipefail

IMAGE="${IMAGE:-cbm-glama-check}"
DIR="$(cd "$(dirname "$0")" && pwd)"

run_with_timeout() {
  local t="$1"; shift
  if command -v timeout >/dev/null 2>&1; then timeout "$t" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then gtimeout "$t" "$@"
  else "$@"; fi
}

echo "==> building ${IMAGE} (${DIR}/Dockerfile)"
# shellcheck disable=SC2086
docker build ${DOCKER_BUILD_ARGS:-} -f "${DIR}/Dockerfile" -t "${IMAGE}" "${DIR}"

echo "==> MCP introspection handshake (initialize -> initialized -> tools/list)"
REQ="$(printf '%s\n%s\n%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"glama-verify","version":"1"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}')"
OUT="$(printf '%s' "${REQ}" | run_with_timeout 60 docker run -i --rm "${IMAGE}" || true)"

printf '%s\n' "${OUT}"

echo "==> assertions"
printf '%s' "${OUT}" | grep -q '"result"'     || { echo "FAIL: no JSON-RPC result (server did not respond)"; exit 1; }
printf '%s' "${OUT}" | grep -q 'search_graph' || { echo "FAIL: tools/list missing expected tool 'search_graph'"; exit 1; }
COUNT="$(printf '%s' "${OUT}" | grep -o '"name"' | wc -l | tr -d ' ')"
echo "PASS: server started and introspected; ~${COUNT} name entries (>=14 tools expected)"
