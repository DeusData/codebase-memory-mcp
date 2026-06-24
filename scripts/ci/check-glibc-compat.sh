#!/usr/bin/env bash
# Regression guard: the linux binary we ship must START on OLD glibc.
#
# The standard linux release binary dynamically links glibc 2.38+ / GLIBCXX
# 3.4.32 and fails to start on Debian 11, RHEL/Rocky 8, Ubuntu 20.04, Amazon
# Linux 2, etc. The fix points all linux install + self-update paths at the
# fully-static "-portable" asset. This runs a given linux binary inside an
# old-glibc container (debian:bullseye, glibc 2.31) and asserts it starts:
#   - RED  for the dynamic standard binary  (GLIBC_2.38 not found)
#   - GREEN for the static -portable binary (runs anywhere)
#
# Usage: check-glibc-compat.sh <path-to-linux-binary>
# Env:   GLIBC_TEST_IMAGE  (default: debian:bullseye-slim, glibc 2.31)
set -euo pipefail

BIN="${1:?usage: check-glibc-compat.sh <path-to-linux-binary>}"
IMAGE="${GLIBC_TEST_IMAGE:-debian:bullseye-slim}"
BIN_ABS="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"

echo "==> running $(basename "$BIN") --version inside ${IMAGE} (glibc 2.31)"
docker run --rm -v "${BIN_ABS}:/cbm:ro" "${IMAGE}" /cbm --version
echo "PASS: binary starts on old glibc (${IMAGE})"
