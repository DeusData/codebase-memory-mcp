#!/usr/bin/env bash
# Regression guard: the linux binary we ship must START on OLD glibc.
#
# The standard linux release binary is built in a pinned Ubuntu 22.04 container
# and dynamically links glibc / libstdc++ with a 2.35 / GLIBCXX_3.4.30 floor
# (asserted on the built bytes by check-binary-composition.sh A6), so it fails
# to start on Debian 11, RHEL/Rocky 8, Ubuntu 20.04, Amazon Linux 2, etc. All
# linux install + self-update paths therefore use the fully-static "-portable"
# asset. This runs a given linux binary inside an old-glibc container
# (debian:bullseye, glibc 2.31) and asserts it starts:
#   - RED  for the dynamic standard binary  (GLIBC_2.3x not found)
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
