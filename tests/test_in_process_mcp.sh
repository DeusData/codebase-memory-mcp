#!/usr/bin/env bash
# test_in_process_mcp.sh — CBM_IN_PROCESS writes no shared state, and serves a
# read-only cache (#2072). Requested by the maintainer on PR #2072 as the
# conditions for the opt-in mode: an in-process client must never write shared
# cache state — no daemon coordination files, no cohort or lifetime locks, and
# no writes to another process's index — so it cannot disturb a daemon running
# alongside it; and it must work on a read-only cache without ever chmodding
# it, since read-only mounts and sandboxes are the reason the mode exists.
#
# Drives the REAL binary against a private runtime/cache (scripts/test-runtime.sh)
# in five legs, all in tests/test_in_process_mcp.py:
#   - fresh state, no daemon: every mutating tool is attempted, the runtime,
#     cache and HOME directories must still be empty afterwards, and the cache
#     root's mode/ctime/ACL unchanged; caches that are not owner-only, or
#     missing, are refused before initialize and left as they were;
#   - a live daemon (watcher off) owning real indexes: an in-process session
#     reads them with every tool it serves and attempts every mutation, and
#     every entry under those directories must be byte- and metadata-identical
#     afterwards, with no socket or runtime-directory descriptor held;
#   - the same once a daemon-side write has left an index in WAL mode, where
#     only SQLite's reader -shm (and an empty -wal) may appear;
#   - a read-only (0500/0400) copy of that cache, and the same cache on a
#     genuinely read-only mount (a disk image on macOS, a bind mount in a user
#     namespace on Linux; SKIP with the reason where neither is available):
#     every read tool must succeed, and nothing may change.
# A CBM_IN_PROCESS=false positive control proves the descriptor check can fail.
#
# Skipped on Windows-like shells: the descriptor checks need /proc or lsof.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${CBM_TEST_BINARY:-${ROOT}/build/c/codebase-memory-mcp}"

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) echo "skipping in-process isolation test on Windows"; exit 0 ;;
esac
[[ -x "${BINARY}" ]] || { echo "missing binary: ${BINARY}" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "python3 required" >&2; exit 2; }
command -v git >/dev/null 2>&1 || { echo "git required for fixture" >&2; exit 2; }

# shellcheck source=../scripts/test-runtime.sh
source "${ROOT}/scripts/test-runtime.sh"
cbm_test_runtime_init
cleanup() {
  # The driver detaches its disk image and restores write permission itself;
  # this covers a driver that died first, so the helper can remove the root.
  if [[ "$(uname -s)" == Darwin && -d "${CBM_TEST_RUNTIME_ROOT}/romnt" ]]; then
    hdiutil detach -quiet -force "${CBM_TEST_RUNTIME_ROOT}/romnt" >/dev/null 2>&1 || true
  fi
  chmod -R u+w "${CBM_TEST_RUNTIME_ROOT}" 2>/dev/null || true
  cbm_test_runtime_cleanup "${BINARY}"
}
trap cleanup EXIT

python3 "${ROOT}/tests/test_in_process_mcp.py" "${BINARY}" "${CBM_TEST_RUNTIME_ROOT}"
