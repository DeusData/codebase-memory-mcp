#!/usr/bin/env bash
# test_in_process_mcp.sh — CBM_IN_PROCESS writes no shared state (#2072).
# Requested by the maintainer on PR #2072 as the condition for the opt-in mode:
# an in-process client must never write shared cache state — no daemon
# coordination files, no cohort or lifetime locks, and no writes to another
# process's index — so it cannot disturb a daemon running alongside it.
#
# Drives the REAL binary against a private runtime/cache (scripts/test-runtime.sh)
# in three legs, all in tests/test_in_process_mcp.py:
#   - fresh state, no daemon: every mutating tool is attempted, and the runtime,
#     cache and HOME directories must still be empty afterwards;
#   - a live daemon owning real indexes: an in-process session reads them with
#     every tool it serves and attempts every mutation, and every entry under
#     those directories must be byte- and metadata-identical afterwards, with no
#     socket or runtime-directory descriptor held while the session was live;
#   - the same once a daemon-side write has left an index in WAL mode, where
#     only SQLite's reader -shm (and an empty -wal) may appear.
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
  cbm_test_runtime_cleanup "${BINARY}"
}
trap cleanup EXIT

python3 "${ROOT}/tests/test_in_process_mcp.py" "${BINARY}" "${CBM_TEST_RUNTIME_ROOT}"
