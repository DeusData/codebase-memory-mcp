#!/usr/bin/env bash
# Activation-refusal diagnostics must never point at evidence they do not show.
#
# #1416 and #1537 both landed because a refusal printed generic text and told
# the reader to "check the errors above" when nothing was above. The fix was
# cli_activation_diagnostic(): it appends the transaction refusal note, or
# cbm_daemon_ipc_validation_detail(), so the message names the check that
# actually refused.
#
# #1856 showed the fix was incomplete in the way that matters: the property was
# repaired on the paths that had tests and left broken on a sibling call site
# that had none. cli_activation_production_context_init() -- the emitter that
# validates the cache, rendezvous and log directories, i.e. the one MOST likely
# to hold a useful detail -- printed the constant bare. A reporter with a full
# ProcMon trace still could not tell which check refused, because the binary
# discarded the answer before printing.
#
# So the guard here is the CLASS, not the one call site: every emitter of the
# refusal constants goes through the attributing helper. A future emitter that
# bypasses it fails this test instead of reaching a reporter.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

python3 - "$ROOT" <<'PY'
from __future__ import annotations

import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
failures: list[str] = []


def require(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


cli = (root / "src" / "cli" / "cli.c").read_text(encoding="utf-8")

# 1. Every call that passes a refusal constant to a diagnostic sink must use the
#    attributing helper. cli_activation_production_diagnostic is the raw sink --
#    legitimate as an ops->visible_diagnostic member (it receives the ALREADY
#    attributed string), never as a direct emitter of the constant.
ATTRIBUTING = "cli_activation_diagnostic"
CONSTANTS = ("CLI_ACTIVATION_REFUSED_MESSAGE", "CLI_ACTIVATION_BUSY_MESSAGE")

for call in re.finditer(r"(\w*diagnostic)\s*\(([^;]*?)\)\s*;", cli, re.S):
    callee, arguments = call.group(1), call.group(2)
    if not any(constant in arguments for constant in CONSTANTS):
        continue
    if callee == ATTRIBUTING:
        continue
    line = cli.count("\n", 0, call.start()) + 1
    require(
        False,
        f"src/cli/cli.c:{line}: {callee}() emits a refusal constant directly. "
        f"Route it through {ATTRIBUTING}() so the message names the check that "
        "refused instead of pointing at errors that were never printed (#1856)",
    )

# 2. The helper must still be the one that appends both attribution channels --
#    deleting either turns the message back into a dead end.
helper = re.search(
    r"static void cli_activation_diagnostic\((?:.|\n)*?\n\}\n", cli
)
require(helper is not None, "cli_activation_diagnostic() not found in src/cli/cli.c")
body = helper.group(0) if helper else ""
require(
    "cbm_activation_transaction_refusal_note()" in body,
    "cli_activation_diagnostic() must append the transaction refusal note (#1416)",
)
require(
    "cbm_daemon_ipc_validation_detail()" in body,
    "cli_activation_diagnostic() must append the daemon validation detail (#1537)",
)

# 3. The remedy must be one that works. `icacls /remove:g` silently does nothing
#    to an INHERITED ACE -- and the stock C:\ grant for Authenticated Users
#    reaches every new child directory exactly that way, which is the shape
#    #1856 was reported against. Advice that cannot fix the reported case is
#    worse than none: the command succeeds and the refusal persists.
require(
    "/inheritance:r" in body,
    "the refusal remedy must name icacls /inheritance:r for an inherited grant "
    "-- /remove:g cannot remove one, so the advice fails on the stock C:\\ ACE "
    "that #1856 reported",
)

# 4. And the refusal itself must say which of the two shapes it hit, or the
#    reader cannot choose between those commands.
transaction = (root / "src" / "cli" / "activation_transaction.c").read_text(encoding="utf-8")
require(
    re.search(r"AceFlags\s*&\s*INHERITED_ACE", transaction) is not None,
    "src/cli/activation_transaction.c must report whether the refusing ACE was "
    "inherited, so the printed remedy matches the grant (#1856)",
)

if failures:
    print("Activation diagnostic contract FAILED:")
    for failure in failures:
        print(f"  - {failure}")
    sys.exit(1)

print("Activation diagnostic contract passed")
PY
