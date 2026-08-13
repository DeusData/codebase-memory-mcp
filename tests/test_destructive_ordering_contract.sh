#!/usr/bin/env bash
# Contract: irreversible work must never run before a step that can still abort.
#
# This exists because it destroyed a user's data. On 0.9.0 -> 0.10.2,
# `update -y` auto-confirmed deleting OTHER projects' indexes, removed two .db
# files (~59 MB), and THEN aborted on an interactive prompt it could not answer
# under a non-interactive shell (#1558). The run failed, and the data was
# already gone. The prompt that aborted has since been removed, but that fixed
# one instance rather than the rule.
#
# The rule: in both activation flows, the index deletion must come AFTER the
# agent-configuration step, so any failure that can still stop the run happens
# while the indexes are intact. Both flows already return early when agent
# configuration fails, so ordering is the whole guarantee.
#
# This is a source-order check rather than a behavioural one, deliberately: the
# property is "the destructive call sits late in this function", and reordering
# it is exactly the regression to catch. A behavioural test would need to drive
# a full activation to failure, which is far more machinery for a weaker signal.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLI="$ROOT/src/cli/cli.c"

[ -f "$CLI" ] || { echo "FAIL: cannot find $CLI" >&2; exit 1; }

python3 - "$CLI" <<'PY'
import re, sys

src = open(sys.argv[1], encoding="utf-8").read()
failures = []

# The two activation entry points that may delete indexes. Each is checked in
# isolation so a call in one cannot satisfy the contract for the other.
FLOWS = [
    ("cli_install_activate", "install"),
    ("cli_update_activate_binary", "update"),
]

def body_of(name):
    """Source text of a static function, from its signature to the next
    top-level closing brace."""
    m = re.search(r"^static int " + re.escape(name) + r"\(.*?\{", src, re.S | re.M)
    if not m:
        return None
    start = m.end()
    depth = 1
    i = start
    while i < len(src) and depth:
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
        i += 1
    return src[start:i]

for func, label in FLOWS:
    body = body_of(func)
    if body is None:
        # Renamed or restructured: fail loudly rather than pass vacuously.
        failures.append(f"{label}: could not find {func}() — update this contract "
                        f"rather than deleting it")
        continue

    delete_at = body.find("cbm_remove_indexes")
    if delete_at < 0:
        # No deletion in this flow at all is fine — nothing to order.
        continue

    # Key on the configuration CALL, not a local variable name: the two flows
    # spell the result differently (one stores agent_config_rc, the other tests
    # the call inline), and the call is the thing that must precede deletion.
    config_at = body.find("cbm_install_agent_configs")
    if config_at < 0:
        failures.append(f"{label}: deletes indexes but no agent-configuration step "
                        f"was found before it; the ordering guarantee cannot hold")
        continue

    if delete_at < config_at:
        failures.append(
            f"{label}: cbm_remove_indexes() runs BEFORE the agent-configuration\n"
            f"      step. A failure after that point destroys data the user was\n"
            f"      told would be rebuilt, on a run that then aborts (#1558).")

if failures:
    for f in failures:
        print("FAIL: " + f, file=sys.stderr)
    print(f"destructive-ordering contract FAILED with {len(failures)} violation(s)",
          file=sys.stderr)
    sys.exit(1)
print("PASS: index deletion runs after every step that can still abort the run")
PY
