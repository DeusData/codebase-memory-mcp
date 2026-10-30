#!/usr/bin/env bash
# Regression contract for clang-tidy's `,-warnings-as-errors` diagnostic suffix.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/lint-mem-gate.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT

cat > "$FIX/sample.c" <<'EOF'
int sample(int *value) {
    return *value;
}
EOF

cat > "$FIX/whitelist.txt" <<EOF
## $FIX/sample.c :: sample :: clang-analyzer-core.NullDereference
segment-sha256: PLACEHOLDER
why: |
  The test deliberately models a diagnostic that is suppressed by the fixture whitelist.
  This text is long enough to exercise the gate's normal argument validation path.
tried: |
  The fixture is intentionally minimal and only verifies diagnostic parsing.
EOF

HASH="$(LINT_MEM_WHITELIST="$FIX/whitelist.txt" python3 "$ROOT/scripts/lint-mem-gate.py" --hash "$FIX/sample.c" sample | awk '/^segment-sha256:/ {print $2}')"
python3 - "$FIX/whitelist.txt" "$HASH" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8").replace("PLACEHOLDER", sys.argv[2])
path.write_text(text, encoding="utf-8")
PY

printf '%s\n' "$FIX/sample.c:2:12: warning: possible null dereference [clang-analyzer-core.NullDereference,-warnings-as-errors]" \
  | LINT_MEM_WHITELIST="$FIX/whitelist.txt" python3 "$ROOT/scripts/lint-mem-gate.py" 2> "$FIX/stderr"

grep -q 'memory gate clean (1 argued false positive' "$FIX/stderr"
echo 'lint-mem-gate warning suffix contract passed'
