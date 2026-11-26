#!/usr/bin/env bash
# Exercise the real source audit against isolated subprocess-call fixtures.
# Wrapper allowances must cover cbm_popen and cbm_popen_git, while raw popen
# requires its own allowance. Identifier substrings must match neither family.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$ROOT" "$BASH" <<'PY'
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

root = Path(sys.argv[1])
bash_path = sys.argv[2]
failures = []
passed = 0

with tempfile.TemporaryDirectory(prefix="cbm-security-subprocess-") as directory:
    fixture = Path(directory)
    (fixture / "scripts").mkdir()
    (fixture / "src").mkdir()
    for name in ("security-audit.sh", "security-network-source-audit.py"):
        shutil.copy2(root / "scripts" / name, fixture / "scripts" / name)

    def check(label, source, allowances, status, required, forbidden=()):
        global passed
        (fixture / "src" / "probe.c").write_text(source, encoding="utf-8")
        (fixture / "scripts" / "security-allowlist.txt").write_text(
            allowances, encoding="utf-8"
        )
        result = subprocess.run(
            [bash_path, str(fixture / "scripts" / "security-audit.sh")],
            cwd=fixture, text=True, capture_output=True,
        )
        output = result.stdout + result.stderr
        if (result.returncode == status
                and all(marker in output for marker in required)
                and all(marker not in output for marker in forbidden)):
            print(f"PASS: {label}")
            passed += 1
        else:
            failures.append(label)
            print(f"FAIL: {label}: exit {result.returncode}, expected {status}")
            for marker in required:
                if marker not in output:
                    print(f"  missing: {marker}")
            for marker in forbidden:
                if marker in output:
                    print(f"  unexpected: {marker}")
            for line in output.splitlines():
                if line.startswith(("BLOCKED:", "REVIEW:", "FAIL:", "policy-error:")):
                    print(f"  {line}")

    wrapper_blocked = "BLOCKED: src/probe.c: contains cbm_popen()"
    raw_blocked = "BLOCKED: src/probe.c: contains popen()"
    audited = "=== Security audit passed ==="
    time_review = "time-related call near dangerous function"
    wrapper_allowance = "src/probe.c:cbm_popen:reviewed synthetic wrapper fixture\n"
    raw_allowance = "src/probe.c:popen:reviewed synthetic raw-call fixture\n"

    for name in ("cbm_popen", "cbm_popen_git"):
        source = f'void probe(void) {{ {name}("fixture"); }}\n'
        check(f"unreviewed {name} is blocked", source, "", 1,
              (wrapper_blocked,), (raw_blocked,))
        check(f"{name} uses only the wrapper allowance", source,
              wrapper_allowance, 0, (audited,), ("BLOCKED:",))
        check(f"raw allowance cannot authorize {name}", source,
              raw_allowance, 1, (wrapper_blocked,), (raw_blocked,))
        spaced = f'void probe(void) {{\n{name} ("fixture");\n}}\n'
        check(f"{name} at column one with call whitespace is blocked", spaced,
              "", 1, (wrapper_blocked,), (raw_blocked,))

    raw_source = 'void probe(void) { popen("fixture", "r"); }\n'
    check("unreviewed raw popen remains blocked", raw_source, "", 1,
          (raw_blocked,), (wrapper_blocked,))
    check("wrapper allowance cannot authorize raw popen", raw_source,
          wrapper_allowance, 1, (raw_blocked,), (wrapper_blocked,))
    check("reviewed raw popen is allowed", raw_source, raw_allowance,
          0, (audited,), ("BLOCKED:",))
    check("raw popen at column one with call whitespace is blocked",
          'void probe(void) {\npopen ("fixture", "r");\n}\n', "", 1,
          (raw_blocked,), (wrapper_blocked,))

    lookalikes = (
        "my_popen", "MY_popen", "prefix9popen", "_popen",
        "my_cbm_popen", "my_cbm_popen_git", "cbm_popen_git_extra",
    )
    source = "void probe(void) {\n" + "".join(
        f'    {name}("fixture");\n' for name in lookalikes
    ) + "}\n"
    check("identifier substrings do not match subprocess functions", source,
          "", 0, (audited,), ("BLOCKED:",))

    for name in ("cbm_popen", "cbm_popen_git"):
        source = f'void probe(void) {{\n    time(0);\n    {name}("fixture");\n}}\n'
        check(f"time proximity includes {name}", source, wrapper_allowance,
              0, (audited, time_review), ("BLOCKED:",))
    source = 'void probe(void) { time(0); my_popen("fixture"); }\n'
    check("time proximity excludes identifier substrings", source, "", 0,
          (audited,), ("BLOCKED:", time_review))

print(f"=== subprocess source audit: {passed} passed, {len(failures)} failed ===")
sys.exit(1 if failures else 0)
PY
