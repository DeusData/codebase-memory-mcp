#!/usr/bin/env python3
"""License-gate policy check over a ScanCode Toolkit JSON scan.

Usage: license-gate-check.py <scan.json> <license-policy.json>

Fails (exit 1) if ANY scanned file carries a detected license expression
containing an SPDX id outside the policy allow-list — one finding is enough.
"""
import json
import re
import sys


def main():
    scan_path, policy_path = sys.argv[1], sys.argv[2]
    with open(policy_path) as fh:
        policy = json.load(fh)
    allowed = {x.lower() for x in policy["allowed_spdx_ids"]}
    ignored_paths = tuple(policy.get("ignored_paths", []))
    skip_tokens = {"and", "or", "with", ""}

    with open(scan_path) as fh:
        scan = json.load(fh)

    violations = []
    checked = 0
    for f in scan.get("files", []):
        path = f.get("path", "?")
        if f.get("type") != "file":
            continue
        if ignored_paths and path.startswith(ignored_paths):
            continue
        expr = f.get("detected_license_expression_spdx")
        if not expr:
            continue
        checked += 1
        for tok in re.split(r"[\s()]+", expr):
            if tok.lower() in skip_tokens:
                continue
            if tok.lower() not in allowed:
                violations.append((path, expr, tok))
                break

    if violations:
        print("BLOCKED: %d file(s) with non-allow-listed license detections:" % len(violations))
        for path, expr, tok in violations[:25]:
            print("  %s: '%s' (offending id: %s)" % (path, expr, tok))
        sys.exit(1)
    print("OK: %d detection(s), all allow-listed" % checked)


if __name__ == "__main__":
    main()
