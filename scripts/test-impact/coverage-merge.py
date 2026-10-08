#!/usr/bin/env python3
"""coverage-merge.py — the incremental coverage map of a main-branch push.

Merges FRESH (coverage-map.py output for the suites the push re-ran, made with
the new commit's instrumented runner, so its functions.tsv is the new image's
complete function table) into PREVIOUS (the previous published map):

  * a suite FRESH ran keeps FRESH's rows exactly;
  * every other suite the new runner still lists is carried from PREVIOUS,
    each row's function ids remapped by (source file, name) onto FRESH's
    table; a function the new image no longer has drops out of the row, and a
    row's status (an incomplete row stays incomplete) is never improved;
  * a PREVIOUS suite the new runner no longer lists is dropped.

The merged meta.json names --commit and recounts every suite, so the C reader
(cbm_coverage_map_metadata_matches) checks it like a full map. When the map was
observed is not decided here: the producer carries the oldest observation of
the previous bundle forward, so an incremental map ages out unless a full
refresh runs (the receipt's 7-day bound).

Usage: coverage-merge.py --previous DIR --fresh DIR --suites FILE --commit SHA --out DIR
Exit: 0 merged · 1 inconsistent inputs · 2 usage.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

SETUP = "*"


def fail(message: str) -> None:
    print(f"coverage-merge: {message}", file=sys.stderr)
    sys.exit(1)


def read_functions(path: str) -> tuple[dict[int, tuple[str, str]], list[str]]:
    by_id: dict[int, tuple[str, str]] = {}
    lines = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if not line:
                continue
            fields = line.split("\t")
            if len(fields) != 3 or not fields[0].isdigit():
                fail(f"malformed function row in {path}: {line!r}")
            by_id[int(fields[0])] = (fields[1], fields[2])
            lines.append(line)
    return by_id, lines


def read_tests(path: str) -> list[tuple[str, str, str, str, list[int]]]:
    rows = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if not line:
                continue
            fields = line.split("\t")
            if len(fields) != 4 or ":" not in fields[0]:
                fail(f"malformed test row in {path}: {line!r}")
            suite, test = fields[0].split(":", 1)
            ids = [int(token) for token in fields[3].split()] if fields[3] else []
            rows.append((suite, test, fields[1], fields[2], ids))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    for flag in ("previous", "fresh", "suites", "commit", "out"):
        parser.add_argument("--" + flag, required=True)
    args = parser.parse_args()

    _, fresh_lines = read_functions(os.path.join(args.fresh, "functions.tsv"))
    fresh_by_id, _ = read_functions(os.path.join(args.fresh, "functions.tsv"))
    previous_by_id, _ = read_functions(os.path.join(args.previous, "functions.tsv"))
    fresh_ids = {key: ident for ident, key in fresh_by_id.items()}
    with open(os.path.join(args.fresh, "meta.json"), encoding="utf-8") as handle:
        fresh_meta = json.load(handle)
    with open(os.path.join(args.previous, "meta.json"), encoding="utf-8") as handle:
        previous_meta = json.load(handle)
    with open(args.suites, encoding="utf-8") as handle:
        listed = [line.strip() for line in handle if line.strip()]

    fresh_rows = read_tests(os.path.join(args.fresh, "tests.tsv"))
    previous_rows = read_tests(os.path.join(args.previous, "tests.tsv"))
    rerun = {row[0] for row in fresh_rows}
    if not rerun <= set(listed):
        fail("a re-run suite is not listed by the new runner")

    merged = [row for row in fresh_rows]
    for suite, test, status, reason, ids in previous_rows:
        if suite in rerun or suite not in listed:
            continue
        mapped = sorted({fresh_ids[previous_by_id[i]] for i in ids
                         if i in previous_by_id and previous_by_id[i] in fresh_ids})
        merged.append((suite, test, status, reason, mapped))
    merged.sort(key=lambda row: (row[0], row[1]))

    suite_meta = {entry["suite"]: entry for entry in previous_meta.get("suites", [])}
    for entry in fresh_meta.get("suites", []):
        suite_meta[entry["suite"]] = entry
    counts: dict[str, list[int]] = {}
    for suite, test, status, _, _ in merged:
        tally = counts.setdefault(suite, [0, 0])
        if test != SETUP:
            tally[0] += 1
            tally[1] += status != "complete"
    suites = []
    for suite in sorted(counts):
        entry = dict(suite_meta.get(suite, {"suite": suite, "exit": 0}))
        entry["tests"], entry["incomplete"] = counts[suite]
        suites.append(entry)

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "functions.tsv"), "w", encoding="utf-8", newline="\n") as handle:
        handle.write("".join(line + "\n" for line in fresh_lines))
    with open(os.path.join(args.out, "tests.tsv"), "w", encoding="utf-8", newline="\n") as handle:
        for suite, test, status, reason, ids in merged:
            handle.write(f"{suite}:{test}\t{status}\t{reason}\t{' '.join(map(str, ids))}\n")
    meta = dict(fresh_meta)
    meta.update({"commit": args.commit, "tests": sum(c[0] for c in counts.values()),
                 "incomplete": sum(c[1] for c in counts.values()), "suites": suites})
    with open(os.path.join(args.out, "meta.json"), "w", encoding="utf-8", newline="\n") as handle:
        json.dump(meta, handle, indent=1)
        handle.write("\n")
    print(f"merged: {len(rerun)} suite(s) re-run, "
          f"{len(counts) - len(rerun & set(counts))} carried, {len(merged)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
