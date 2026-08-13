#!/usr/bin/env python3
"""Label a corpus's test files from the project's own declaration, not from cbm.

Why this exists
---------------
`scaffolding@K` asks what fraction of a scorer's top-K is test scaffolding. Answering it
with any of the server's own test predicates would be circular: those predicates are the
thing under measurement. PR #879's audit hit the same problem from the other side — its
test penalty under-fired precisely because the classifier disagreed with reality.

So the label comes from a source that knows nothing about codebase-memory-mcp:

  declared  the project's own test configuration (pytest testpaths/python_files from
            pyproject.toml, setup.cfg, pytest.ini or tox.ini; jest testMatch from
            package.json)
  spec      the language toolchain's own rule, which is not a heuristic but a
            definition: Go compiles `*_test.go` as tests (cmd/go), Cargo treats
            `tests/*.rs` as integration tests (Cargo book)
  runner    the test runner enumerating its own suite, when it can run without
            installing the project's dependencies

Every label records which of those produced it. Files no independent source can classify
are emitted as `unknown` rather than guessed, because a guess here would quietly
reintroduce the circularity this file exists to remove.

Usage:
    python3 benchmarks/generate_test_labels.py --corpus flask --repo PATH
    python3 benchmarks/generate_test_labels.py --corpus flask   # resolve from the cache
"""

from __future__ import annotations

import argparse
import configparser
import fnmatch
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LABELS_DIR = Path(__file__).resolve().with_name("labels")
SCHEMA_VERSION = 1

# pytest's own defaults when a project declares none (pytest docs: python_files).
PYTEST_DEFAULT_PATTERNS = ("test_*.py", "*_test.py")

SOURCE_EXTENSIONS = {
    ".py", ".go", ".rs", ".js", ".jsx", ".ts", ".tsx", ".c", ".h", ".cc", ".cpp",
    ".hpp", ".java", ".kt", ".rb", ".php", ".cs", ".scala", ".swift", ".m",
}

VENDOR_DIR_NAMES = {".git", "node_modules", "vendor", "target", "dist", "build"}


def tracked_files(repo: Path) -> list[str]:
    """Repo-relative paths of tracked source files, via git so ignores are honored."""
    proc = subprocess.run(
        ["git", "ls-files"], cwd=repo, text=True, capture_output=True, check=False
    )
    if proc.returncode != 0:
        raise SystemExit(f"git ls-files failed in {repo}: {proc.stderr.strip()}")
    paths = []
    for line in proc.stdout.splitlines():
        path = Path(line)
        if path.suffix.lower() not in SOURCE_EXTENSIONS:
            continue
        if VENDOR_DIR_NAMES & set(path.parts):
            continue
        paths.append(line)
    return sorted(paths)


def pytest_declaration(repo: Path) -> dict[str, Any] | None:
    """Read the project's declared pytest configuration, in pytest's own lookup order."""
    pyproject = repo / "pyproject.toml"
    if pyproject.is_file():
        data = tomllib.loads(pyproject.read_text(encoding="utf-8", errors="replace"))
        ini = data.get("tool", {}).get("pytest", {}).get("ini_options")
        if isinstance(ini, dict):
            return {
                "testpaths": list(ini.get("testpaths") or []),
                "python_files": tuple(
                    ini.get("python_files") or PYTEST_DEFAULT_PATTERNS
                ),
                "declared_in": "pyproject.toml [tool.pytest.ini_options]",
            }
    for name, section in (("pytest.ini", "pytest"), ("tox.ini", "pytest"), ("setup.cfg", "tool:pytest")):
        path = repo / name
        if not path.is_file():
            continue
        parser = configparser.ConfigParser()
        parser.read_string(path.read_text(encoding="utf-8", errors="replace"))
        if not parser.has_section(section):
            continue
        return {
            "testpaths": parser.get(section, "testpaths", fallback="").split(),
            "python_files": tuple(
                parser.get(section, "python_files", fallback="").split()
                or PYTEST_DEFAULT_PATTERNS
            ),
            "declared_in": f"{name} [{section}]",
        }
    return None


def jest_declaration(repo: Path) -> dict[str, Any] | None:
    package = repo / "package.json"
    if not package.is_file():
        return None
    data = json.loads(package.read_text(encoding="utf-8", errors="replace"))
    config = data.get("jest")
    if not isinstance(config, dict):
        return None
    patterns = config.get("testMatch") or config.get("testRegex")
    if not patterns:
        return None
    return {
        "patterns": patterns if isinstance(patterns, list) else [patterns],
        "declared_in": "package.json [jest]",
    }


def label_path(
    path: str, pytest_config: dict[str, Any] | None, jest_config: dict[str, Any] | None
) -> tuple[bool | None, str, str]:
    """Return (is_test, source, evidence) for one repo-relative path."""
    suffix = Path(path).suffix.lower()

    # Language-spec rules. These are definitions, not conventions: the toolchain will
    # not compile the file as anything else.
    if suffix == ".go":
        is_test = path.endswith("_test.go")
        return is_test, "spec", "go: cmd/go compiles *_test.go as the test binary"
    if suffix == ".rs":
        if path.startswith("tests/") or "/tests/" in path:
            return True, "spec", "cargo: tests/ holds integration tests"
        return None, "unknown", "cargo: unit tests live in #[cfg(test)] modules inline"

    if suffix == ".py" and pytest_config is not None:
        name = Path(path).name
        # python_files is what makes a module a test module. testpaths only sets the
        # default collection root, so gating on it would mislabel a real test suite that
        # simply is not collected by a bare `pytest` invocation — flask's
        # examples/tutorial/tests/ is exactly that case, and it is scaffolding either way.
        if any(
            fnmatch.fnmatch(name, pattern) for pattern in pytest_config["python_files"]
        ):
            return (
                True,
                "declared",
                f"pytest: matches python_files ({pytest_config['declared_in']})",
            )
        # conftest.py is test-support infrastructure wherever pytest would load it,
        # which is any directory on the path to a collected test module.
        if name == "conftest.py":
            return (
                True,
                "declared",
                "pytest: conftest.py is test-support infrastructure",
            )
        return (
            False,
            "declared",
            f"pytest: does not match python_files ({pytest_config['declared_in']})",
        )

    if suffix in {".js", ".jsx", ".ts", ".tsx"} and jest_config is not None:
        for pattern in jest_config["patterns"]:
            if pattern.startswith(("**", "!")) or "*" in pattern:
                if fnmatch.fnmatch(path, pattern.lstrip("!")) or fnmatch.fnmatch(
                    "/" + path, pattern.lstrip("!")
                ):
                    return True, "declared", f"jest: testMatch ({jest_config['declared_in']})"
            elif re.search(pattern, path):
                return True, "declared", f"jest: testRegex ({jest_config['declared_in']})"
        return False, "declared", f"jest: not matched ({jest_config['declared_in']})"

    return None, "unknown", "no independent declaration or language rule applies"


def repo_revision(repo: Path) -> str:
    """The checkout's commit, or empty when it is not a git checkout."""
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, text=True, capture_output=True, check=False
    ).stdout.strip()


def build_labels(
    corpus_id: str, repo: Path, paths: list[str] | None = None
) -> dict[str, Any]:
    """Label a corpus's files, reading its own declared test configuration.

    `paths` exists so the labelling rules can be exercised against a plain list of
    file names. Without it every caller — including every test — has to construct a
    git repository just to reach the rules, which drags repository-mutating commands
    into code that only classifies strings. Production passes None and gets the
    tracked-file list, so `.gitignore` is still honored where it matters.
    """
    pytest_config = pytest_declaration(repo)
    jest_config = jest_declaration(repo)
    revision = repo_revision(repo)

    rows: list[dict[str, Any]] = []
    counts = {"test": 0, "not_test": 0, "unknown": 0}
    for path in (tracked_files(repo) if paths is None else sorted(paths)):
        is_test, source, evidence = label_path(path, pytest_config, jest_config)
        counts["unknown" if is_test is None else "test" if is_test else "not_test"] += 1
        rows.append(
            {
                "file_path": path,
                "is_test": is_test,
                "source": source,
                "evidence": evidence,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "corpus": corpus_id,
        "revision": revision,
        "generated_by": "benchmarks/generate_test_labels.py",
        "independence": (
            "Labels derive from the project's own declaration or the language "
            "toolchain's rule. No codebase-memory-mcp test predicate was consulted, so "
            "scaffolding@K does not measure the classifier against itself."
        ),
        "declarations": {
            "pytest": pytest_config,
            "jest": jest_config,
        },
        "counts": counts,
        "labels": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True, help="Corpus id from corpora-v1.json.")
    parser.add_argument(
        "--repo",
        default="",
        help="Checkout to label; defaults to the shared bench-repos cache entry.",
    )
    parser.add_argument("--out", default="", help="Output path; defaults to labels/<corpus>.json.")
    args = parser.parse_args()

    repo = (
        Path(args.repo).expanduser()
        if args.repo
        else Path.home() / ".cache" / "codebase-memory-mcp" / "bench-repos" / args.corpus
    )
    if not (repo / ".git").exists():
        raise SystemExit(f"not a git checkout: {repo}")

    document = build_labels(args.corpus, repo)
    out = Path(args.out) if args.out else LABELS_DIR / f"{args.corpus}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    counts = document["counts"]
    print(
        f"wrote {out.relative_to(ROOT) if out.is_relative_to(ROOT) else out}: "
        f"{counts['test']} test, {counts['not_test']} not-test, "
        f"{counts['unknown']} unknown"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
