#!/usr/bin/env python3
"""
autotune.py — Auto-tune codebase-memory-mcp ranking parameters.

Usage:
  python3 scripts/autotune.py [--binary PATH] [--timeout SECS] [--clone]
                               [--repo-url NAME=URL ...]

Sends JSON-RPC directly to the binary via stdin/stdout (no MCP client library).
For each experiment: resets config to defaults, applies overrides, deletes each
repo's SQLite DB, then queries codebase://architecture (which triggers a full
reindex including PageRank with the new weights). Scores results against the
expected top-10 ground truth and reports the best-scoring configuration.

Config changes are GLOBAL (stored in the binary's SQLite config DB). The script
resets all tunable keys to defaults on exit — including after errors — via atexit.

Repo discovery order (for each repo):
  1. candidate_paths checked in order (primary system paths first)
  2. If --clone and a URL is known (via --repo-url or clone_url), clone to the
     last candidate path (adjacent to this script file)
  3. If no URL available, print a hint and return None

Examples:
  python3 scripts/autotune.py
  python3 scripts/autotune.py --timeout 300    # override per-repo timeout
  python3 scripts/autotune.py --clone --repo-url rtk=https://github.com/user/rtk
  python3 scripts/autotune.py --binary /usr/local/bin/codebase-memory-mcp
"""
from __future__ import annotations

import argparse
import atexit
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Directory containing this script — used as the fallback clone target root.
_SCRIPT_DIR = Path(__file__).parent


# ── Repo definitions ──────────────────────────────────────────────────────────
# Each Repo lists: candidate_paths to check in order, expected top-10 ground
# truth names, and an optional clone_url (may be None for private repos).
# Users can supply clone URLs at runtime with --repo-url name=https://...

@dataclass
class Repo:
    name: str
    expected: list[str]
    candidate_paths: list[Path]
    clone_url: str | None = None  # None = private / URL unknown


REPOS: list[Repo] = [
    Repo(
        name="codebase-memory-mcp",
        expected=[
            "cbm_arena_alloc",                   # in-degree 21, core allocator
            "cbm_store_close",                   # in-degree 19
            "cbm_store_upsert_node",             # in-degree 18
            "cbm_gbuf_insert_edge",              # in-degree 18
            "cbm_node_text",                     # in-degree 18
            "cbm_arena_strdup",                  # in-degree 18
            "cbm_pagerank_compute_with_config",  # PageRank entry point
            "cbm_mcp_server_handle",             # MCP entry point
            "cbm_pipeline_check_cancel",         # pipeline control
            "build_key_functions_sql",           # architecture SQL builder
        ],
        candidate_paths=[
            Path.home() / ".claude/codebase-memory-mcp",  # primary (developer)
            Path.home() / "codebase-memory-mcp",          # alternate home location
            _SCRIPT_DIR / "codebase-memory-mcp",          # adjacent to script (clone target)
        ],
        clone_url=None,  # supply via --repo-url codebase-memory-mcp=https://...
    ),
    Repo(
        name="autorun",
        expected=[
            "session_state",           # 375 callers — hot path
            "check_blocked_commands",  # 170 callers — command engine
            "command_matches_pattern", # 145 callers
            "_not_in_pipe",            # 106 callers
            "get_tmux_utilities",      # 96 callers
            "is_premature_stop",       # 64 callers
            "normalize_hook_payload",  # 60 callers
            "validate_hook_response",  # core hook
            "SessionStateManager",     # key class
            "AutorunApp",              # main class
        ],
        candidate_paths=[
            Path.home() / ".claude/autorun",  # primary (developer)
            Path.home() / "autorun",          # alternate
            _SCRIPT_DIR / "autorun",          # adjacent to script (clone target)
        ],
        clone_url=None,  # supply via --repo-url autorun=https://...
    ),
    Repo(
        name="rtk",
        expected=[
            "tokenize",                  # 115 callers — central lexer
            "resolved_command",          # 77 callers
            "status",                    # 68 callers (hook_check.rs)
            "strip_ansi",               # high combined degree
            "check_for_hook",            # main hook dispatch
            "check_for_hook_inner",      # hook logic
            "try_route_native_command",  # routing
            "auto_detect_filter",        # pipe detection
            "estimate_tokens",           # token tracking
            "make_filters",              # filter config
            # EXCLUDED: args() — test helper with 300 callers, not production code
        ],
        candidate_paths=[
            Path.home() / "source/rtk",  # primary (developer)
            Path.home() / "rtk",         # alternate
            _SCRIPT_DIR / "rtk",         # adjacent to script (clone target)
        ],
        clone_url=None,  # supply via --repo-url rtk=https://...
    ),
]


# ── Config defaults ───────────────────────────────────────────────────────────
# Reset before each experiment AND on script exit (atexit), preventing config leaks.

DEFAULTS: dict[str, str] = {
    "edge_weight_calls":     "1.0",
    "edge_weight_usage":     "0.7",
    "edge_weight_defines":   "0.1",
    "edge_weight_tests":     "0.05",
    "edge_weight_imports":   "0.3",
    "key_functions_count":   "25",
    "key_functions_exclude": "",
    "pagerank_max_iter":     "20",
}


# ── Experiment definitions ────────────────────────────────────────────────────

@dataclass
class Experiment:
    label: str
    overrides: dict[str, str] = field(default_factory=dict)
    notes: str = ""


EXPERIMENTS: list[Experiment] = [
    Experiment("baseline_25",
               {"key_functions_count": "25"},
               "Default config, just raise count from 10 to 25"),
    Experiment("exclude_ui",
               {"key_functions_count": "25",
                "key_functions_exclude": "graph-ui/**,tools/**,scripts/**"},
               "Filter TypeScript UI and tooling — exposes C core functions"),
    Experiment("exclude_ui_tests",
               {"key_functions_count": "25",
                "key_functions_exclude": "graph-ui/**,tools/**,scripts/**,tests/**"},
               "Filter UI, tooling, and test files — exposes C core + Python/Rust prod"),
    Experiment("calls_boost",
               {"key_functions_count": "25",
                "edge_weight_calls": "2.0",
                "edge_weight_usage": "0.3"},
               "Boost direct call edges, dampen type-reference edges"),
    Experiment("usage_dampen",
               {"key_functions_count": "25",
                "edge_weight_usage": "0.3",
                "edge_weight_defines": "0.05"},
               "Dampen usage and define weights"),
    Experiment("tests_kill",
               {"key_functions_count": "25",
                "edge_weight_tests": "0.01",
                "edge_weight_usage": "0.3"},
               "Suppress test-file influence on production rankings"),
    Experiment("calls_boost_excl_tests",
               {"key_functions_count": "25",
                "edge_weight_calls": "2.0",
                "edge_weight_usage": "0.3",
                "key_functions_exclude": "graph-ui/**,tools/**,scripts/**,tests/**"},
               "Combined: boost calls + exclude UI and tests"),
    Experiment("more_iters",
               {"key_functions_count": "25",
                "pagerank_max_iter": "100"},
               "More PageRank iterations for convergence on large graphs"),
]


# ── Repo discovery ────────────────────────────────────────────────────────────

def _resolve_repo(repo: Repo, clone: bool,
                  extra_urls: dict[str, str]) -> Path | None:
    """Return the first existing candidate path, or clone if requested.

    Resolution order:
      1. Check candidate_paths in order — first existing dir wins.
      2. If none found and --clone is set: clone using extra_urls[name] or
         repo.clone_url into the last candidate path (script-adjacent dir).
      3. If no URL available, print a hint and return None.
    """
    for path in repo.candidate_paths:
        if path.is_dir():
            return path

    clone_url = extra_urls.get(repo.name) or repo.clone_url
    if not clone_url:
        print(f"  [info] '{repo.name}' not found at any candidate path.")
        print(f"    Tried: {[str(p) for p in repo.candidate_paths]}")
        print(f"    Supply a URL with: --repo-url {repo.name}=https://github.com/user/{repo.name}")
        if not clone:
            print(f"    Or pass --clone to auto-clone once a URL is set.")
        return None

    if not clone:
        print(f"  [info] '{repo.name}' not found. Pass --clone to auto-clone from {clone_url}")
        return None

    target = repo.candidate_paths[-1]  # script-adjacent dir as clone target
    print(f"  [clone] {repo.name} -> {target}  (from {clone_url})")
    target.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["git", "clone", "--depth=1", clone_url, str(target)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  [error] clone failed: {result.stderr.strip()}", file=sys.stderr)
        return None
    return target


# ── JSON-RPC helpers ──────────────────────────────────────────────────────────

def _jsonrpc(req_id: int, method: str, params: dict[str, Any] | None = None) -> str:
    msg: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params:
        msg["params"] = params
    return json.dumps(msg)


def _send_batch(binary: str, messages: list[str], timeout: int,
                env: dict[str, str] | None = None,
                cwd: str | None = None) -> dict[int, Any]:
    """Open a stdio MCP session with the binary, send messages, return responses.

    Messages are processed sequentially by the binary's message loop. Synchronous
    tool calls (like index_repository) block until complete before the binary reads
    the next message — so ordering guarantees correct sequencing of index→query.

    env: extra environment variables to merge (e.g. CBM_TOOL_MODE=classic).
    cwd: working directory for the binary subprocess. CRITICAL: the binary uses
         getcwd() (not rootUri) to set session_root and session_project, so this
         must be set to repo_root for architecture queries to return the right data.
    """
    payload = "\n".join(messages) + "\n"
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    try:
        proc = subprocess.run(
            [binary],
            input=payload.encode(),
            capture_output=True,
            timeout=timeout,
            env=merged_env,
            cwd=cwd,
        )
    except subprocess.TimeoutExpired:
        print(f"  [warn] binary timed out after {timeout}s — "
              "raise --timeout for first-time indexing", file=sys.stderr)
        return {}
    except FileNotFoundError:
        print(f"  [error] binary not found: {binary}", file=sys.stderr)
        sys.exit(1)

    responses: dict[int, Any] = {}
    for line in proc.stdout.decode(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
            if "id" in r:
                responses[r["id"]] = r
        except json.JSONDecodeError:
            pass
    return responses


def index_and_query_architecture(binary: str, repo_root: str,
                                 timeout: int) -> list[dict[str, Any]]:
    """Open one MCP session, synchronously index the repo, then read architecture.

    Uses CBM_TOOL_MODE=classic so index_repository is available. Messages are:
      1. initialize  (sets session root)
      2. tools/call index_repository  (synchronous pipeline + PageRank; blocks)
      3. resources/read codebase://architecture  (reads fresh ranked data)

    The binary processes these in order — index completes before architecture read.
    """
    init = _jsonrpc(1, "initialize", {
        "protocolVersion": "2024-11-05",
        "capabilities": {"tools": {}, "resources": {}},
        "clientInfo": {"name": "autotune", "version": "1.0"},
        "rootUri": f"file://{repo_root}",
    })
    index_call = _jsonrpc(2, "tools/call", {
        "name": "index_repository",
        "arguments": {"repo_path": repo_root},
    })
    arch_read = _jsonrpc(3, "resources/read", {"uri": "codebase://architecture"})

    responses = _send_batch(
        binary,
        [init, index_call, arch_read],
        timeout,
        env={"CBM_TOOL_MODE": "classic"},
        cwd=repo_root,
    )

    r2 = responses.get(2, {})
    if r2.get("error"):
        print(f"  [warn] index_repository error: {r2['error']}", file=sys.stderr)

    r3 = responses.get(3, {})
    contents = r3.get("result", {}).get("contents", [])
    if contents:
        try:
            data = json.loads(contents[0].get("text", "{}"))
            return data.get("key_functions", [])
        except (json.JSONDecodeError, KeyError):
            pass
    return []


def set_config(binary: str, key: str, value: str, timeout: int = 10) -> None:
    """Set a config value via binary CLI: `binary config set key value`."""
    try:
        subprocess.run(
            [binary, "config", "set", key, value],
            capture_output=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"  [warn] config set {key!r} timed out", file=sys.stderr)


def reset_to_defaults(binary: str) -> None:
    """Reset all tunable config keys to baseline defaults.

    Called before each experiment and registered with atexit so no stale config
    persists after a crash or KeyboardInterrupt.
    """
    for k, v in DEFAULTS.items():
        set_config(binary, k, v)


def project_name_from_path(repo_path: Path) -> str:
    """Mirror cbm_project_name_from_path() from src/pipeline/fqn.c.

    Converts an absolute path to the DB filename stem used by the binary:
      /Users/bob/myrepo  →  Users-bob-myrepo
    """
    s = str(repo_path.resolve())
    s = s.replace("\\", "/")
    s = re.sub(r"[/:]", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    s = s.strip("-")
    return s or "root"


def delete_project_db(repo_path: Path) -> None:
    """Delete the binary's SQLite DB for a repo so index_repository does a full reindex."""
    name = project_name_from_path(repo_path)
    db = Path.home() / ".cache" / "codebase-memory-mcp" / f"{name}.db"
    if db.exists():
        db.unlink()
        print(f"  [delete db] {db.name}")



# ── Scoring ───────────────────────────────────────────────────────────────────

def score_result(key_functions: list[dict[str, Any]], expected: list[str]) -> int:
    """Count how many expected names appear in key_functions (case-insensitive)."""
    names: set[str] = set()
    for kf in key_functions:
        name = kf.get("name", "")
        if name:
            names.add(name.lower())
        qn = kf.get("qualified_name", "")
        if qn:
            # Qualified names encode full paths; take the last segment
            names.add(qn.split(".")[-1].lower())
    return sum(1 for e in expected if e.lower() in names)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auto-tune codebase-memory-mcp ranking via JSON-RPC.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 scripts/autotune.py\n"
            "  python3 scripts/autotune.py --timeout 300    # override per-repo timeout\n"
            "  python3 scripts/autotune.py --clone --repo-url rtk=https://github.com/user/rtk\n"
            "  python3 scripts/autotune.py --binary /usr/local/bin/codebase-memory-mcp\n"
            "\n"
            "NOTE: Config changes are global (stored in the binary's SQLite DB).\n"
            "      Stop any running codebase-memory-mcp MCP server before running autotune,\n"
            "      or accept that the server will use whatever config autotune is currently testing.\n"
            "      All config is reset to defaults on exit (including Ctrl-C).\n"
        ),
    )
    parser.add_argument(
        "--binary",
        default=str(Path.home() / ".local/bin/codebase-memory-mcp"),
        help="Path to binary (default: ~/.local/bin/codebase-memory-mcp)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1200,
        help="Seconds before JSON-RPC times out per repo per experiment (default: 1200)",
    )
    parser.add_argument(
        "--top-matches",
        type=int,
        default=10,
        help="How many top key_functions to display per repo per experiment (default: 10)",
    )
    parser.add_argument(
        "--key-count",
        type=int,
        default=25,
        help="key_functions_count to request (default: 25; overrides experiment baseline)",
    )
    parser.add_argument(
        "--clone",
        action="store_true",
        help="Auto-clone missing repos (requires --repo-url or clone_url set in REPOS)",
    )
    parser.add_argument(
        "--repo-url",
        action="append",
        default=[],
        metavar="NAME=URL",
        help="Clone URL for a repo, e.g. --repo-url rtk=https://github.com/user/rtk "
             "(can be repeated for multiple repos)",
    )
    args = parser.parse_args()
    binary = args.binary

    # Parse --repo-url NAME=URL pairs into a dict
    extra_urls: dict[str, str] = {}
    for item in args.repo_url:
        if "=" in item:
            name, url = item.split("=", 1)
            extra_urls[name.strip()] = url.strip()
        else:
            print(f"[warn] --repo-url {item!r} ignored: expected NAME=URL format",
                  file=sys.stderr)

    if not Path(binary).is_file():
        print(f"Error: binary not found: {binary}", file=sys.stderr)
        print("Build with: env -i HOME=$HOME PATH=$PATH make -f Makefile.cbm cbm",
              file=sys.stderr)
        sys.exit(1)

    # Resolve repos before experiments (discovery/cloning happens once)
    resolved: list[tuple[Repo, Path]] = []
    for repo in REPOS:
        path = _resolve_repo(repo, args.clone, extra_urls)
        if path is not None:
            resolved.append((repo, path))

    if not resolved:
        print("Error: no repos found. Use --clone with --repo-url, or place repos at "
              "the candidate paths listed above.", file=sys.stderr)
        sys.exit(1)

    # Always reset config on exit — even after Ctrl-C or crash
    atexit.register(reset_to_defaults, binary)

    # Apply --key-count as a floor on all experiments' key_functions_count
    key_count_str = str(args.key_count)
    for exp in EXPERIMENTS:
        exp.overrides.setdefault("key_functions_count", key_count_str)

    total_expected = sum(len(repo.expected) for repo, _ in resolved)
    print(f"Binary:    {binary}")
    print(f"Repos:     {[(repo.name, str(path)) for repo, path in resolved]}")
    print(f"Timeout:   {args.timeout}s per repo per experiment")
    print(f"key_count: {args.key_count}  top_matches: {args.top_matches}")
    print(f"Max score: {total_expected} ({len(resolved)} repos × {len(REPOS[0].expected)} each)\n")

    best_experiment: Experiment | None = None
    best_score = -1
    all_results: list[tuple[str, int]] = []

    for exp in EXPERIMENTS:
        print(f"\n=== {exp.label} ===")
        if exp.notes:
            print(f"  ({exp.notes})")
        reset_to_defaults(binary)
        for k, v in exp.overrides.items():
            set_config(binary, k, v)
            print(f"  config set {k} = {v!r}")

        total_score = 0
        exp_repo_results: list[dict[str, Any]] = []
        for repo, repo_path in resolved:
            # One MCP session: initialize → tools/call index_repository (synchronous,
            # forces full pipeline+PageRank with current edge weights) → read architecture.
            # Do NOT delete the DB first — an empty DB triggers the background autoindex
            # thread which races with the explicit index_repository tool call.
            print(f"  [index+query] {repo.name}...", end=" ", flush=True)
            kf = index_and_query_architecture(binary, str(repo_path), args.timeout)
            if not kf:
                print(f"no key_functions returned")
                exp_repo_results.append({"repo": repo.name, "score": 0,
                                          "top_n": [], "matched": []})
                continue
            score = score_result(kf, repo.expected)
            total_score += score
            n = args.top_matches
            def _fname(item: dict[str, Any]) -> str:
                name = item.get("name", "")
                if name:
                    return name
                qn = item.get("qualified_name", "")
                return qn.split(".")[-1] if qn else "?"
            top_n = [_fname(item) for item in kf[:n]]
            # matched = expected names that appear anywhere in the full key_functions list
            all_names = {_fname(item).lower() for item in kf}
            matched = [e for e in repo.expected if e.lower() in all_names]
            print(f"{score}/{len(repo.expected)}  matched={matched or 'none'}")
            print(f"    top-{n}: {top_n}")
            exp_repo_results.append({"repo": repo.name, "score": score,
                                      "top_n": top_n, "matched": matched})

        print(f"  TOTAL: {total_score}/{total_expected}")
        all_results.append((exp.label, total_score, exp_repo_results, exp.overrides))
        if total_score > best_score:
            best_score = total_score
            best_experiment = exp

    print("\n" + "=" * 60)
    if best_experiment is None:
        print("No experiments produced results. Ensure repos are indexed.")
        print("Index a repo: codebase-memory-mcp index <path>")
        return

    print(f"BEST: {best_experiment.label}  score={best_score}/{total_expected}")
    if best_experiment.notes:
        print(f"  ({best_experiment.notes})")
    print("\nApply permanently:")
    for k, v in best_experiment.overrides.items():
        print(f"  codebase-memory-mcp config set {k} {v!r}")

    print("\nAll results (best first):")
    sorted_results = sorted(all_results, key=lambda x: x[1], reverse=True)
    for label, score, _repo_results, _overrides in sorted_results:
        marker = " ◀ BEST" if label == best_experiment.label else ""
        bar = "█" * score + "░" * (total_expected - score)
        print(f"  {score:3d}/{total_expected}  [{bar}]  {label}{marker}")

    # ── Save run record to JSON ────────────────────────────────────────────────
    results_file = _SCRIPT_DIR / "autotune_results.json"
    run_record: dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "binary": binary,
        "repos": [repo.name for repo, _ in resolved],
        "total_expected": total_expected,
        "best": {"label": best_experiment.label, "score": best_score,
                 "overrides": best_experiment.overrides},
        "experiments": [
            {
                "label": label,
                "score": score,
                "overrides": overrides,
                "repos": repo_results,
            }
            for label, score, repo_results, overrides in all_results
        ],
    }
    existing: list[dict[str, Any]] = []
    if results_file.exists():
        try:
            existing = json.loads(results_file.read_text())
        except (json.JSONDecodeError, OSError):
            existing = []
    existing.append(run_record)
    results_file.write_text(json.dumps(existing, indent=2))
    print(f"\nRun saved → {results_file}")


if __name__ == "__main__":
    main()
