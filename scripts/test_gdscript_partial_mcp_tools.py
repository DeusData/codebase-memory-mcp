#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def git(*args: str, cwd: Path, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def parse_json_line(text: str) -> dict:
    for line in reversed([ln.strip() for ln in text.splitlines() if ln.strip()]):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    fail(f"could not parse JSON from output:\n{text}")


def run_tool(binary: Path, tool: str, params: dict | None = None) -> dict:
    command = [str(binary), "cli", tool]
    if params is not None:
        command.append(json.dumps(params))

    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        fail(
            f"tool {tool} failed with code {result.returncode}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    envelope = parse_json_line(result.stdout)
    if envelope.get("isError"):
        fail(f"tool {tool} returned isError=true: {envelope}")

    content = envelope.get("content") or []
    if not content or "text" not in content[0]:
        fail(f"tool {tool} returned malformed content: {envelope}")

    try:
        return json.loads(content[0]["text"])
    except json.JSONDecodeError:
        fail(f"tool {tool} returned non-JSON payload: {content[0]['text']}")


def build_fixture_repo(temp_root: Path) -> Path:
    fixture_repo = temp_root / "fixture-repo"
    fixture_repo.mkdir()

    write_file(
        fixture_repo / "project.godot",
        """; Engine configuration file.
[application]
config/name=\"Partial Tool Fixture\"
config/features=PackedStringArray(\"4.3\")
""",
    )
    write_file(
        fixture_repo / "PlayerWalking.gd",
        """class_name PlayerWalking

func Update():
    Move()

func Move():
    Transition(\"Idle\")

func Transition(_state):
    pass
""",
    )

    git("init", cwd=fixture_repo)
    git("add", ".", cwd=fixture_repo)
    commit_env = os.environ.copy()
    commit_env.update(
        {
            "GIT_AUTHOR_NAME": "CBM Test",
            "GIT_AUTHOR_EMAIL": "cbm-test@example.invalid",
            "GIT_COMMITTER_NAME": "CBM Test",
            "GIT_COMMITTER_EMAIL": "cbm-test@example.invalid",
        }
    )
    git("commit", "-m", "fixture", cwd=fixture_repo, env=commit_env)
    return fixture_repo


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    binary = repo_root / "build" / "c" / "codebase-memory-mcp"
    if not binary.is_file():
        fail(f"binary not found at {binary}")

    with tempfile.TemporaryDirectory(prefix="gd-partial-mcp-tools-") as tmpdir:
        temp_root = Path(tmpdir)
        repo_path = build_fixture_repo(temp_root)

        index_payload = run_tool(
            binary,
            "index_repository",
            {"repo_path": str(repo_path), "mode": "fast"},
        )
        project = index_payload.get("project")
        if not isinstance(project, str) or not project:
            fail(f"index_repository did not return project name: {index_payload}")

        try:
            schema = run_tool(binary, "get_graph_schema", {"project": project})
            node_labels = {row.get("label") for row in schema.get("node_labels", [])}
            edge_types = {row.get("type") for row in schema.get("edge_types", [])}
            if "Method" not in node_labels or "Class" not in node_labels:
                fail(f"schema missing expected GDScript labels: {schema}")
            if "CALLS" not in edge_types:
                fail(f"schema missing CALLS edge for fixture call chain: {schema}")

            architecture = run_tool(
                binary,
                "get_architecture",
                {"project": project, "aspects": ["all"]},
            )
            if architecture.get("total_nodes", 0) < 5:
                fail(f"architecture total_nodes too small: {architecture}")
            if architecture.get("total_edges", 0) < 5:
                fail(f"architecture total_edges too small: {architecture}")

            search = run_tool(
                binary,
                "search_graph",
                {
                    "project": project,
                    "label": "Method",
                    "name_pattern": ".*Move.*",
                    "limit": 10,
                },
            )
            search_results = search.get("results") or []
            move = next(
                (row for row in search_results if row.get("name") == "Move"), None
            )
            if move is None:
                fail(f"search_graph did not return Move method: {search}")
            move_qn = move.get("qualified_name")
            if not move_qn:
                fail(f"search_graph result missing qualified_name: {move}")

            trace = run_tool(
                binary,
                "trace_call_path",
                {
                    "project": project,
                    "function_name": "Move",
                    "direction": "both",
                    "depth": 2,
                },
            )
            caller_names = {row.get("name") for row in trace.get("callers") or []}
            callee_names = {row.get("name") for row in trace.get("callees") or []}
            if "Update" not in caller_names:
                fail(f"trace_call_path missing Update caller for Move: {trace}")
            if "Transition" not in callee_names:
                fail(f"trace_call_path missing Transition callee for Move: {trace}")

            snippet = run_tool(
                binary,
                "get_code_snippet",
                {"project": project, "qualified_name": move_qn},
            )
            source = snippet.get("source") or ""
            if snippet.get("label") != "Method":
                fail(f"get_code_snippet did not return Method label: {snippet}")
            if "func Move():" not in source or 'Transition("Idle")' not in source:
                fail(f"get_code_snippet source missing expected Move body: {snippet}")

            search_code = run_tool(
                binary,
                "search_code",
                {
                    "project": project,
                    "pattern": "Transition",
                    "file_pattern": "*.gd",
                    "mode": "compact",
                    "limit": 10,
                },
            )
            code_results = search_code.get("results") or []
            if not code_results:
                fail(f"search_code returned no compact results: {search_code}")
            if not any(row.get("file") == "PlayerWalking.gd" for row in code_results):
                fail(
                    f"search_code did not map match back to fixture file: {search_code}"
                )

        finally:
            run_tool(binary, "delete_project", {"project": project})

    print("PASS")


if __name__ == "__main__":
    main()
