#!/usr/bin/env python3
"""Fast, read-only checks for repository shelves, indexes, and metadata."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
ERRORS: list[str] = []
PASSES: list[str] = []


def resolve_git() -> str:
    explicit = os.environ.get("GIT_EXECUTABLE")
    if explicit:
        located = shutil.which(explicit)
        if located:
            return located
        if Path(explicit).is_file():
            return explicit
    located = shutil.which("git")
    if located:
        return located
    for variable in ("ProgramFiles", "ProgramFiles(x86)"):
        base = os.environ.get(variable)
        if base:
            candidate = Path(base) / "Git" / "cmd" / "git.exe"
            if candidate.is_file():
                return str(candidate)
    return "git"


GIT = resolve_git()


def error(message: str) -> None:
    ERRORS.append(message)


def passed(message: str) -> None:
    PASSES.append(message)


def run_check(script: str, *args: str) -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / script), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        error(f"{script} failed: {detail}")
    else:
        passed((result.stdout.strip() or script).splitlines()[-1])


def read_index_text(path: str) -> str:
    result = subprocess.run(
        [GIT, "show", f":{path}"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"cannot read staged {path}: {result.stderr.strip()}")
    return result.stdout


def git_index_files() -> list[str]:
    result = subprocess.run(
        [GIT, "ls-files", "--cached", "-z"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        raise RuntimeError("git ls-files failed while reading the staged tree")
    return sorted(item.decode("utf-8") for item in result.stdout.split(b"\0") if item)


def check_required_files() -> None:
    required = [
        ".editorconfig",
        ".github/CODEOWNERS",
        "MAINTAINERS.md",
        "Makefile",
        "Makefile.cbm",
        "VERSION",
        "docs/ARCHITECTURE.md",
        "docs/ORGANIZATION_PLAN.md",
        "docs/README.md",
        "docs/REPOSITORY_INDEX.md",
        "docs/organization-plan.json",
        "docs/repository-layout.json",
        "pkg/release-checksums.json",
        "scripts/check-test-registration.py",
        "scripts/generate-repository-index.py",
        "scripts/sync-version.py",
    ]
    missing = [path for path in required if not (ROOT / path).is_file()]
    if missing:
        error("required repository files missing: " + ", ".join(missing))
    else:
        passed(f"required maps and command files: {len(required)} present")


def check_derived_facts() -> None:
    wrappers = sorted((ROOT / "internal" / "cbm").glob("grammar_*.c"))
    grammars = sorted(path for path in (ROOT / "internal" / "cbm" / "vendored" / "grammars").iterdir() if path.is_dir())
    if not wrappers or len(wrappers) != len(grammars):
        error(f"grammar inventory mismatch: found {len(wrappers)} wrappers and {len(grammars)} directories")
        return
    language_count = len(wrappers)

    active_docs = [
        ROOT / "README.md",
        ROOT / "CONTRIBUTING.md",
        ROOT / "docs" / "index.html",
        ROOT / "docs" / "llms.txt",
        ROOT / "pkg" / "npm" / "README.md",
        ROOT / "server.json",
    ]
    active_docs.extend((ROOT / "pkg" / "winget").rglob("*.locale.en-US.yaml"))
    stale: list[str] = []
    total_claim_patterns = (
        r"\b(?:supports|parses)\s+(\d+)\s+languages\b",
        r"\b(?:analysis|parsing) across(?: all)?\s+(\d+)\s+languages\b",
        r"\bevery one of the\s+(\d+)\s+languages\b",
        r"(?m)^(\d+)\s+languages,\s+all parsed",
        r"\((\d+)\s+languages[,)]",
        r"\b(\d+)\s+languages,",
    )
    for path in active_docs:
        content = path.read_text(encoding="utf-8")
        claims = [int(value) for pattern in total_claim_patterns for value in re.findall(pattern, content, flags=re.IGNORECASE)]
        if not claims or any(value != language_count for value in claims):
            stale.append(f"{path.relative_to(ROOT).as_posix()} ({claims})")
    if stale:
        error("stale language inventory claims: " + ", ".join(stale))
        return

    mcp_source = (ROOT / "src" / "mcp" / "mcp.c").read_text(encoding="utf-8")
    try:
        tool_block = mcp_source.split("static const tool_def_t TOOLS[] = {", 1)[1].split("\n};", 1)[0]
    except IndexError:
        error("could not locate the MCP TOOLS definition")
        return
    tool_count = len(re.findall(r'^\s*\{"[a-z][a-z_]+",', tool_block, flags=re.MULTILINE))
    tool_docs = [
        ROOT / "README.md",
        ROOT / "CONTRIBUTING.md",
        ROOT / "docs" / "ARCHITECTURE.md",
        ROOT / "pkg" / "npm" / "README.md",
    ]
    tool_docs.extend((ROOT / "pkg" / "winget").rglob("*.locale.en-US.yaml"))
    stale_tools: list[str] = []
    for path in tool_docs:
        content = path.read_text(encoding="utf-8")
        claims = [
            int(value)
            for groups in re.findall(
                r"\b(\d+) MCP tools\b|MCP server \([^\n]*?\b(\d+) tools\b",
                content,
            )
            for value in groups
            if value
        ]
        if claims and any(value != tool_count for value in claims):
            stale_tools.append(f"{path.relative_to(ROOT).as_posix()} ({claims})")
    if stale_tools:
        error("stale MCP tool-count claims: " + ", ".join(stale_tools))
    else:
        passed(f"derived inventories: {language_count} languages and {tool_count} MCP tools")


def git_visible_files() -> list[Path]:
    result = subprocess.run(
        [GIT, "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        error("git ls-files failed while checking artifacts")
        return []
    return [ROOT / item.decode("utf-8") for item in result.stdout.split(b"\0") if item]


def layout_policy_errors(paths: list[str], layout: dict[str, object]) -> list[str]:
    issues: list[str] = []
    visible = set(paths)
    if layout.get("schemaVersion") != 1:
        issues.append("repository layout schemaVersion must be 1")
    default_owner = layout.get("defaultOwner")
    if not isinstance(default_owner, str) or not default_owner.strip():
        issues.append("repository layout defaultOwner must be a nonempty string")

    def string_set(field: str) -> set[str]:
        value = layout.get(field, [])
        if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
            issues.append(f"{field} must be an array of nonempty strings")
            return set()
        if len(value) != len(set(value)):
            issues.append(f"{field} must not contain duplicates")
        return set(value)

    actual_root_files = {path for path in visible if "/" not in path}
    expected_root_files = string_set("rootFiles")
    if actual_root_files != expected_root_files:
        missing = sorted(expected_root_files - actual_root_files)
        unknown = sorted(actual_root_files - expected_root_files)
        if missing:
            issues.append("declared root files missing: " + ", ".join(missing))
        if unknown:
            issues.append("undeclared root files: " + ", ".join(unknown))

    shelves = layout.get("shelves", {})
    if not isinstance(shelves, dict):
        return issues + ["shelves must be an object"]
    actual_shelves = {path.split("/", 1)[0] for path in visible if "/" in path}
    expected_shelves = set(shelves)
    if actual_shelves != expected_shelves:
        missing = sorted(expected_shelves - actual_shelves)
        unknown = sorted(actual_shelves - expected_shelves)
        if missing:
            issues.append("declared shelves missing: " + ", ".join(missing))
        if unknown:
            issues.append("undeclared shelves: " + ", ".join(unknown))
    for shelf, metadata in shelves.items():
        if (
            not isinstance(metadata, dict)
            or not isinstance(metadata.get("purpose"), str)
            or not metadata.get("purpose", "").strip()
            or not isinstance(metadata.get("owner"), str)
            or not metadata.get("owner", "").strip()
        ):
            issues.append(f"shelf {shelf} requires nonempty purpose and owner")

    src_root_files = string_set("srcRootFiles")
    src_modules = layout.get("srcModules", {})
    if not isinstance(src_modules, dict):
        issues.append("srcModules must be an object")
    else:
        actual_modules: set[str] = set()
        for path in visible:
            parts = path.split("/")
            if not parts or parts[0] != "src":
                continue
            if len(parts) == 2:
                if parts[1] not in src_root_files:
                    issues.append(f"undeclared direct src file: {path}")
            elif len(parts) > 2:
                actual_modules.add(parts[1])
        expected_modules = set(src_modules)
        if actual_modules != expected_modules:
            missing = sorted(expected_modules - actual_modules)
            unknown = sorted(actual_modules - expected_modules)
            if missing:
                issues.append("declared src modules missing: " + ", ".join(missing))
            if unknown:
                issues.append("undeclared src modules: " + ", ".join(unknown))
        for module, purpose in src_modules.items():
            if not isinstance(purpose, str) or not purpose.strip():
                issues.append(f"src module {module} requires a nonempty purpose")

    package_shelves = layout.get("packageShelves", {})
    if not isinstance(package_shelves, dict):
        issues.append("packageShelves must be an object")
    else:
        actual_packages = {
            path.split("/")[1]
            for path in visible
            if path.startswith("pkg/") and len(path.split("/")) > 2
        }
        expected_packages = set(package_shelves)
        if actual_packages != expected_packages:
            missing = sorted(expected_packages - actual_packages)
            unknown = sorted(actual_packages - expected_packages)
            if missing:
                issues.append("declared package shelves missing: " + ", ".join(missing))
            if unknown:
                issues.append("undeclared package shelves: " + ", ".join(unknown))
        for shelf, purpose in package_shelves.items():
            if not isinstance(purpose, str) or not purpose.strip():
                issues.append(f"package shelf {shelf} requires a nonempty purpose")

    graph_root_files = string_set("graphUiRootFiles")
    graph_directories = string_set("graphUiDirectories")
    for path in visible:
        parts = path.split("/")
        if not parts or parts[0] != "graph-ui":
            continue
        if len(parts) == 2 and parts[1] not in graph_root_files:
            issues.append(f"undeclared graph-ui root file: {path}")
        if len(parts) > 2 and parts[1] not in graph_directories:
            issues.append(f"undeclared graph-ui directory: {parts[1]}")
    return sorted(set(issues))


def check_layout_policy() -> None:
    try:
        layout = json.loads(read_index_text("docs/repository-layout.json"))
    except (RuntimeError, json.JSONDecodeError) as exc:
        error(f"repository layout policy is invalid: {exc}")
        return
    paths = git_index_files()
    issues = layout_policy_errors(paths, layout)
    if issues:
        for issue in issues:
            error("repository layout: " + issue)
        return

    result = subprocess.run(
        [GIT, "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        error("git ls-files failed while checking staged catalog inputs")
        return
    untracked = sorted(item.decode("utf-8") for item in result.stdout.split(b"\0") if item)
    if untracked:
        error("stage intended catalog files or remove local scratch files: " + ", ".join(untracked))
    else:
        passed(f"repository layout: {len(layout['shelves'])} shelves and all root files declared")


def check_artifacts() -> None:
    forbidden: list[str] = []
    for path in git_visible_files():
        relative = path.relative_to(ROOT).as_posix()
        parts = path.relative_to(ROOT).parts
        if (
            relative.startswith("graph-ui/@/")
            or relative.endswith(".tsbuildinfo")
            or "__pycache__" in parts
            or path.suffix in {".pyc", ".pyo"}
        ):
            forbidden.append(relative)
    if forbidden:
        error("forbidden source or generated artifacts: " + ", ".join(sorted(forbidden)))
    else:
        passed("artifact hygiene: no duplicate UI tree, build info, or Python bytecode")

    ignore_text = (ROOT / ".gitignore").read_text(encoding="utf-8")
    required_ignores = ["*.tsbuildinfo", "__pycache__/", "*.py[cod]", "graph-ui/coverage/"]
    absent = [item for item in required_ignores if item not in ignore_text]
    if absent:
        error(".gitignore is missing: " + ", ".join(absent))


def check_hooks() -> None:
    hooks = [ROOT / "scripts" / "hooks" / name for name in ("pre-commit", "commit-msg")]
    installer = (ROOT / "scripts" / "install-git-hooks.sh").read_text(encoding="utf-8")
    commit_message = hooks[1].read_text(encoding="utf-8") if hooks[1].is_file() else ""
    contributing = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    lint_workflow = (ROOT / ".github" / "workflows" / "_lint.yml").read_text(encoding="utf-8")
    if not all(path.is_file() for path in hooks):
        error("pre-commit and commit-msg must both live in scripts/hooks")
    elif "core.hooksPath scripts/hooks" not in installer or "--check" not in installer:
        error("hook installer must configure and self-check scripts/hooks")
    elif "git config core.hooksPath" in contributing:
        error("CONTRIBUTING.md bypasses the single hook installer")
    elif (ROOT / "scripts" / "git-hooks" / "commit-msg").exists():
        error("legacy scripts/git-hooks/commit-msg still exists")
    elif "git interpret-trailers --parse" not in commit_message or "GIT_AUTHOR_IDENT" not in commit_message:
        error("commit-msg must require a parsed sign-off matching the author email")
    elif "mismatched sign-off" not in installer or "bash -n" not in installer:
        error("hook installer self-test must reject a mismatched sign-off and parse both hooks")
    elif "run: bash scripts/install-git-hooks.sh --self-test" not in lint_workflow:
        error("lint CI must execute the Git hook behavioral self-test")
    else:
        result = subprocess.run(
            [GIT, "ls-files", "-s", "scripts/hooks/pre-commit", "scripts/hooks/commit-msg", "scripts/install-git-hooks.sh"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        modes = {
            row.split(maxsplit=3)[3]: row.split(maxsplit=1)[0]
            for row in result.stdout.splitlines()
            if len(row.split(maxsplit=3)) == 4
        }
        expected = {
            "scripts/hooks/pre-commit",
            "scripts/hooks/commit-msg",
            "scripts/install-git-hooks.sh",
        }
        nonexecutable = sorted(path for path in expected if modes.get(path) != "100755")
        if result.returncode or nonexecutable:
            error("hook scripts must be executable in the Git index: " + ", ".join(nonexecutable))
        else:
            passed("Git hooks: one directory, author-matched DCO, and executable self-test")


def check_frontend() -> None:
    package = json.loads((ROOT / "graph-ui" / "package.json").read_text(encoding="utf-8"))
    config = (ROOT / "graph-ui" / "vite.config.ts").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "_test.yml").read_text(encoding="utf-8")
    dependabot = (ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    tests = list((ROOT / "graph-ui" / "src").rglob("*.test.ts")) + list((ROOT / "graph-ui" / "src").rglob("*.test.tsx"))
    issues = []
    if "@vitest/coverage-v8" not in package.get("devDependencies", {}):
        issues.append("explicit coverage provider")
    if not all(token in config for token in ("thresholds", "statements: 50", "branches: 40", "functions: 40", "lines: 50")):
        issues.append("measured coverage thresholds")
    if len(tests) < 13:
        issues.append("thirteen or more frontend test files")
    if package.get("scripts", {}).get("test:coverage") != "vitest run --coverage":
        issues.append("deterministic coverage command")
    app_source = (ROOT / "graph-ui" / "src" / "App.tsx").read_text(encoding="utf-8")
    if "lazy(() => import(" not in app_source or "manualChunks" not in config:
        issues.append("lazy WebGL route and measured vendor chunks")
    if not all(token in config for token in ("enforce-js-chunk-budget", "assertChunkBudget", "MAX_JS_CHUNK_BYTES")):
        issues.append("failing production JavaScript chunk budget")
    if not (ROOT / "graph-ui" / "src" / "lib" / "chunkBudget.test.ts").is_file():
        issues.append("chunk-budget mutation tests")
    if not all(token in workflow for token in ("working-directory: graph-ui", "npm ci", "npm run build", "npm run test:coverage")):
        issues.append("frontend build and test CI job")
    if 'package-ecosystem: "npm"' not in dependabot or 'directory: "/graph-ui"' not in dependabot:
        issues.append("frontend Dependabot entry")
    if issues:
        error("frontend organization missing: " + ", ".join(issues))
    else:
        passed(f"frontend: {len(tests)} test files, coverage floors, split build, hard chunk budget, and weekly updates")


def check_json_files() -> None:
    invalid: list[str] = []
    checked = 0
    excluded_parts = {".git", "build", "node_modules", "vendored"}
    for path in git_visible_files():
        if path.suffix != ".json" or not path.is_file():
            continue
        relative_parts = path.relative_to(ROOT).parts
        if excluded_parts.intersection(relative_parts):
            continue
        checked += 1
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            invalid.append(f"{path.relative_to(ROOT).as_posix()}: {exc}")
    if invalid:
        error("invalid first-party JSON: " + "; ".join(invalid))
    else:
        passed(f"first-party JSON: {checked} files parsed")


def check_markdown_links() -> None:
    docs = list(ROOT.glob("*.md")) + list((ROOT / "docs").glob("*.md"))
    docs.extend((ROOT / "pkg").glob("*/README.md"))
    missing: list[str] = []
    checked = 0
    for document in docs:
        content = document.read_text(encoding="utf-8")
        for raw in re.findall(r"!?\[[^\]]*\]\(([^)]+)\)", content):
            target = raw.strip()
            if target.startswith("<") and target.endswith(">"):
                target = target[1:-1]
            else:
                target = target.split()[0]
            if not target or target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            target = unquote(target.split("#", 1)[0].split("?", 1)[0])
            if not target:
                continue
            checked += 1
            resolved = (document.parent / target).resolve()
            if not resolved.exists():
                missing.append(f"{document.relative_to(ROOT).as_posix()} -> {target}")
    if missing:
        error("missing local Markdown links: " + ", ".join(missing))
    else:
        passed(f"local Markdown links: {checked} targets exist")


def plan_tracker_errors(data: dict[str, object]) -> list[str]:
    issues: list[str] = []
    if data.get("schemaVersion") != 1:
        issues.append("organization tracker schemaVersion must be 1")
    if not isinstance(data.get("planDate"), str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(data.get("planDate"))):
        issues.append("organization tracker planDate must use YYYY-MM-DD")
    if not isinstance(data.get("baselineCommit"), str) or not re.fullmatch(r"[0-9a-f]{40}", str(data.get("baselineCommit"))):
        issues.append("organization tracker baselineCommit must be a full Git SHA")
    if not isinstance(data.get("branch"), str) or not str(data.get("branch")).strip():
        issues.append("organization tracker branch must be a nonempty string")
    if not isinstance(data.get("scoreBasis"), str) or not str(data.get("scoreBasis")).strip():
        issues.append("organization tracker scoreBasis must be a nonempty string")
    tasks = data.get("tasks", [])
    if not isinstance(tasks, list) or not tasks:
        return ["organization tracker requires a nonempty tasks array"]
    if any(not isinstance(task, dict) for task in tasks):
        return ["organization tracker tasks must be objects"]
    if any(not isinstance(task.get("dependsOn"), list) for task in tasks):
        return ["organization tracker dependencies must be arrays"]
    if any(not isinstance(task.get("id"), str) or not task.get("id") for task in tasks):
        return ["organization tracker task IDs must be nonempty strings"]
    if any(not isinstance(dependency, str) for task in tasks for dependency in task["dependsOn"]):
        return ["organization tracker dependencies must be task-ID strings"]
    ids = [task.get("id") for task in tasks]
    known = set(ids)
    bad_dependencies = sorted(
        f"{task.get('id')}->{dependency}"
        for task in tasks
        for dependency in task.get("dependsOn", [])
        if dependency not in known
    )
    statuses = {"pending", "in_progress", "complete"}
    for field in ("planQualityScore", "targetOrganizationScore"):
        score = data.get(field)
        if isinstance(score, bool) or not isinstance(score, int) or not 0 <= score <= 100:
            issues.append(f"organization tracker {field} must be an integer from 0 to 100")
    execution_status = data.get("executionStatus")
    if execution_status not in statuses:
        issues.append("organization tracker executionStatus is invalid")
    if len(ids) != len(known) or None in known:
        issues.append("organization tracker task IDs must be unique and nonempty")
    invalid_ids = sorted(str(task_id) for task_id in known if not re.fullmatch(r"ORG-\d{3}", str(task_id)))
    if invalid_ids:
        issues.append("organization tracker task IDs must match ORG-NNN: " + ", ".join(invalid_ids))
    if bad_dependencies:
        issues.append("organization tracker has unknown dependencies: " + ", ".join(bad_dependencies))
    if any(task.get("status") not in statuses for task in tasks):
        issues.append("organization tracker contains an invalid status")
    incomplete = sorted(str(task.get("id")) for task in tasks if task.get("status") != "complete")
    if execution_status == "complete" and incomplete:
        issues.append("organization tracker has incomplete tasks: " + ", ".join(incomplete))
    self_dependencies = sorted(
        str(task.get("id"))
        for task in tasks
        if task.get("id") in task.get("dependsOn", [])
    )
    if self_dependencies:
        issues.append("organization tracker has self dependencies: " + ", ".join(self_dependencies))
    dependencies = {str(task["id"]): [str(item) for item in task["dependsOn"]] for task in tasks}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(task_id: str) -> bool:
        if task_id in visiting:
            return True
        if task_id in visited:
            return False
        visiting.add(task_id)
        if any(dependency in dependencies and visit(dependency) for dependency in dependencies.get(task_id, [])):
            return True
        visiting.remove(task_id)
        visited.add(task_id)
        return False

    if any(visit(task_id) for task_id in sorted(dependencies)):
        issues.append("organization tracker dependency graph must be acyclic")
    if "finalIndex" in data:
        issues.append("live finalIndex claims are not reproducible after the temporary MCP cache is removed")
    audit = data.get("historicalMcpAudit")
    if not isinstance(audit, dict) or audit.get("status") != "historical" or audit.get("ephemeral") is not True:
        issues.append("historicalMcpAudit must be explicitly historical and ephemeral")
    elif (
        any(isinstance(audit.get(field), bool) or not isinstance(audit.get(field), int) or audit.get(field, -1) < 0
            for field in ("nodes", "edges", "files", "skippedFiles"))
        or audit.get("adrPresent") is not True
    ):
        issues.append("historicalMcpAudit counts must be nonnegative integers and adrPresent must be true")
    return issues


def check_plan_tracker() -> None:
    data = json.loads((ROOT / "docs" / "organization-plan.json").read_text(encoding="utf-8"))
    issues = plan_tracker_errors(data)
    if issues:
        for issue in issues:
            error(issue)
    else:
        passed(f"organization plan tracker: {len(data['tasks'])} complete tasks")


def check_command_surface() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    declared = set(re.findall(r"^([a-z][a-z0-9-]*):", makefile, flags=re.MULTILINE))
    required = {"help", "build", "test", "lint", "security", "frontend", "organization", "organization-tests", "clean"}
    missing = sorted(required - declared)
    if missing:
        error("root Makefile targets missing: " + ", ".join(missing))
    elif "command -v python3" not in makefile or "command -v py" not in makefile:
        error("root Makefile must discover Python 3 across Unix and Windows launchers")
    else:
        passed("root command surface: exact targets and portable Python discovery")


def check_line_endings() -> None:
    attributes = (ROOT / ".gitattributes").read_text(encoding="utf-8")
    if "* text=auto eol=lf" not in attributes:
        error(".gitattributes must enforce LF for repository text")
        return
    result = subprocess.run(
        [GIT, "ls-files", "--eol", "-z"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        error("git ls-files --eol failed")
        return
    bad: list[str] = []
    for row in result.stdout.decode("utf-8", errors="replace").split("\0"):
        if not row:
            continue
        metadata, _, path = row.partition("\t")
        if "i/crlf" in metadata and not path.endswith((".bat", ".cmd")):
            bad.append(path)
    if bad:
        error("tracked text has CRLF in the Git index: " + ", ".join(bad))
    else:
        passed("line endings: LF enforced in the Git index")


def check_no_permanent_mcp_state() -> None:
    forbidden = [ROOT / ".codebase-memory", ROOT / ".mcp.json"]
    present = [path.name for path in forbidden if path.exists()]
    if present:
        error("temporary MCP state was persisted in the repository: " + ", ".join(present))
    else:
        passed("MCP lifecycle: no permanent repository cache or configuration")


def main() -> int:
    ERRORS.clear()
    PASSES.clear()
    run_check("sync-version.py")
    run_check("check-test-registration.py")
    run_check("generate-repository-index.py", "--check")
    checks = (
        ("required files", check_required_files),
        ("layout policy", check_layout_policy),
        ("derived facts", check_derived_facts),
        ("artifact hygiene", check_artifacts),
        ("Git hooks", check_hooks),
        ("frontend", check_frontend),
        ("JSON", check_json_files),
        ("Markdown links", check_markdown_links),
        ("plan tracker", check_plan_tracker),
        ("command surface", check_command_surface),
        ("line endings", check_line_endings),
        ("MCP lifecycle", check_no_permanent_mcp_state),
    )
    for label, check in checks:
        try:
            check()
        except Exception as exc:
            error(f"{label} check could not complete: {type(exc).__name__}: {exc}")

    for message in PASSES:
        print(f"OK: {message}")
    if ERRORS:
        for message in ERRORS:
            print(f"ERROR: {message}", file=sys.stderr)
        print(f"repository organization: {len(ERRORS)} failure(s)", file=sys.stderr)
        return 1
    print(f"repository organization: PASS ({len(PASSES)} checks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
