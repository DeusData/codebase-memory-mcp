#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/gdscript-proof.sh --repo /abs/path/to/repo [--repo /abs/path/to/repo2 ...] [--godot-version REPO=4.x] [--label REPO=name]

The command stores paths after canonicalizing to absolute real paths.
EOF
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
WORKTREE_PATH=$(cd -- "$SCRIPT_DIR/.." && pwd -P)

ARTIFACT_ROOT="$WORKTREE_PATH/.artifacts/gdscript-proof"
RUN_TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN_ROOT=""
STATE_ROOT=""
STATE_HOME=""
STATE_CONFIG=""
STATE_CACHE=""
STATE_STORE=""
EFFECTIVE_STORE_ROOT=""
TMP_ROOT=""
ENV_FILE=""
COMMANDS_LOG=""
BINARY_PATH=""
BUILD_LOG=""
AGGREGATE_SUMMARY=""
AGG_INDEXING_COVERAGE="false"
AGG_SIGNAL_COVERAGE="false"
AGG_IMPORTS_COVERAGE="false"
AGG_INHERITS_COVERAGE="false"
AGGREGATE_PASS="false"
AGGREGATE_MISSING_CATEGORIES=""

BUILD_STATUS="pending"
BUILD_MESSAGE=""

REPO_PATHS=()
REPO_GODOT_VERSIONS=()
REPO_SLUGS=()
REPO_GIT_REFS=()
REPO_GIT_COMMITS=()
REPO_GIT_BRANCHES=()
REPO_LABELS=()
REPO_PROJECT_IDS=()
REPO_INDEX_STATUSES=()
REPO_PROJECT_RESOLUTION_STATUSES=()
REPO_OVERALL_STATUSES=()
REPO_STATUS_MESSAGES=()
REPO_CLI_CAPTURE_MODES=()
REPO_CLI_CAPTURE_NOTES=()
WORKTREE_BRANCH=""
WORKTREE_COMMIT=""
CBM_LAST_ERROR=""
CBM_LAST_CAPTURE_MODE=""
GDSCRIPT_LAST_QUERY_NAME=""
GDSCRIPT_QUERY_FALLBACK_TOOLS=""

GDSCRIPT_QUERY_NAMES=(
  "gd-files"
  "gd-classes"
  "gd-methods"
  "gd-class-sample"
  "gd-method-sample"
  "signal-calls"
  "gd-inherits"
  "gd-imports"
)

GDSCRIPT_QUERY_FILES=(
  "gd-files.json"
  "gd-classes.json"
  "gd-methods.json"
  "gd-class-sample.json"
  "gd-method-sample.json"
  "signal-calls.json"
  "gd-inherits.json"
  "gd-imports.json"
)

GDSCRIPT_QUERIES=(
  'MATCH (f:File) WHERE f.file_path ENDS WITH ".gd" RETURN count(f) AS gd_files'
  'MATCH (c:Class) WHERE c.file_path ENDS WITH ".gd" RETURN count(c) AS gd_classes'
  'MATCH (m:Method) WHERE m.file_path ENDS WITH ".gd" RETURN count(m) AS gd_methods'
  'MATCH (c:Class) WHERE c.file_path ENDS WITH ".gd" RETURN c.name, c.file_path ORDER BY c.file_path, c.name LIMIT 5'
  'MATCH (m:Method) WHERE m.file_path ENDS WITH ".gd" RETURN m.name, m.file_path ORDER BY m.file_path, m.name LIMIT 5'
  'MATCH (caller)-[c:CALLS]->(t:Function) WHERE t.qualified_name CONTAINS ".signal." RETURN count(c) AS signal_calls'
  'MATCH (a)-[r:INHERITS]->(b) WHERE (a.file_path ENDS WITH ".gd") OR (b.file_path ENDS WITH ".gd") RETURN count(r) AS gd_inherits_edges'
  'MATCH (m)-[r:IMPORTS]->(n) WHERE n.file_path ENDS WITH ".gd" RETURN count(r) AS gd_deps'
)

GDSCRIPT_INVALID_QUERY_FRAGMENT='RETURN THIS_WILL_NOT_PARSE'

log_command() {
  printf '%s\n' "$1" >> "$COMMANDS_LOG"
}

make_run_temp() {
  local prefix="$1"

  mkdir -p "$TMP_ROOT"
  mktemp "$TMP_ROOT/${prefix}.XXXXXX"
}

parse_repo_path() {
  local repo_path="$1"

  if ! repo_path=$(python3 - "$repo_path" <<'PY'
import os
import sys

path = sys.argv[1]
if os.path.isdir(path):
    print(os.path.realpath(path))
else:
    print(os.path.abspath(os.path.normpath(path)))
PY
  ); then
    echo "error: unable to resolve repository path: $repo_path" >&2
    return 1
  fi

  printf '%s\n' "$repo_path"
}

assert_repo_declared() {
  local repo_path="$1"
  local ignored

  if ! ignored=$(repo_index_for_path "$repo_path"); then
    echo "error: --label/--godot-version require a prior --repo for: $repo_path" >&2
    return 1
  fi
}

repo_index_for_path() {
  local target="$1"
  local i

  for i in "${!REPO_PATHS[@]}"; do
    if [ "${REPO_PATHS[$i]}" = "$target" ]; then
      printf '%s\n' "$i"
      return 0
    fi
  done

  return 1
}

ensure_repo_entry() {
  local repo_path="$1"
  local index

  if index=$(repo_index_for_path "$repo_path"); then
    return 0
  fi

  REPO_PATHS+=("$repo_path")
  REPO_LABELS+=("")
  REPO_GODOT_VERSIONS+=("")
  REPO_SLUGS+=("")
  REPO_GIT_REFS+=("")
  REPO_GIT_COMMITS+=("")
  REPO_GIT_BRANCHES+=("")
  REPO_PROJECT_IDS+=("")
  REPO_INDEX_STATUSES+=("pending")
  REPO_PROJECT_RESOLUTION_STATUSES+=("pending")
  REPO_OVERALL_STATUSES+=("pending")
  REPO_STATUS_MESSAGES+=("")
  REPO_CLI_CAPTURE_MODES+=("pending")
  REPO_CLI_CAPTURE_NOTES+=("")
}

set_repo_metadata() {
  local repo_path="$1"
  local label="$2"
  local version="$3"
  local index

  if ! index=$(repo_index_for_path "$repo_path"); then
    echo "error: cannot set metadata for unregistered repo: $repo_path" >&2
    return 1
  fi

  if [ -n "$label" ]; then
    REPO_LABELS[$index]="$label"
  fi

  if [ -n "$version" ]; then
    REPO_GODOT_VERSIONS[$index]="$version"
  fi
}

initialize_workspace_root() {
  local template

  mkdir -p "$ARTIFACT_ROOT"
  template="${ARTIFACT_ROOT}/${RUN_TIMESTAMP}-$$-XXXXXX"

  if ! RUN_ROOT=$(mktemp -d "$template"); then
    echo "error: failed to create unique run root under $ARTIFACT_ROOT" >&2
    exit 1
  fi

  STATE_ROOT="$RUN_ROOT/state"
  STATE_HOME="$STATE_ROOT/home"
  STATE_CONFIG="$STATE_ROOT/config"
  STATE_CACHE="$STATE_ROOT/cache"
  STATE_STORE="$STATE_ROOT/store"
  EFFECTIVE_STORE_ROOT="$STATE_CACHE/codebase-memory-mcp"
  TMP_ROOT="$RUN_ROOT/tmp"

  ENV_FILE="$RUN_ROOT/env.txt"
  COMMANDS_LOG="$RUN_ROOT/commands.log"
  BINARY_PATH="$WORKTREE_PATH/build/codebase-memory-mcp"
  BUILD_LOG="$RUN_ROOT/build.log"
  AGGREGATE_SUMMARY="$RUN_ROOT/aggregate-summary.md"
}

apply_isolated_runtime_env() {
  export HOME="$STATE_HOME"
  export XDG_CONFIG_HOME="$STATE_CONFIG"
  export XDG_CACHE_HOME="$STATE_CACHE"
}

parse_metadata_assignment() {
  local assignment="$1"
  if [[ "$assignment" != *=* ]]; then
    echo "error: expected path=value form, got: $assignment" >&2
    return 1
  fi

  printf '%s\n' "${assignment%%=*}"
  printf '%s\n' "${assignment#*=}"
}

sanitize_repo_name() {
  local raw_name="$1"
  local sanitized

  sanitized=$(printf '%s' "$raw_name" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9._-]+/-/g; s/^-+//; s/-+$//; s/-+/-/g')
  [ -z "$sanitized" ] && sanitized="repo"

  printf '%s\n' "$sanitized"
}

repo_path_hash() {
  local repo_path="$1"
  local digest

  digest=$(python3 - "$repo_path" <<'PY'
import hashlib
import sys

path = sys.argv[1]
print(hashlib.sha1(path.encode('utf-8')).hexdigest())
PY
  )

  printf '%s\n' "$digest"
}

derive_repo_artifact_slug() {
  local repo_path="$1"
  local short_commit="$2"
  local repo_name
  local path_hash
  local base_slug

  # Recipe: <repo basename slug>-<12-char commit prefix or unavailable>-<40-char canonical path hash>
  repo_name=$(sanitize_repo_name "$(basename -- "$repo_path")")
  short_commit=${short_commit:-"unavailable"}
  path_hash=$(repo_path_hash "$repo_path")

  base_slug="${repo_name}-${short_commit}-${path_hash}"
  printf '%s\n' "$base_slug"
}

collect_repo_git_metadata() {
  local repo_path="$1"
  local git_ref
  local git_commit
  local git_branch

  if command -v git >/dev/null 2>&1 && git -C "$repo_path" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git_commit=$(git -C "$repo_path" rev-parse HEAD 2>/dev/null || true)
    if [ -n "$git_commit" ]; then
      git_ref=${git_commit:0:12}
    else
      git_ref="unavailable"
    fi
    git_branch=$(git -C "$repo_path" rev-parse --abbrev-ref HEAD 2>/dev/null || true)
  fi

  printf '%s\n%s\n%s\n' "${git_ref:-unavailable}" "${git_commit:-unavailable}" "${git_branch:-unavailable}"
}

collect_repo_metadata_fields() {
  local repo_path="$1"
  local godot_version="$2"

  local git_ref
  local git_commit
  local git_branch
  local short_commit
  local qualifies
  local slug

  {
    IFS= read -r git_ref
    IFS= read -r git_commit
    IFS= read -r git_branch
  } < <(collect_repo_git_metadata "$repo_path")

  short_commit=$git_ref
  slug=$(derive_repo_artifact_slug "$repo_path" "$short_commit")

  if godot_version_is_4x "$godot_version"; then
    qualifies="true"
  else
    qualifies="false"
  fi

  printf '%s\n%s\n%s\n%s\n%s\n' "$slug" "$git_ref" "$git_commit" "$git_branch" "$qualifies"
}

godot_version_is_4x() {
  local version="$1"
  local major

  if [ -z "$version" ]; then
    return 1
  fi

  version=${version#Godot }
  version=${version#godot }
  major="${version%%.*}"
  if [ "$major" = "4" ] || [ "$major" = "v4" ]; then
    return 0
  fi

  return 1
}

write_repo_meta_json() {
  local repo_dir="$1"
  local repo_path="$2"
  local repo_label="$3"
  local repo_name="$4"
  local slug="$5"
  local project_id="$6"
  local proof_timestamp="$7"
  local git_ref="$8"
  local git_commit="$9"
  local git_branch="${10}"
  local godot_version="${11}"
  local qualifies_4x="${12}"

  local output_file="$repo_dir/repo-meta.json"
  local worktree_path="$WORKTREE_PATH"
  local worktree_branch="$WORKTREE_BRANCH"
  local worktree_commit="$WORKTREE_COMMIT"

  python3 - "$output_file" "$repo_path" "$repo_label" "$repo_name" "$slug" "$project_id" "$proof_timestamp" "$git_ref" "$git_commit" "$git_branch" "$godot_version" "$qualifies_4x" "$worktree_path" "$worktree_branch" "$worktree_commit" <<'PY'
import json
import sys

(
    output_file,
    repo_path,
    repo_label,
    repo_name,
    slug,
    project_id,
    proof_timestamp,
    git_ref,
    git_commit,
    git_branch,
    godot_version,
    qualifies_4x,
    worktree_path,
    worktree_branch,
    worktree_commit,
) = sys.argv[1:]

payload = {
    "repo_path": repo_path,
    "repo_name": repo_name,
    "artifact_slug": slug,
    "project_id": None if project_id in ("", "pending", "null") else project_id,
    "proof_run_utc": proof_timestamp,
    "git_ref": git_ref,
    "git_commit": git_commit,
    "git_branch": git_branch,
    "godot_version": None if godot_version == "" else godot_version,
    "qualifies_godot4x": True if qualifies_4x == "true" else False,
    "codebase_memory_mcp": {
        "worktree": worktree_path,
        "branch": worktree_branch,
        "commit": worktree_commit,
    },
}

if repo_label:
    payload["repo_label"] = repo_label

with open(output_file, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

format_command_for_log() {
  local formatted

  printf -v formatted '%q ' "$@"
  formatted=${formatted% }
  printf '%s\n' "$formatted"
}

append_command_capture() {
  local log_file="$1"
  local command_line="$2"
  local stdout_file="$3"
  local stderr_file="$4"

  {
    printf '== command ==\n%s\n' "$command_line"
    printf -- '-- stdout --\n'
    if [ -s "$stdout_file" ]; then
      cat "$stdout_file"
    fi
    printf '\n'
    printf -- '-- stderr --\n'
    if [ -s "$stderr_file" ]; then
      cat "$stderr_file"
    fi
    printf '\n'
  } >> "$log_file"
}

run_logged_command_capture() {
  local workdir="$1"
  local stdout_file="$2"
  local stderr_file="$3"
  shift 3

  if [ -n "$workdir" ]; then
    (
      cd -- "$workdir"
      "$@"
    ) >"$stdout_file" 2>"$stderr_file"
  else
    "$@" >"$stdout_file" 2>"$stderr_file"
  fi
}

normalize_cli_output() {
  local input_file="$1"
  local output_file="$2"

  python3 - "$input_file" "$output_file" <<'PY'
import json
import pathlib
import sys

input_path, output_path = sys.argv[1:3]
raw_text = pathlib.Path(input_path).read_text(encoding="utf-8")

try:
    payload = json.loads(raw_text)
except json.JSONDecodeError as exc:
    sys.stderr.write(f"invalid JSON output: {exc}\n")
    sys.exit(2)

if isinstance(payload, dict) and isinstance(payload.get("content"), list):
    items = payload["content"]
    text = ""
    if items and isinstance(items[0], dict):
        candidate = items[0].get("text")
        if isinstance(candidate, str):
            text = candidate
    pathlib.Path(output_path).write_text(
        text if not text or text.endswith("\n") else text + "\n",
        encoding="utf-8",
    )
    sys.stdout.write("wrapped_error\n" if payload.get("isError") else "wrapped_ok\n")
else:
    pathlib.Path(output_path).write_text(
        raw_text if raw_text.endswith("\n") else raw_text + "\n",
        encoding="utf-8",
    )
    sys.stdout.write("raw\n")
PY
}

build_index_payload() {
  local repo_path="$1"

  python3 - "$repo_path" <<'PY'
import json
import sys

print(json.dumps({"repo_path": sys.argv[1], "mode": "full"}, separators=(",", ":")))
PY
}

describe_failure_output() {
  local normalized_file="$1"
  local stderr_file="$2"
  local fallback_message="$3"
  local normalized_text=""
  local stderr_text=""

  if [ -f "$normalized_file" ]; then
    normalized_text=$(python3 - "$normalized_file" <<'PY'
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").strip()
print(text)
PY
    )
  fi

  if [ -f "$stderr_file" ]; then
    stderr_text=$(python3 - "$stderr_file" <<'PY'
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").strip()
print(text)
PY
    )
  fi

  if [ -n "$normalized_text" ]; then
    printf '%s\n' "$normalized_text"
  elif [ -n "$stderr_text" ]; then
    printf '%s\n' "$stderr_text"
  else
    printf '%s\n' "$fallback_message"
  fi
}

build_query_graph_payload() {
  local project_id="$1"
  local query="$2"

  python3 - "$project_id" "$query" <<'PY'
import json
import sys

payload = {
    "project": sys.argv[1],
    "query": sys.argv[2],
    "max_rows": 5,
}

print(json.dumps(payload, separators=(",", ":")))
PY
}

apply_query_injection() {
  local query_name="$1"
  local query_text="$2"
  local inject_name="${GDSCRIPT_PROOF_INJECT_INVALID_QUERY:-}"

  if [ "$inject_name" = "$query_name" ]; then
    printf '%s' "$GDSCRIPT_INVALID_QUERY_FRAGMENT"
  else
    printf '%s' "$query_text"
  fi
}

write_query_wrapper_json() {
  local output_file="$1"
  local query_name="$2"
  local project_id="$3"
  local artifact_slug="$4"
  local query_text="$5"
  local result_source="$6"

  python3 - "$output_file" "$query_name" "$project_id" "$artifact_slug" "$query_text" "$result_source" <<'PY'
import json
import pathlib
import sys

output_file, query_name, project_id, artifact_slug, query_text, result_source = sys.argv[1:7]

raw_text = pathlib.Path(result_source).read_text(encoding="utf-8")

try:
    result = json.loads(raw_text)
except json.JSONDecodeError:
    result = raw_text.strip()

payload = {
    "query_name": query_name,
    "project_id": project_id,
    "artifact_slug": artifact_slug,
    "query": query_text,
    "result": result,
}

with open(output_file, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

run_query_suite() {
  local project_id="$1"
  local repo_slug="$2"
  local queries_dir="$3"
  local log_file="$4"

  local i
  local query_name
  local query_file
  local query_text
  local query_result_file
  local injected_query
  local args_json
  local -a fallback_tools=()

  GDSCRIPT_LAST_QUERY_NAME=""
  GDSCRIPT_QUERY_FALLBACK_TOOLS=""
  fallback_tools=()

  for i in "${!GDSCRIPT_QUERY_NAMES[@]}"; do
    query_name=${GDSCRIPT_QUERY_NAMES[$i]}
    query_file=${GDSCRIPT_QUERY_FILES[$i]}
    query_text=${GDSCRIPT_QUERIES[$i]}
    query_result_file="$queries_dir/$query_file"
    GDSCRIPT_LAST_QUERY_NAME="$query_name"

    injected_query=$(apply_query_injection "$query_name" "$query_text")
    args_json=$(build_query_graph_payload "$project_id" "$injected_query")

    if ! run_cbm_cli_json "$log_file" "$query_result_file" query_graph "$args_json"; then
      GDSCRIPT_QUERY_FALLBACK_TOOLS="${fallback_tools[*]-}"
      return 1
    fi

    if [ "$CBM_LAST_CAPTURE_MODE" = "wrapped_fallback" ]; then
      fallback_tools+=("query_graph:$query_name")
    fi

    if ! write_query_wrapper_json "$query_result_file" "$query_name" "$project_id" "$repo_slug" "$injected_query" "$query_result_file"; then
      GDSCRIPT_QUERY_FALLBACK_TOOLS="${fallback_tools[*]-}"
      return 1
    fi
  done

  GDSCRIPT_QUERY_FALLBACK_TOOLS="${fallback_tools[*]-}"

  return 0
}

run_cbm_cli_json() {
  local log_file="$1"
  local raw_output_file="$2"
  local tool_name="$3"
  local args_json="${4-}"

  local stdout_tmp
  local stderr_tmp
  local normalized_tmp
  local command_line
  local mode
  local rc=0
  local normalized_text=""
  local -a command

  stdout_tmp=$(make_run_temp cbm-stdout)
  stderr_tmp=$(make_run_temp cbm-stderr)
  normalized_tmp=$(make_run_temp cbm-normalized)
  CBM_LAST_ERROR=""
  CBM_LAST_CAPTURE_MODE=""

  command=("$BINARY_PATH" cli --raw "$tool_name")
  if [ -n "$args_json" ]; then
    command+=("$args_json")
  fi
  command_line=$(format_command_for_log "${command[@]}")
  log_command "$command_line"
  if run_logged_command_capture "$WORKTREE_PATH" "$stdout_tmp" "$stderr_tmp" "${command[@]}"; then
    rc=0
  else
    rc=$?
  fi
  append_command_capture "$log_file" "$command_line" "$stdout_tmp" "$stderr_tmp"

  if ! mode=$(normalize_cli_output "$stdout_tmp" "$normalized_tmp"); then
    CBM_LAST_ERROR=$(describe_failure_output "$normalized_tmp" "$stderr_tmp" "failed to parse CLI output")
    rm -f "$stdout_tmp" "$stderr_tmp" "$normalized_tmp"
    return 1
  fi

  normalized_text=$(python3 - "$normalized_tmp" <<'PY'
import pathlib
import sys

print(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").strip())
PY
  )

  if [ "$mode" = "raw" ] && [ "$rc" -eq 0 ]; then
    CBM_LAST_CAPTURE_MODE="raw"
    mv "$normalized_tmp" "$raw_output_file"
    rm -f "$stdout_tmp" "$stderr_tmp"
    return 0
  fi

  if [ "$mode" = "wrapped_error" ] && [[ "$normalized_text" == "unknown tool: --raw" ]]; then
    command=("$BINARY_PATH" cli "$tool_name")
    if [ -n "$args_json" ]; then
      command+=("$args_json")
    fi
    command_line=$(format_command_for_log "${command[@]}")
    log_command "$command_line"
    : > "$stdout_tmp"
    : > "$stderr_tmp"
    if run_logged_command_capture "$WORKTREE_PATH" "$stdout_tmp" "$stderr_tmp" "${command[@]}"; then
      rc=0
    else
      rc=$?
    fi
    append_command_capture "$log_file" "$command_line" "$stdout_tmp" "$stderr_tmp"

    if ! mode=$(normalize_cli_output "$stdout_tmp" "$normalized_tmp"); then
      CBM_LAST_ERROR=$(describe_failure_output "$normalized_tmp" "$stderr_tmp" "failed to parse fallback CLI output")
      rm -f "$stdout_tmp" "$stderr_tmp" "$normalized_tmp"
      return 1
    fi

    normalized_text=$(python3 - "$normalized_tmp" <<'PY'
import pathlib
import sys

print(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").strip())
PY
    )

    if [ "$mode" = "wrapped_ok" ] && [ "$rc" -eq 0 ]; then
      CBM_LAST_CAPTURE_MODE="wrapped_fallback"
      mv "$normalized_tmp" "$raw_output_file"
      rm -f "$stdout_tmp" "$stderr_tmp"
      return 0
    fi
  fi

  CBM_LAST_ERROR=$(describe_failure_output "$normalized_tmp" "$stderr_tmp" "CLI command failed")
  rm -f "$stdout_tmp" "$stderr_tmp" "$normalized_tmp"
  return 1
}

resolve_project_id_from_list_projects() {
  local repo_path="$1"
  local list_projects_file="$2"

  python3 - "$repo_path" "$list_projects_file" <<'PY'
import json
import os
import sys

repo_path = os.path.realpath(sys.argv[1])
list_projects_path = sys.argv[2]

with open(list_projects_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

projects = payload.get("projects")
if not isinstance(projects, list):
    raise SystemExit(2)

matches = []
for project in projects:
    if not isinstance(project, dict):
        continue
    root_path = project.get("root_path")
    name = project.get("name")
    if not isinstance(root_path, str) or not isinstance(name, str):
        continue
    if os.path.realpath(root_path) == repo_path:
        matches.append(name)

if len(matches) != 1:
    raise SystemExit(1)

print(matches[0])
PY
}

update_repo_meta_json_task5() {
  local repo_meta_file="$1"
  local project_id="$2"
  local index_status="$3"
  local project_resolution_status="$4"
  local overall_status="$5"
  local status_message="$6"
  local cli_capture_mode="$7"
  local cli_capture_note="$8"

  python3 - "$repo_meta_file" "$project_id" "$index_status" "$project_resolution_status" "$overall_status" "$status_message" "$cli_capture_mode" "$cli_capture_note" <<'PY'
import json
import sys

repo_meta_file, project_id, index_status, resolution_status, overall_status, status_message, cli_capture_mode, cli_capture_note = sys.argv[1:9]

with open(repo_meta_file, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

payload["project_id"] = None if project_id in ("", "null") else project_id
payload["task5"] = {
    "indexing": {
        "status": index_status,
        "log_file": "index.log",
    },
    "project_resolution": {
        "status": resolution_status,
    },
    "cli_capture": {
        "mode": cli_capture_mode,
        "note": None if cli_capture_note == "" else cli_capture_note,
    },
    "status": overall_status,
    "message": None if status_message == "" else status_message,
}

with open(repo_meta_file, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

set_repo_task5_status() {
  local repo_index="$1"
  local project_id="$2"
  local index_status="$3"
  local resolution_status="$4"
  local overall_status="$5"
  local status_message="$6"
  local cli_capture_mode="$7"
  local cli_capture_note="$8"
  local slug="${REPO_SLUGS[$repo_index]}"
  local repo_meta_file="$RUN_ROOT/$slug/repo-meta.json"

  REPO_PROJECT_IDS[$repo_index]="$project_id"
  REPO_INDEX_STATUSES[$repo_index]="$index_status"
  REPO_PROJECT_RESOLUTION_STATUSES[$repo_index]="$resolution_status"
  REPO_OVERALL_STATUSES[$repo_index]="$overall_status"
  REPO_STATUS_MESSAGES[$repo_index]="$status_message"
  REPO_CLI_CAPTURE_MODES[$repo_index]="$cli_capture_mode"
  REPO_CLI_CAPTURE_NOTES[$repo_index]="$cli_capture_note"

  update_repo_meta_json_task5 "$repo_meta_file" "$project_id" "$index_status" "$resolution_status" "$overall_status" "$status_message" "$cli_capture_mode" "$cli_capture_note"
}

validate_index_output() {
  local index_output_file="$1"

  python3 - "$index_output_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)

if not isinstance(payload, dict):
    print("index_repository returned non-object JSON")
    raise SystemExit(1)

status = payload.get("status")
if status == "indexed":
    raise SystemExit(0)

if isinstance(payload.get("error"), str) and payload["error"]:
    print(payload["error"])
elif isinstance(status, str) and status:
    print(f"unexpected index status: {status}")
else:
    print("index_repository did not report indexed status")
raise SystemExit(1)
PY
}

build_local_binary() {
  local stdout_tmp
  local stderr_tmp
  local command_line
  local rc=0
  local built_binary_path="$WORKTREE_PATH/build/c/codebase-memory-mcp"

  stdout_tmp=$(make_run_temp build-stdout)
  stderr_tmp=$(make_run_temp build-stderr)
  command_line=$(format_command_for_log make -f Makefile.cbm cbm)
  log_command "$command_line"

  if run_logged_command_capture "$WORKTREE_PATH" "$stdout_tmp" "$stderr_tmp" make -f Makefile.cbm cbm; then
    rc=0
  else
    rc=$?
  fi
  append_command_capture "$BUILD_LOG" "$command_line" "$stdout_tmp" "$stderr_tmp"

  if [ "$rc" -eq 0 ] && [ ! -x "$BINARY_PATH" ] && [ -x "$built_binary_path" ]; then
    ln -sfn "c/codebase-memory-mcp" "$BINARY_PATH"
  fi

  if [ "$rc" -ne 0 ] || [ ! -x "$BINARY_PATH" ]; then
    BUILD_STATUS="failed"
    BUILD_MESSAGE=$(describe_failure_output "$stdout_tmp" "$stderr_tmp" "failed to build $BINARY_PATH")
    rm -f "$stdout_tmp" "$stderr_tmp"
    return 1
  fi

  BUILD_STATUS="succeeded"
  BUILD_MESSAGE="binary refreshed at $BINARY_PATH"
  rm -f "$stdout_tmp" "$stderr_tmp"
  return 0
}

process_repo_indexing() {
  local i
  local repo_path
  local slug
  local repo_dir
  local index_log
  local index_json
  local list_projects_json
  local args_json
  local queries_dir
  local project_id
  local failure_message
  local cli_capture_mode
  local cli_capture_note
  local -a fallback_tools

  for i in "${!REPO_PATHS[@]}"; do
    repo_path="${REPO_PATHS[$i]}"
    slug="${REPO_SLUGS[$i]}"
    repo_dir="$RUN_ROOT/$slug"
    index_log="$repo_dir/index.log"
    index_json="$repo_dir/index.json"
    list_projects_json="$repo_dir/list-projects.json"
    queries_dir="$repo_dir/queries"

    : > "$index_log"
    cli_capture_mode="raw"
    cli_capture_note=""
    fallback_tools=()

    if [ "$BUILD_STATUS" != "succeeded" ]; then
      set_repo_task5_status "$i" "" "not_attempted" "not_attempted" "incomplete" "build failed before indexing: $BUILD_MESSAGE" "not_attempted" ""
      continue
    fi

    if [ ! -d "$repo_path" ]; then
      if [ -e "$repo_path" ]; then
        failure_message="repository path is not a directory: $repo_path"
      else
        failure_message="repository path does not exist: $repo_path"
      fi
      set_repo_task5_status "$i" "" "failed" "skipped" "incomplete" "$failure_message" "not_attempted" ""
      continue
    fi

    args_json=$(build_index_payload "$repo_path")
    if ! run_cbm_cli_json "$index_log" "$index_json" index_repository "$args_json"; then
      failure_message=${CBM_LAST_ERROR:-"index_repository failed"}
      if [ "$CBM_LAST_CAPTURE_MODE" = "wrapped_fallback" ]; then
        cli_capture_mode="wrapped_fallback"
        fallback_tools+=("index_repository")
      fi
      cli_capture_note=""
      if [ "${#fallback_tools[@]}" -gt 0 ]; then
        cli_capture_note="wrapped MCP payload fallback used because binary lacks cli --raw support: ${fallback_tools[*]}"
      fi
      set_repo_task5_status "$i" "" "failed" "skipped" "incomplete" "$failure_message" "$cli_capture_mode" "$cli_capture_note"
      continue
    fi

    if [ "$CBM_LAST_CAPTURE_MODE" = "wrapped_fallback" ]; then
      cli_capture_mode="wrapped_fallback"
      fallback_tools+=("index_repository")
    fi

    if ! failure_message=$(validate_index_output "$index_json"); then
      cli_capture_note=""
      if [ "${#fallback_tools[@]}" -gt 0 ]; then
        cli_capture_note="wrapped MCP payload fallback used because binary lacks cli --raw support: ${fallback_tools[*]}"
      fi
      set_repo_task5_status "$i" "" "failed" "skipped" "incomplete" "$failure_message" "$cli_capture_mode" "$cli_capture_note"
      continue
    fi

    if ! run_cbm_cli_json "$index_log" "$list_projects_json" list_projects; then
      failure_message=${CBM_LAST_ERROR:-"list_projects failed"}
      if [ "$CBM_LAST_CAPTURE_MODE" = "wrapped_fallback" ]; then
        cli_capture_mode="wrapped_fallback"
        fallback_tools+=("list_projects")
      fi
      cli_capture_note=""
      if [ "${#fallback_tools[@]}" -gt 0 ]; then
        cli_capture_note="wrapped MCP payload fallback used because binary lacks cli --raw support: ${fallback_tools[*]}"
      fi
      set_repo_task5_status "$i" "" "succeeded" "failed" "incomplete" "$failure_message" "$cli_capture_mode" "$cli_capture_note"
      continue
    fi

    if [ "$CBM_LAST_CAPTURE_MODE" = "wrapped_fallback" ]; then
      cli_capture_mode="wrapped_fallback"
      fallback_tools+=("list_projects")
    fi

    if ! project_id=$(resolve_project_id_from_list_projects "$repo_path" "$list_projects_json"); then
      cli_capture_note=""
      if [ "${#fallback_tools[@]}" -gt 0 ]; then
        cli_capture_note="wrapped MCP payload fallback used because binary lacks cli --raw support: ${fallback_tools[*]}"
      fi
      set_repo_task5_status "$i" "" "succeeded" "failed" "incomplete" "unable to resolve project_id from list_projects for $repo_path" "$cli_capture_mode" "$cli_capture_note"
      continue
    fi

    mkdir -p "$queries_dir"

    cli_capture_note=""
    if [ "${#fallback_tools[@]}" -gt 0 ]; then
      cli_capture_note="wrapped MCP payload fallback used because binary lacks cli --raw support: ${fallback_tools[*]}"
    fi

    if ! run_query_suite "$project_id" "$slug" "$queries_dir" "$index_log"; then
      failure_message="query '${GDSCRIPT_LAST_QUERY_NAME:-unknown}' failed: ${CBM_LAST_ERROR:-query suite failed}"
      if [ "$CBM_LAST_CAPTURE_MODE" = "wrapped_fallback" ]; then
        cli_capture_mode="wrapped_fallback"
      fi
      if [ -n "$GDSCRIPT_QUERY_FALLBACK_TOOLS" ]; then
        for query_tool in $GDSCRIPT_QUERY_FALLBACK_TOOLS; do
          fallback_tools+=("$query_tool")
        done
      fi
      cli_capture_note=""
      if [ "${#fallback_tools[@]}" -gt 0 ]; then
        cli_capture_note="wrapped MCP payload fallback used because binary lacks cli --raw support: ${fallback_tools[*]}"
      fi
      set_repo_task5_status "$i" "$project_id" "succeeded" "resolved" "incomplete" "$failure_message" "$cli_capture_mode" "$cli_capture_note"
      continue
    fi

    if [ -n "$GDSCRIPT_QUERY_FALLBACK_TOOLS" ]; then
      for query_tool in $GDSCRIPT_QUERY_FALLBACK_TOOLS; do
        fallback_tools+=("$query_tool")
      done
    fi

    if [ "$CBM_LAST_CAPTURE_MODE" = "wrapped_fallback" ]; then
      cli_capture_mode="wrapped_fallback"
    fi

    cli_capture_note=""
    if [ "${#fallback_tools[@]}" -gt 0 ]; then
      cli_capture_note="wrapped MCP payload fallback used because binary lacks cli --raw support: ${fallback_tools[*]}"
    fi

    set_repo_task5_status "$i" "$project_id" "succeeded" "resolved" "complete" "" "$cli_capture_mode" "$cli_capture_note"
  done
}

write_repo_and_aggregate_summaries() {
  {
    IFS= read -r AGG_INDEXING_COVERAGE
    IFS= read -r AGG_SIGNAL_COVERAGE
    IFS= read -r AGG_IMPORTS_COVERAGE
    IFS= read -r AGG_INHERITS_COVERAGE
    IFS= read -r AGGREGATE_PASS
    IFS= read -r AGGREGATE_MISSING_CATEGORIES
  } < <(python3 - "$RUN_ROOT" "$AGGREGATE_SUMMARY" "$BINARY_PATH" "$BUILD_STATUS" "$BUILD_MESSAGE" "$WORKTREE_PATH" "$WORKTREE_BRANCH" "$WORKTREE_COMMIT" "${REPO_SLUGS[@]}" <<'PY'
import json
import sys
from pathlib import Path

(
    run_root,
    aggregate_summary_path,
    binary_path,
    build_status,
    build_message,
    worktree_path,
    worktree_branch,
    worktree_commit,
    *slugs,
) = sys.argv[1:]

run_root_path = Path(run_root)
aggregate_summary_file = Path(aggregate_summary_path)

QUERY_SPECS = [
    ("gd-files.json", "gd-files", "gd_files"),
    ("gd-classes.json", "gd-classes", "gd_classes"),
    ("gd-methods.json", "gd-methods", "gd_methods"),
    ("gd-class-sample.json", "gd-class-sample", None),
    ("gd-method-sample.json", "gd-method-sample", None),
    ("signal-calls.json", "signal-calls", "signal_calls"),
    ("gd-inherits.json", "gd-inherits", "gd_inherits_edges"),
    ("gd-imports.json", "gd-imports", "gd_deps"),
]


def version_major(version):
    if version in (None, ""):
        return None
    text = str(version).strip().lower()
    if text.startswith("godot "):
        text = text[6:].strip()
    if text.startswith("v"):
        text = text[1:]
    major = text.split(".", 1)[0]
    return major if major.isdigit() else None


def version_label(version):
    if version in (None, ""):
        return "unknown"
    text = str(version).strip()
    if text.lower().startswith("godot "):
        return text
    return f"Godot {text}"


def escape_cell(text):
    return str(text).replace("\r", " ").replace("\n", " <br> ").replace("|", "\\|")


def markdown_code(value, empty="unknown"):
    text = empty if value in (None, "") else str(value)
    return f"`{text.replace('`', '\\`')}`"


def extract_count(wrapper, key):
    if not isinstance(wrapper, dict):
        raise ValueError("wrapper payload is not an object")
    result = wrapper.get("result")
    if not isinstance(result, dict):
        raise ValueError("wrapper result is not an object")
    rows = result.get("rows")
    if rows in (None, []):
        return 0
    if not isinstance(rows, list) or not rows:
        raise ValueError("wrapper rows missing")
    first_row = rows[0]
    if not isinstance(first_row, list) or not first_row:
        raise ValueError("wrapper first row missing")
    value = first_row[0]
    if isinstance(value, str):
        value = value.strip()
    if value in (None, ""):
        return 0
    return int(value)


def load_query_data(repo_dir):
    queries_dir = repo_dir / "queries"
    counts = {
        "gd_files": None,
        "gd_classes": None,
        "gd_methods": None,
        "signal_calls": None,
        "gd_inherits_edges": None,
        "gd_deps": None,
    }
    issues = []
    present = []

    for filename, expected_query_name, count_key in QUERY_SPECS:
      query_file = queries_dir / filename
      if not query_file.exists():
          issues.append(f"missing {filename}")
          continue

      try:
          payload = json.loads(query_file.read_text(encoding="utf-8"))
      except Exception as exc:  # noqa: BLE001
          issues.append(f"unparseable {filename}: {exc}")
          continue

      if not isinstance(payload, dict):
          issues.append(f"invalid {filename}: wrapper is not an object")
          continue
      if payload.get("query_name") != expected_query_name:
          issues.append(
              f"invalid {filename}: expected query_name {expected_query_name!r}, got {payload.get('query_name')!r}"
          )
          continue

      present.append(filename)
      if count_key is None:
          continue

      try:
          counts[count_key] = extract_count(payload, count_key)
      except Exception as exc:  # noqa: BLE001
          issues.append(f"invalid {filename}: {exc}")

    return counts, issues, present


def repo_summary_data(run_root_path, slug):
    repo_dir = run_root_path / slug
    repo_meta_file = repo_dir / "repo-meta.json"
    meta = json.loads(repo_meta_file.read_text(encoding="utf-8"))
    task_state = meta.get("task5") or {}
    indexing_state = (task_state.get("indexing") or {}).get("status", "pending")
    project_resolution_state = (task_state.get("project_resolution") or {}).get("status", "pending")
    overall_status = task_state.get("status", "pending")
    overall_message = task_state.get("message")
    cli_capture = task_state.get("cli_capture") or {}
    cli_mode = cli_capture.get("mode", "unknown")
    cli_note = cli_capture.get("note")
    counts, query_issues, _present = load_query_data(repo_dir)

    project_id = meta.get("project_id")
    godot_version = meta.get("godot_version")
    godot_label = version_label(godot_version)
    major = version_major(godot_version)
    qualifies_godot4x = bool(meta.get("qualifies_godot4x"))
    confirmed_3x = major == "3"
    repo_complete = (
        indexing_state == "succeeded"
        and project_resolution_state == "resolved"
        and overall_status == "complete"
        and project_id not in (None, "")
        and not query_issues
    )

    gd_files = counts["gd_files"]
    gd_classes = counts["gd_classes"]
    gd_methods = counts["gd_methods"]
    signal_calls = counts["signal_calls"]
    gd_inherits_edges = counts["gd_inherits_edges"]
    gd_deps = counts["gd_deps"]

    indexing_coverage = bool(
        repo_complete
        and qualifies_godot4x
        and (gd_files or 0) > 0
        and (gd_classes or 0) > 0
        and (gd_methods or 0) > 0
    )
    category_eligible = repo_complete and not confirmed_3x
    signal_coverage = bool(category_eligible and (signal_calls or 0) > 0)
    imports_coverage = bool(category_eligible and (gd_deps or 0) > 0)
    inherits_coverage = bool(category_eligible and (gd_inherits_edges or 0) > 0)

    if qualifies_godot4x:
        qualification_status = f"confirmed {godot_label}; eligible for indexing coverage"
    elif confirmed_3x:
        qualification_status = f"confirmed {godot_label}; excluded from all acceptance categories"
    elif godot_version in (None, ""):
        qualification_status = "unknown Godot version; non-qualifying for the Godot 4.x indexing requirement"
    else:
        qualification_status = f"{godot_label}; non-qualifying for the Godot 4.x indexing requirement"

    proof_points = []
    if (gd_files or 0) > 0 and (gd_classes or 0) > 0 and (gd_methods or 0) > 0:
        if qualifies_godot4x:
            proof_points.append("non-zero .gd file/class/method indexing on a confirmed Godot 4.x repo")
        else:
            proof_points.append("non-zero .gd file/class/method indexing")
    if (signal_calls or 0) > 0:
        proof_points.append("signal CALLS coverage")
    if (gd_deps or 0) > 0:
        proof_points.append(".gd IMPORTS coverage")
    if (gd_inherits_edges or 0) > 0:
        proof_points.append(".gd INHERITS coverage")

    notes = []
    if proof_points:
        notes.append("Proves " + ", ".join(proof_points) + ".")
    else:
        notes.append("Does not currently satisfy any aggregate acceptance category.")
    if not repo_complete:
        if overall_message:
            notes.append(f"Incomplete: {overall_message}.")
        if query_issues:
            notes.append("Missing or invalid query artifacts: " + "; ".join(query_issues) + ".")
    if confirmed_3x:
        notes.append("Confirmed Godot 3.x repos cannot contribute to aggregate acceptance.")
    elif not qualifies_godot4x:
        notes.append("This repo cannot satisfy the Godot 4.x indexing requirement.")

    counts_display = {
        "gd_files": "n/a" if gd_files is None else str(gd_files),
        "gd_classes": "n/a" if gd_classes is None else str(gd_classes),
        "gd_methods": "n/a" if gd_methods is None else str(gd_methods),
        "signal_calls": "n/a" if signal_calls is None else str(signal_calls),
        "gd_inherits_edges": "n/a" if gd_inherits_edges is None else str(gd_inherits_edges),
        "gd_deps": "n/a" if gd_deps is None else str(gd_deps),
    }

    cli_summary = cli_mode
    if cli_note:
        cli_summary += f" — {cli_note}"

    summary_lines = [
        "# GDScript Proof Summary",
        "",
        f"- Repo path: {markdown_code(meta.get('repo_path'))}",
        f"- Repo name: {markdown_code(meta.get('repo_name'))}",
        f"- Artifact slug: {markdown_code(meta.get('artifact_slug'))}",
        f"- Resolved project ID: {markdown_code(project_id)}",
        f"- Git ref: {markdown_code(meta.get('git_ref'))}",
        f"- Git commit: {markdown_code(meta.get('git_commit'))}",
        f"- Git branch: {markdown_code(meta.get('git_branch'))}",
        f"- Godot version: {markdown_code(godot_version)}",
        f"- Qualification status: {qualification_status}",
        f"- Repo complete: {markdown_code(str(repo_complete).lower())}",
        f"- Indexing status: {markdown_code(indexing_state)}",
        f"- Project resolution status: {markdown_code(project_resolution_state)}",
        f"- Overall status: {markdown_code(overall_status)}",
        f"- CLI capture: {cli_summary}",
        f"- `.gd` files: {markdown_code(counts_display['gd_files'], empty='n/a')}",
        f"- `.gd` classes: {markdown_code(counts_display['gd_classes'], empty='n/a')}",
        f"- `.gd` methods: {markdown_code(counts_display['gd_methods'], empty='n/a')}",
        f"- Signal calls: {markdown_code(counts_display['signal_calls'], empty='n/a')}",
        f"- `.gd` inherits: {markdown_code(counts_display['gd_inherits_edges'], empty='n/a')}",
        f"- `.gd` imports: {markdown_code(counts_display['gd_deps'], empty='n/a')}",
        "",
        "## Coverage contributions",
        f"- Indexing coverage: {markdown_code('yes' if indexing_coverage else 'no')}",
        f"- Signal coverage: {markdown_code('yes' if signal_coverage else 'no')}",
        f"- Imports coverage: {markdown_code('yes' if imports_coverage else 'no')}",
        f"- Inherits coverage: {markdown_code('yes' if inherits_coverage else 'no')}",
        "",
        "## What this repo proves",
    ]
    for note in notes:
        summary_lines.append(f"- {note}")
    if not repo_complete:
        summary_lines.extend(["", "## Incomplete or failed status"])
        if overall_message:
            summary_lines.append(f"- {overall_message}")
        if query_issues:
            for issue in query_issues:
                summary_lines.append(f"- {issue}")

    summary_file = repo_dir / "summary.md"
    summary_file.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {
        "repo_path": meta.get("repo_path", ""),
        "repo_name": meta.get("repo_name", ""),
        "artifact_slug": meta.get("artifact_slug", slug),
        "summary_relpath": f"{slug}/summary.md",
        "qualification_status": qualification_status,
        "repo_complete": repo_complete,
        "indexing_status": indexing_state,
        "project_resolution_status": project_resolution_state,
        "overall_status": overall_status,
        "overall_message": overall_message or "",
        "cli_summary": cli_summary,
        "godot_version": "unknown" if godot_version in (None, "") else str(godot_version),
        "gd_files": counts_display["gd_files"],
        "gd_classes": counts_display["gd_classes"],
        "gd_methods": counts_display["gd_methods"],
        "signal_calls": counts_display["signal_calls"],
        "gd_inherits_edges": counts_display["gd_inherits_edges"],
        "gd_deps": counts_display["gd_deps"],
        "indexing_coverage": indexing_coverage,
        "signal_coverage": signal_coverage,
        "imports_coverage": imports_coverage,
        "inherits_coverage": inherits_coverage,
        "note": " ".join(notes),
    }


repo_summaries = [repo_summary_data(run_root_path, slug) for slug in slugs]

indexing_contributors = [repo for repo in repo_summaries if repo["indexing_coverage"]]
signal_contributors = [repo for repo in repo_summaries if repo["signal_coverage"]]
imports_contributors = [repo for repo in repo_summaries if repo["imports_coverage"]]
inherits_contributors = [repo for repo in repo_summaries if repo["inherits_coverage"]]

indexing_coverage = bool(indexing_contributors)
signal_coverage = bool(signal_contributors)
imports_coverage = bool(imports_contributors)
inherits_coverage = bool(inherits_contributors)
aggregate_pass = indexing_coverage and signal_coverage and imports_coverage and inherits_coverage

missing_categories = []
if not indexing_coverage:
    missing_categories.append("indexing_coverage")
if not signal_coverage:
    missing_categories.append("signal_coverage")
if not imports_coverage:
    missing_categories.append("imports_coverage")
if not inherits_coverage:
    missing_categories.append("inherits_coverage")


def contributor_text(items):
    if not items:
        return "none"
    return ", ".join(f"{item['artifact_slug']} ({item['repo_path']})" for item in items)


lines = [
    "# GDScript Proof Aggregate Summary",
    "",
    f"- Run root: {markdown_code(run_root)}",
    f"- Binary: {markdown_code(binary_path)}",
    f"- Build status: {markdown_code(build_status)}",
]
if build_message:
    lines.append(f"- Build note: {build_message}")
lines.extend(
    [
        f"- codebase-memory-mcp worktree under test: {markdown_code(worktree_path)}",
        f"- codebase-memory-mcp branch under test: {markdown_code(worktree_branch)}",
        f"- codebase-memory-mcp commit under test: {markdown_code(worktree_commit)}",
        f"- Final acceptance: {markdown_code('passed' if aggregate_pass else 'failed')}",
        f"- Missing coverage categories: {markdown_code(', '.join(missing_categories) if missing_categories else 'none', empty='none')}",
        "",
        "## Coverage results",
        f"- indexing_coverage: {markdown_code('pass' if indexing_coverage else 'fail')} — {contributor_text(indexing_contributors)}",
        f"- signal_coverage: {markdown_code('pass' if signal_coverage else 'fail')} — {contributor_text(signal_contributors)}",
        f"- imports_coverage: {markdown_code('pass' if imports_coverage else 'fail')} — {contributor_text(imports_contributors)}",
        f"- inherits_coverage: {markdown_code('pass' if inherits_coverage else 'fail')} — {contributor_text(inherits_contributors)}",
        "",
        "## Repos processed",
    ]
)

for repo in repo_summaries:
    lines.append(
        f"- {markdown_code(repo['repo_path'])} → {markdown_code(repo['artifact_slug'])} ({markdown_code(repo['summary_relpath'], empty='summary.md')})"
    )

lines.extend(
    [
        "",
        "## Per-repo results",
        "| Repo | Artifact slug | Godot version | Qualification | Complete | .gd files | .gd classes | .gd methods | Signal calls | .gd inherits | .gd imports | Indexing | Signal | Imports | Inherits | Overall | Note |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
)

for repo in repo_summaries:
    lines.append(
        "| {repo} | {slug} | {godot} | {qualification} | {complete} | {gd_files} | {gd_classes} | {gd_methods} | {signal_calls} | {gd_inherits} | {gd_imports} | {indexing} | {signal} | {imports} | {inherits} | {overall} | {note} |".format(
            repo=markdown_code(repo["repo_path"]),
            slug=markdown_code(repo["artifact_slug"]),
            godot=markdown_code(repo["godot_version"]),
            qualification=escape_cell(repo["qualification_status"]),
            complete=markdown_code("true" if repo["repo_complete"] else "false"),
            gd_files=markdown_code(repo["gd_files"], empty="n/a"),
            gd_classes=markdown_code(repo["gd_classes"], empty="n/a"),
            gd_methods=markdown_code(repo["gd_methods"], empty="n/a"),
            signal_calls=markdown_code(repo["signal_calls"], empty="n/a"),
            gd_inherits=markdown_code(repo["gd_inherits_edges"], empty="n/a"),
            gd_imports=markdown_code(repo["gd_deps"], empty="n/a"),
            indexing=markdown_code("yes" if repo["indexing_coverage"] else "no"),
            signal=markdown_code("yes" if repo["signal_coverage"] else "no"),
            imports=markdown_code("yes" if repo["imports_coverage"] else "no"),
            inherits=markdown_code("yes" if repo["inherits_coverage"] else "no"),
            overall=markdown_code(repo["overall_status"]),
            note=escape_cell(repo["note"]),
        )
    )

aggregate_summary_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

print("true" if indexing_coverage else "false")
print("true" if signal_coverage else "false")
print("true" if imports_coverage else "false")
print("true" if inherits_coverage else "false")
print("true" if aggregate_pass else "false")
print(",".join(missing_categories))
PY
  )
}

final_exit_code() {
  [ "$AGGREGATE_PASS" = "true" ]
}

prepare_repo_metadata() {
  local i
  local repo_path
  local repo_name
  local repo_label
  local repo_godot_version
  local slug
  local git_ref
  local git_commit
  local git_branch
  local qualifies
  for i in "${!REPO_PATHS[@]}"; do
    repo_path="${REPO_PATHS[$i]}"
    repo_label="${REPO_LABELS[$i]}"
    repo_godot_version="${REPO_GODOT_VERSIONS[$i]}"

    repo_name=$(basename -- "$repo_path")
    {
      IFS= read -r slug
      IFS= read -r git_ref
      IFS= read -r git_commit
      IFS= read -r git_branch
      IFS= read -r qualifies
    } < <(collect_repo_metadata_fields "$repo_path" "$repo_godot_version")

    REPO_SLUGS[$i]="$slug"

    REPO_GIT_REFS[$i]="$git_ref"
    REPO_GIT_COMMITS[$i]="$git_commit"
    REPO_GIT_BRANCHES[$i]="$git_branch"

    mkdir -p "$RUN_ROOT/$slug"
    write_repo_meta_json "$RUN_ROOT/$slug" "$repo_path" "$repo_label" "$repo_name" "$slug" "null" "$RUN_TIMESTAMP" "$git_ref" "$git_commit" "$git_branch" "$repo_godot_version" "$qualifies"
  done
}

parse_args() {
  local repo_path
  local raw_value
  local label
  local version

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --repo)
        if [[ $# -lt 2 ]]; then
          echo "error: --repo requires a repository path" >&2
          usage >&2
          exit 2
        fi

        if ! repo_path=$(parse_repo_path "$2"); then
          exit 2
        fi

        ensure_repo_entry "$repo_path"
        shift 2
        ;;
      --label)
        if [[ $# -lt 2 ]]; then
          echo "error: --label requires repo=value form" >&2
          usage >&2
          exit 2
        fi

        if ! raw_value=$(parse_metadata_assignment "$2"); then
          exit 2
        fi

        repo_path=${raw_value%%$'\n'*}
        label=${raw_value#*$'\n'}
        if ! repo_path=$(parse_repo_path "$repo_path"); then
          exit 2
        fi

        if ! assert_repo_declared "$repo_path"; then
          exit 2
        fi

        set_repo_metadata "$repo_path" "$label" ""
        shift 2
        ;;
      --godot-version)
        if [[ $# -lt 2 ]]; then
          echo "error: --godot-version requires repo=value form" >&2
          usage >&2
          exit 2
        fi

        if ! raw_value=$(parse_metadata_assignment "$2"); then
          exit 2
        fi

        repo_path=${raw_value%%$'\n'*}
        version=${raw_value#*$'\n'}
        if ! repo_path=$(parse_repo_path "$repo_path"); then
          exit 2
        fi

        if ! assert_repo_declared "$repo_path"; then
          exit 2
        fi

        set_repo_metadata "$repo_path" "" "$version"
        shift 2
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        echo "error: unknown option '$1'" >&2
        usage >&2
        exit 2
        ;;
    esac
  done

  if (( ${#REPO_PATHS[@]} == 0 )); then
    echo "error: at least one --repo is required" >&2
    usage >&2
    exit 2
  fi

  return 0
}

record_repo_metadata_env() {
  local i
  local idx

  if (( ${#REPO_PATHS[@]} == 0 )); then
    return 0
  fi

  {
    printf '%s\n' "repo_count=${#REPO_PATHS[@]}"

    for i in "${!REPO_PATHS[@]}"; do
      idx=$((i + 1))
      printf '%s\n' "repo_${idx}_path=${REPO_PATHS[$i]}"
      printf '%s\n' "repo_${idx}_label=${REPO_LABELS[$i]}"
      printf '%s\n' "repo_${idx}_godot_version=${REPO_GODOT_VERSIONS[$i]}"
    done
  } >> "$ENV_FILE"
}

ensure_workspace_root() {
  mkdir -p "$RUN_ROOT"
}

ensure_workspace_state_dirs() {
  mkdir -p "$STATE_HOME" "$STATE_CONFIG" "$STATE_CACHE"
  mkdir -p "$EFFECTIVE_STORE_ROOT"
  mkdir -p "$TMP_ROOT"
}

ensure_workspace_compat_links() {
  ln -sfn "$STATE_CACHE" "$STATE_HOME/.cache"
  ln -sfn "$EFFECTIVE_STORE_ROOT" "$STATE_STORE"
}

initialize_workspace_logs() {
  : > "$ENV_FILE"
  : > "$COMMANDS_LOG"

  log_command "export HOME=\"$STATE_HOME\""
  log_command "export XDG_CONFIG_HOME=\"$STATE_CONFIG\""
  log_command "export XDG_CACHE_HOME=\"$STATE_CACHE\""
  log_command "ln -sfn \"$STATE_CACHE\" \"$STATE_HOME/.cache\""
  log_command "ln -sfn \"$EFFECTIVE_STORE_ROOT\" \"$STATE_STORE\""
}

record_workspace_env() {
  local worktree_path="$1"
  local worktree_branch
  local worktree_sha

  worktree_branch=$(git -C "$worktree_path" rev-parse --abbrev-ref HEAD 2>/dev/null || true)
  worktree_branch=${worktree_branch:-"<unknown>"}

  worktree_sha=$(git -C "$worktree_path" rev-parse HEAD 2>/dev/null || true)
  worktree_sha=${worktree_sha:-"<unknown>"}

  WORKTREE_BRANCH=$worktree_branch
  WORKTREE_COMMIT=$worktree_sha

  {
    printf '%s\n' "run_timestamp_utc=$RUN_TIMESTAMP"
    printf '%s\n' "run_root=$RUN_ROOT"
    printf '%s\n' "worktree_path=$worktree_path"
    printf '%s\n' "worktree_branch=$worktree_branch"
    printf '%s\n' "worktree_commit_sha=$worktree_sha"
    printf '%s\n' "binary_path=$WORKTREE_PATH/build/codebase-memory-mcp"
    printf '%s\n' "home=$STATE_HOME"
    printf '%s\n' "xdg_config_home=$STATE_CONFIG"
    printf '%s\n' "xdg_cache_home=$STATE_CACHE"
    printf '%s\n' "store_root=$EFFECTIVE_STORE_ROOT"
  } > "$ENV_FILE"

  record_repo_metadata_env
}

setup_workspace() {
  ensure_workspace_root
  ensure_workspace_state_dirs
  ensure_workspace_compat_links
  initialize_workspace_logs
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

parse_args "$@"
initialize_workspace_root
setup_workspace
apply_isolated_runtime_env
record_workspace_env "$WORKTREE_PATH"
prepare_repo_metadata
build_local_binary || true
process_repo_indexing
write_repo_and_aggregate_summaries

printf 'Proof run root: %s\n' "$RUN_ROOT"

if final_exit_code; then
  exit 0
fi

exit 1
