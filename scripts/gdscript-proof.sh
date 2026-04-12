#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/gdscript-proof.sh [--manifest path/to/manifest.json] --repo /abs/path/to/repo [--repo /abs/path/to/repo2 ...] [--godot-version REPO=4.x] [--label REPO=name]

The command stores paths after canonicalizing to absolute real paths.
In manifest mode, every repo should have a label declared in the manifest and missing Godot versions are filled from manifest metadata when available.
EOF
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
WORKTREE_PATH=$(cd -- "$SCRIPT_DIR/.." && pwd -P)

ARTIFACT_ROOT="$WORKTREE_PATH/.artifacts/gdscript-proof"
RUN_TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN_ROOT=""
STATE_ROOT=""
REPO_STATE_ROOT=""
REPO_STATE_HOME=""
REPO_STATE_CONFIG=""
REPO_STATE_CACHE=""
REPO_STATE_CACHE_STORE=""
TMP_ROOT=""
ENV_FILE=""
COMMANDS_LOG=""
BINARY_PATH=""
BUILD_LOG=""
AGGREGATE_SUMMARY=""
RUN_INDEX_FILE=""
SEMANTIC_PARITY_JSON=""
SEMANTIC_PARITY_MD=""
AGG_INDEXING_COVERAGE="false"
AGG_SIGNAL_COVERAGE="false"
AGG_IMPORTS_COVERAGE="false"
AGG_INHERITS_COVERAGE="false"
AGGREGATE_PASS="false"
AGGREGATE_OUTCOME="fail"
AGGREGATE_MISSING_CATEGORIES=""
AGGREGATE_NOTE=""

BUILD_STATUS="pending"
BUILD_MESSAGE=""
MANIFEST_PATH=""

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
REPO_REQUESTED_MODES=()
REPO_ACTUAL_MODES=()
REPO_COMPARISON_LABELS=()
WORKTREE_BRANCH=""
WORKTREE_COMMIT=""
CBM_LAST_ERROR=""
CBM_LAST_CAPTURE_MODE=""
GDSCRIPT_LAST_QUERY_NAME=""
GDSCRIPT_QUERY_FALLBACK_TOOLS=""
CBM_ACTIVE_FORCE_PIPELINE_MODE=""

GDSCRIPT_QUERY_NAMES=(
  "gd-files"
  "gd-classes"
  "gd-methods"
  "gd-class-sample"
  "gd-method-sample"
  "signal-calls"
  "gd-inherits"
  "gd-imports"
  "signal-call-edges"
  "gd-inherits-edges"
  "gd-import-edges"
  "gd-same-script-calls"
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
  "signal-call-edges.json"
  "gd-inherits-edges.json"
  "gd-import-edges.json"
  "gd-same-script-calls.json"
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
  'MATCH (caller)-[:CALLS]->(t:Function) WHERE caller.file_path ENDS WITH ".gd" AND t.file_path ENDS WITH ".gd" AND t.qualified_name CONTAINS ".signal." RETURN caller.file_path, caller.name, caller.qualified_name, t.file_path, t.name, t.qualified_name ORDER BY caller.file_path, caller.name, t.file_path, t.name'
  'MATCH (a)-[:INHERITS]->(b) WHERE (a.file_path ENDS WITH ".gd") OR (b.file_path ENDS WITH ".gd") RETURN a.file_path, a.name, a.qualified_name, b.file_path, b.name, b.qualified_name ORDER BY a.file_path, a.name, b.file_path, b.name'
  'MATCH (m)-[:IMPORTS]->(n) WHERE m.file_path ENDS WITH ".gd" AND n.file_path ENDS WITH ".gd" RETURN m.file_path, m.name, m.qualified_name, n.file_path, n.name, n.qualified_name ORDER BY m.file_path, n.file_path'
  'MATCH (caller)-[:CALLS]->(t) WHERE caller.file_path ENDS WITH ".gd" AND t.file_path ENDS WITH ".gd" RETURN caller.file_path, caller.name, caller.qualified_name, t.file_path, t.name, t.qualified_name ORDER BY caller.file_path, caller.name, t.file_path, t.name'
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

parse_manifest_path() {
  local manifest_path="$1"

  if ! manifest_path=$(python3 - "$manifest_path" <<'PY'
import os
import sys

path = sys.argv[1]
if os.path.exists(path):
    print(os.path.realpath(path))
else:
    print(os.path.abspath(os.path.normpath(path)))
PY
  ); then
    echo "error: unable to resolve manifest path: $manifest_path" >&2
    return 1
  fi

  printf '%s\n' "$manifest_path"
}

manifest_repo_godot_version() {
  local repo_label="$1"

  if [ -z "$MANIFEST_PATH" ] || [ ! -f "$MANIFEST_PATH" ] || [ -z "$repo_label" ]; then
    return 1
  fi

  python3 - "$MANIFEST_PATH" "$repo_label" <<'PY'
import json
import sys

manifest_path, repo_label = sys.argv[1:3]

with open(manifest_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

for repo in payload.get("repos", []):
    if isinstance(repo, dict) and repo.get("label") == repo_label:
        value = repo.get("godot_version")
        if isinstance(value, str) and value.strip():
            print(value)
            raise SystemExit(0)
        raise SystemExit(1)

raise SystemExit(1)
PY
}

apply_manifest_defaults() {
  local i
  local repo_label
  local manifest_version

  if [ -z "$MANIFEST_PATH" ] || [ ! -f "$MANIFEST_PATH" ]; then
    return 0
  fi

  for i in "${!REPO_PATHS[@]}"; do
    repo_label="${REPO_LABELS[$i]}"
    if [ -z "$repo_label" ] || [ -n "${REPO_GODOT_VERSIONS[$i]}" ]; then
      continue
    fi

    if manifest_version=$(manifest_repo_godot_version "$repo_label" 2>/dev/null); then
      REPO_GODOT_VERSIONS[$i]="$manifest_version"
    fi
  done
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
  REPO_REQUESTED_MODES+=("auto")
  REPO_ACTUAL_MODES+=("pending")
  REPO_COMPARISON_LABELS+=("")
}

append_repo_entry() {
  local repo_path="$1"
  local label="$2"
  local version="$3"
  local requested_mode="$4"
  local comparison_label="$5"

  REPO_PATHS+=("$repo_path")
  REPO_LABELS+=("$label")
  REPO_GODOT_VERSIONS+=("$version")
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
  REPO_REQUESTED_MODES+=("$requested_mode")
  REPO_ACTUAL_MODES+=("pending")
  REPO_COMPARISON_LABELS+=("$comparison_label")
}

expand_manifest_dual_mode_entries() {
  local -a original_paths=("${REPO_PATHS[@]}")
  local -a original_labels=("${REPO_LABELS[@]}")
  local -a original_versions=("${REPO_GODOT_VERSIONS[@]}")
  local i
  local comparison_label

  if [ -z "$MANIFEST_PATH" ]; then
    return 0
  fi

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
  REPO_REQUESTED_MODES=()
  REPO_ACTUAL_MODES=()
  REPO_COMPARISON_LABELS=()

  for i in "${!original_paths[@]}"; do
    comparison_label="${original_labels[$i]}"
    if [ -z "$comparison_label" ]; then
      comparison_label=$(basename -- "${original_paths[$i]}")
    fi
    append_repo_entry "${original_paths[$i]}" "${original_labels[$i]}" "${original_versions[$i]}" "sequential" "$comparison_label"
    append_repo_entry "${original_paths[$i]}" "${original_labels[$i]}" "${original_versions[$i]}" "parallel" "$comparison_label"
  done
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
  TMP_ROOT="$RUN_ROOT/tmp"

  ENV_FILE="$RUN_ROOT/env.txt"
  COMMANDS_LOG="$RUN_ROOT/commands.log"
  BINARY_PATH="$WORKTREE_PATH/build/c/codebase-memory-mcp"
  BUILD_LOG="$RUN_ROOT/build.log"
  AGGREGATE_SUMMARY="$RUN_ROOT/aggregate-summary.md"
  RUN_INDEX_FILE="$RUN_ROOT/run-index.json"
  SEMANTIC_PARITY_JSON="$RUN_ROOT/semantic-parity.json"
  SEMANTIC_PARITY_MD="$RUN_ROOT/semantic-parity.md"
}

configure_repo_runtime_state() {
  local repo_slug="$1"

  REPO_STATE_ROOT="$STATE_ROOT/$repo_slug"
  REPO_STATE_HOME="$REPO_STATE_ROOT/home"
  REPO_STATE_CONFIG="$REPO_STATE_ROOT/config"
  REPO_STATE_CACHE="$REPO_STATE_ROOT/cache"
  REPO_STATE_CACHE_STORE="$REPO_STATE_CACHE/codebase-memory-mcp"

  mkdir -p "$REPO_STATE_HOME" "$REPO_STATE_CONFIG" "$REPO_STATE_CACHE" "$REPO_STATE_CACHE_STORE"
  ln -sfn "$REPO_STATE_CACHE" "$REPO_STATE_HOME/.cache"
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
  local preferred_name="$2"
  local short_commit="$3"
  local requested_mode="$4"
  local repo_name
  local path_hash
  local base_slug
  local commit_id

  if [ -n "$preferred_name" ]; then
    repo_name=$(sanitize_repo_name "$preferred_name")
  else
    repo_name=$(sanitize_repo_name "$(basename -- "$repo_path")")
  fi

  if [ -z "$repo_name" ]; then
    repo_name="repo"
  fi

  # Deterministic, human-readable slugs stay unique per repository by adding
  # a stable path hash prefix after the commit/ref identifier.
  if [ -n "${short_commit:-}" ] && [ "$short_commit" != "unavailable" ]; then
    commit_id="$short_commit"
  else
    commit_id="unavailable"
  fi

  path_hash=$(repo_path_hash "$repo_path")
  path_hash=${path_hash:0:12}

  base_slug="${repo_name}-${commit_id}-${path_hash}"
  if [ -n "$requested_mode" ] && [ "$requested_mode" != "auto" ]; then
    base_slug="${repo_name}-${requested_mode}-${commit_id}-${path_hash}"
  fi
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
  local repo_label="$2"
  local godot_version="$3"
  local requested_mode="$4"

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
  slug=$(derive_repo_artifact_slug "$repo_path" "$repo_label" "$short_commit" "$requested_mode")

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
  local manifest_path="${13}"
  local requested_mode="${14}"
  local actual_mode="${15}"
  local comparison_label="${16}"

  local output_file="$repo_dir/repo-meta.json"
  local worktree_path="$WORKTREE_PATH"
  local worktree_branch="$WORKTREE_BRANCH"
  local worktree_commit="$WORKTREE_COMMIT"

  python3 - "$output_file" "$repo_path" "$repo_label" "$repo_name" "$slug" "$project_id" "$proof_timestamp" "$git_ref" "$git_commit" "$git_branch" "$godot_version" "$qualifies_4x" "$manifest_path" "$worktree_path" "$worktree_branch" "$worktree_commit" "$requested_mode" "$actual_mode" "$comparison_label" <<'PY'
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
    manifest_path,
    worktree_path,
    worktree_branch,
    worktree_commit,
    requested_mode,
    actual_mode,
    comparison_label,
) = sys.argv[1:]


def load_manifest_entry(path_text, label_text):
    if not path_text or not label_text:
        return None
    try:
        with open(path_text, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None
    for repo in payload.get("repos", []):
        if isinstance(repo, dict) and repo.get("label") == label_text:
            return repo
    return None


manifest_entry = load_manifest_entry(manifest_path, repo_label)
manifest_mode = bool(manifest_path)
canonical_identity = {
    "remote": None,
    "pinned_commit": git_commit,
    "project_subpath": None,
    "godot_version": None if godot_version == "" else godot_version,
}
approval_status = "non-canonical-debug-only"
qualification_status = "non-qualifying"

if manifest_entry is not None:
    canonical_identity = {
        "remote": manifest_entry.get("remote"),
        "pinned_commit": manifest_entry.get("pinned_commit") or git_commit,
        "project_subpath": manifest_entry.get("project_subpath"),
        "godot_version": manifest_entry.get("godot_version") or (None if godot_version == "" else godot_version),
    }
    approval_status = "canonical-approval-bearing"
    qualification_status = (
        "godot-4.x-qualifying" if qualifies_4x == "true" else "non-qualifying"
    )

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
    "approval_status": approval_status,
    "qualification_status": qualification_status,
    "canonical_identity": canonical_identity,
    "label_role": "readability-only",
    "local_checkout_path_role": "run-evidence-only",
    "manifest_mode": manifest_mode,
    "requested_mode": None if requested_mode == "" else requested_mode,
    "actual_mode": None if actual_mode == "" else actual_mode,
    "comparison_label": None if comparison_label == "" else comparison_label,
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
    "max_rows": 500,
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

  command=(env)
  [ -n "$REPO_STATE_HOME" ] && command+=("HOME=$REPO_STATE_HOME")
  [ -n "$REPO_STATE_CONFIG" ] && command+=("XDG_CONFIG_HOME=$REPO_STATE_CONFIG")
  [ -n "$REPO_STATE_CACHE" ] && command+=("XDG_CACHE_HOME=$REPO_STATE_CACHE")
  if [ "$tool_name" = "index_repository" ] && [ -n "$CBM_ACTIVE_FORCE_PIPELINE_MODE" ]; then
    command+=("CBM_FORCE_PIPELINE_MODE=$CBM_ACTIVE_FORCE_PIPELINE_MODE")
  fi
  command+=("$BINARY_PATH" cli --raw "$tool_name")
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
    command=(env)
    [ -n "$REPO_STATE_HOME" ] && command+=("HOME=$REPO_STATE_HOME")
    [ -n "$REPO_STATE_CONFIG" ] && command+=("XDG_CONFIG_HOME=$REPO_STATE_CONFIG")
    [ -n "$REPO_STATE_CACHE" ] && command+=("XDG_CACHE_HOME=$REPO_STATE_CACHE")
    command+=("$BINARY_PATH" cli "$tool_name")
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
  local requested_mode="$9"
  local actual_mode="${10}"
  local comparison_label="${11}"

  python3 - "$repo_meta_file" "$project_id" "$index_status" "$project_resolution_status" "$overall_status" "$status_message" "$cli_capture_mode" "$cli_capture_note" "$requested_mode" "$actual_mode" "$comparison_label" <<'PY'
import json
import sys

repo_meta_file, project_id, index_status, resolution_status, overall_status, status_message, cli_capture_mode, cli_capture_note, requested_mode, actual_mode, comparison_label = sys.argv[1:12]

with open(repo_meta_file, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

payload["project_id"] = None if project_id in ("", "null") else project_id
payload["requested_mode"] = None if requested_mode == "" else requested_mode
payload["actual_mode"] = None if actual_mode == "" else actual_mode
payload["comparison_label"] = None if comparison_label == "" else comparison_label
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
  local actual_mode="$9"
  local slug="${REPO_SLUGS[$repo_index]}"
  local repo_meta_file="$RUN_ROOT/$slug/repo-meta.json"
  local requested_mode="${REPO_REQUESTED_MODES[$repo_index]:-auto}"
  local comparison_label="${REPO_COMPARISON_LABELS[$repo_index]}"

  REPO_PROJECT_IDS[$repo_index]="$project_id"
  REPO_INDEX_STATUSES[$repo_index]="$index_status"
  REPO_PROJECT_RESOLUTION_STATUSES[$repo_index]="$resolution_status"
  REPO_OVERALL_STATUSES[$repo_index]="$overall_status"
  REPO_STATUS_MESSAGES[$repo_index]="$status_message"
  REPO_CLI_CAPTURE_MODES[$repo_index]="$cli_capture_mode"
  REPO_CLI_CAPTURE_NOTES[$repo_index]="$cli_capture_note"
  REPO_ACTUAL_MODES[$repo_index]="$actual_mode"

  update_repo_meta_json_task5 "$repo_meta_file" "$project_id" "$index_status" "$resolution_status" "$overall_status" "$status_message" "$cli_capture_mode" "$cli_capture_note" "$requested_mode" "$actual_mode" "$comparison_label"
}

write_run_index_json() {
  python3 - "$RUN_ROOT" "$RUN_INDEX_FILE" "$RUN_TIMESTAMP" "$ENV_FILE" "$COMMANDS_LOG" "$BUILD_LOG" "$AGGREGATE_SUMMARY" "$SEMANTIC_PARITY_JSON" "$SEMANTIC_PARITY_MD" "$BINARY_PATH" "$BUILD_STATUS" "$BUILD_MESSAGE" "$WORKTREE_PATH" "$WORKTREE_BRANCH" "$WORKTREE_COMMIT" "$MANIFEST_PATH" "${GDSCRIPT_QUERY_NAMES[@]}" <<'PY'
import json
import sys
from pathlib import Path

(
    run_root,
    output_file,
    run_timestamp,
    env_file,
    commands_log,
    build_log,
    aggregate_summary,
    semantic_parity_json,
    semantic_parity_md,
    binary_path,
    build_status,
    build_message,
    worktree_path,
    worktree_branch,
    worktree_commit,
    manifest_path,
    *query_names,
) = sys.argv[1:]

run_root_path = Path(run_root)


def rel_text(path: Path) -> str:
    return str(path.relative_to(run_root_path))


def maybe_rel(path: Path):
    return rel_text(path) if path.exists() else None


def extract_failed_query(message):
    if not isinstance(message, str):
        return None
    prefix = "query '"
    marker = "' failed:"
    if not message.startswith(prefix) or marker not in message:
        return None
    return message[len(prefix):message.index(marker)]


repos = {}
semantic_pairs = {}
for repo_meta_path in sorted(run_root_path.glob("*/repo-meta.json")):
    repo_dir = repo_meta_path.parent
    payload = json.loads(repo_meta_path.read_text(encoding="utf-8"))
    slug = payload.get("artifact_slug") or repo_dir.name
    task5 = payload.get("task5") or {}
    indexing = task5.get("indexing") or {}
    resolution = task5.get("project_resolution") or {}
    cli_capture = task5.get("cli_capture") or {}
    failed_query = extract_failed_query(task5.get("message"))
    requested_mode = payload.get("requested_mode")
    actual_mode = payload.get("actual_mode")
    comparison_label = payload.get("comparison_label") or payload.get("repo_label") or slug

    queries = {}
    present = []
    missing = []
    failure_triggered = False
    for query_name in query_names:
        query_path = repo_dir / "queries" / f"{query_name}.json"
        if query_path.exists():
            queries[query_name] = {
                "path": rel_text(query_path),
                "status": "present",
            }
            present.append(query_name)
            continue

        if failed_query == query_name:
            queries[query_name] = {
                "path": None,
                "status": "failed",
            }
            missing.append(query_name)
            failure_triggered = True
            continue

        if failure_triggered:
            queries[query_name] = {
                "path": None,
                "status": "not_run",
            }
        else:
            queries[query_name] = {
                "path": None,
                "status": "missing",
            }
            missing.append(query_name)

    repos[slug] = {
        "repo_path": payload.get("repo_path"),
        "repo_label": payload.get("repo_label"),
        "comparison_label": comparison_label,
        "requested_mode": requested_mode,
        "actual_mode": actual_mode,
        "project_id": payload.get("project_id"),
        "status": task5.get("status", "pending"),
        "stages": {
            "indexing": indexing.get("status", "pending"),
            "project_resolution": resolution.get("status", "pending"),
        },
        "failure_context": {
            "message": task5.get("message"),
            "failed_query": failed_query,
            "cli_capture_mode": cli_capture.get("mode", "unknown"),
            "cli_capture_note": cli_capture.get("note"),
        },
        "task5": task5,
        "artifacts": {
            "repo_meta": rel_text(repo_meta_path),
            "index": maybe_rel(repo_dir / "index.json"),
            "list_projects": maybe_rel(repo_dir / "list-projects.json"),
            "summary": maybe_rel(repo_dir / "summary.md"),
            "queries": queries,
        },
        "query_wrappers_present": present,
        "query_wrappers_missing": missing,
        "runtime": {
            "state_root": maybe_rel(run_root_path / "state" / slug),
            "home": maybe_rel(run_root_path / "state" / slug / "home"),
            "xdg_config_home": maybe_rel(run_root_path / "state" / slug / "config"),
            "xdg_cache_home": maybe_rel(run_root_path / "state" / slug / "cache"),
            "store_root": maybe_rel(run_root_path / "state" / slug / "cache" / "codebase-memory-mcp"),
        },
    }

    if comparison_label and requested_mode:
        pair_entry = semantic_pairs.setdefault(comparison_label, {"comparison_label": comparison_label, "requested_modes": {}})
        pair_entry["requested_modes"][requested_mode] = {
            "artifact_slug": slug,
            "repo_label": payload.get("repo_label"),
            "requested_mode": requested_mode,
            "actual_mode": actual_mode,
            "status": task5.get("status", "pending"),
            "repo_meta": rel_text(repo_meta_path),
            "summary": maybe_rel(repo_dir / "summary.md"),
            "queries_dir": maybe_rel(repo_dir / "queries"),
        }

payload = {
    "schema_version": 1,
    "run": {
        "proof_run_utc": run_timestamp,
        "run_root": str(run_root_path),
        "env_file": maybe_rel(Path(env_file)),
        "commands_log": maybe_rel(Path(commands_log)),
        "build_log": maybe_rel(Path(build_log)),
        "aggregate_summary": maybe_rel(Path(aggregate_summary)),
        "semantic_parity_json": maybe_rel(Path(semantic_parity_json)),
        "semantic_parity_md": maybe_rel(Path(semantic_parity_md)),
        "state_root": maybe_rel(run_root_path / "state"),
        "manifest_path": manifest_path or None,
        "binary": {
            "path": binary_path,
            "status": build_status,
            "message": build_message or None,
        },
        "worktree": {
            "path": worktree_path,
            "branch": worktree_branch,
            "commit": worktree_commit,
        },
    },
    "query_suite": list(query_names),
    "semantic_pairs": semantic_pairs,
    "repos": repos,
}

Path(output_file).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
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
    local requested_mode="${REPO_REQUESTED_MODES[$i]:-auto}"
    repo_dir="$RUN_ROOT/$slug"
    index_log="$repo_dir/index.log"
    index_json="$repo_dir/index.json"
    list_projects_json="$repo_dir/list-projects.json"
    queries_dir="$repo_dir/queries"

    : > "$index_log"
    configure_repo_runtime_state "$slug"
    {
      printf '%s\n' "repo_state_root=$REPO_STATE_ROOT"
      printf '%s\n' "repo_home=$REPO_STATE_HOME"
      printf '%s\n' "repo_xdg_config_home=$REPO_STATE_CONFIG"
      printf '%s\n' "repo_xdg_cache_home=$REPO_STATE_CACHE"
      printf '%s\n' "repo_store_root=$REPO_STATE_CACHE_STORE"
      printf '%s\n' "requested_mode=$requested_mode"
    } > "$index_log"
    cli_capture_mode="raw"
    cli_capture_note=""
    fallback_tools=()

    if [ "$BUILD_STATUS" != "succeeded" ]; then
      set_repo_task5_status "$i" "" "not_attempted" "not_attempted" "incomplete" "build failed before indexing: $BUILD_MESSAGE" "not_attempted" "" "not-run"
      continue
    fi

    if [ ! -d "$repo_path" ]; then
      if [ -e "$repo_path" ]; then
        failure_message="repository path is not a directory: $repo_path"
      else
        failure_message="repository path does not exist: $repo_path"
      fi
      set_repo_task5_status "$i" "" "failed" "skipped" "incomplete" "$failure_message" "not_attempted" "" "not-run"
      continue
    fi

    CBM_ACTIVE_FORCE_PIPELINE_MODE="$requested_mode"
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
      set_repo_task5_status "$i" "" "failed" "skipped" "incomplete" "$failure_message" "$cli_capture_mode" "$cli_capture_note" "unknown"
      CBM_ACTIVE_FORCE_PIPELINE_MODE=""
      continue
    fi
    CBM_ACTIVE_FORCE_PIPELINE_MODE=""

    if [ "$CBM_LAST_CAPTURE_MODE" = "wrapped_fallback" ]; then
      cli_capture_mode="wrapped_fallback"
      fallback_tools+=("index_repository")
    fi

    if ! failure_message=$(validate_index_output "$index_json"); then
      cli_capture_note=""
      if [ "${#fallback_tools[@]}" -gt 0 ]; then
        cli_capture_note="wrapped MCP payload fallback used because binary lacks cli --raw support: ${fallback_tools[*]}"
      fi
      set_repo_task5_status "$i" "" "failed" "skipped" "incomplete" "$failure_message" "$cli_capture_mode" "$cli_capture_note" "unknown"
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
      set_repo_task5_status "$i" "" "succeeded" "failed" "incomplete" "$failure_message" "$cli_capture_mode" "$cli_capture_note" "$requested_mode"
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
      set_repo_task5_status "$i" "" "succeeded" "failed" "incomplete" "unable to resolve project_id from list_projects for $repo_path" "$cli_capture_mode" "$cli_capture_note" "$requested_mode"
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
      set_repo_task5_status "$i" "$project_id" "succeeded" "resolved" "incomplete" "$failure_message" "$cli_capture_mode" "$cli_capture_note" "$requested_mode"
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

    set_repo_task5_status "$i" "$project_id" "succeeded" "resolved" "complete" "" "$cli_capture_mode" "$cli_capture_note" "$requested_mode"
  done
}

write_repo_and_aggregate_summaries() {
  local summary_fields_file

  summary_fields_file=$(make_run_temp summary-fields)
  if ! python3 - "$RUN_ROOT" "$AGGREGATE_SUMMARY" "$BINARY_PATH" "$BUILD_STATUS" "$BUILD_MESSAGE" "$WORKTREE_PATH" "$WORKTREE_BRANCH" "$WORKTREE_COMMIT" "$MANIFEST_PATH" "${REPO_SLUGS[@]}" > "$summary_fields_file" <<'PY'
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
    manifest_path,
    *slugs,
) = sys.argv[1:]

run_root_path = Path(run_root)
aggregate_summary_file = Path(aggregate_summary_path)
manifest_mode = bool(manifest_path)

VALID_CLASSIFICATIONS = {"gating", "informational"}
VALID_EXPECTED_KEYS = {"count", "contains", "contains_edges"}

QUERY_FILE_TO_NAME = {
    "gd-files.json": "gd-files",
    "gd-classes.json": "gd-classes",
    "gd-methods.json": "gd-methods",
    "gd-class-sample.json": "gd-class-sample",
    "gd-method-sample.json": "gd-method-sample",
    "signal-calls.json": "signal-calls",
    "gd-inherits.json": "gd-inherits",
    "gd-imports.json": "gd-imports",
    "signal-call-edges.json": "signal-call-edges",
    "gd-inherits-edges.json": "gd-inherits-edges",
    "gd-import-edges.json": "gd-import-edges",
    "gd-same-script-calls.json": "gd-same-script-calls",
}

COUNT_QUERY_KEYS = {
    "gd-files": "gd_files",
    "gd-classes": "gd_classes",
    "gd-methods": "gd_methods",
    "signal-calls": "signal_calls",
    "gd-inherits": "gd_inherits_edges",
    "gd-imports": "gd_deps",
}

EDGE_DETAIL_QUERY = {
    "signal-calls": "signal-call-edges",
    "gd-imports": "gd-import-edges",
    "gd-inherits": "gd-inherits-edges",
    "gd-same-script-calls": "gd-same-script-calls",
}


def escape_cell(text):
    return str(text).replace("\r", " ").replace("\n", " <br> ").replace("|", "\\|")


def markdown_code(value, empty="unknown"):
    text = empty if value in (None, "") else str(value)
    return "`" + text.replace("`", "\\`") + "`"


def json_compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def load_wrapper(path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("wrapper payload is not an object")
    query_name = payload.get("query_name")
    if not isinstance(query_name, str) or not query_name:
        raise ValueError("wrapper query_name missing")
    return query_name, payload


def load_query_wrappers(repo_dir):
    queries_dir = repo_dir / "queries"
    wrappers = {}
    issues = []
    if not queries_dir.exists():
        return wrappers, ["missing queries directory"]

    for filename, expected_name in QUERY_FILE_TO_NAME.items():
        query_file = queries_dir / filename
        if not query_file.exists():
            continue
        try:
            query_name, payload = load_wrapper(query_file)
        except Exception as exc:  # noqa: BLE001
            issues.append(f"unparseable {filename}: {exc}")
            continue
        if query_name != expected_name:
            issues.append(f"invalid {filename}: expected query_name {expected_name!r}, got {query_name!r}")
            continue
        wrappers[query_name] = payload
    return wrappers, issues


def extract_rows(wrapper):
    result = wrapper.get("result")
    if not isinstance(result, dict):
        raise ValueError("wrapper result is not an object")
    rows = result.get("rows")
    if rows in (None, []):
        return []
    if not isinstance(rows, list):
        raise ValueError("wrapper rows missing")
    return rows


def extract_count_from_wrapper(wrapper):
    rows = extract_rows(wrapper)
    if not rows:
        return 0
    first_row = rows[0]
    if not isinstance(first_row, list) or not first_row:
        raise ValueError("wrapper first row missing")
    value = first_row[0]
    if isinstance(value, str):
        value = value.strip()
    if value in (None, ""):
        return 0
    return int(value)


def count_value(query_name, wrappers):
    if query_name == "gd-same-script-calls":
        return len(edge_values(query_name, wrappers))
    wrapper = wrappers.get(query_name)
    if wrapper is None:
        raise ValueError(f"missing wrapper for {query_name}")
    return extract_count_from_wrapper(wrapper)


def sample_values(query_name, wrappers):
    wrapper = wrappers.get(query_name)
    if wrapper is None:
        raise ValueError(f"missing wrapper for {query_name}")
    values = []
    for row in extract_rows(wrapper):
        if not isinstance(row, list) or len(row) < 2:
            raise ValueError(f"invalid row for {query_name}: {row!r}")
        values.append(f"{row[1]}:{row[0]}")
    return sorted(set(values))


def edge_values(query_name, wrappers):
    detail_query = EDGE_DETAIL_QUERY.get(query_name, query_name)
    wrapper = wrappers.get(detail_query)
    if wrapper is None:
        raise ValueError(f"missing wrapper for {detail_query}")

    values = []
    for row in extract_rows(wrapper):
        if not isinstance(row, list) or len(row) < 6:
            raise ValueError(f"invalid row for {detail_query}: {row!r}")
        src_file, src_name, _src_qn, dst_file, dst_name, _dst_qn = row[:6]
        if query_name == "gd-imports":
            values.append(f"{src_file}->{dst_file}")
        elif query_name == "gd-same-script-calls":
            if src_file != dst_file:
                continue
            if _dst_qn and ".signal." in str(_dst_qn):
                continue
            values.append(f"{src_file}:{src_name}->{dst_name}")
        else:
            values.append(f"{src_file}:{src_name}->{dst_file}:{dst_name}")
    return sorted(set(values))


def compare_assertion(assertion, wrappers):
    assertion_id = assertion.get("id", "<unknown>")
    classification = assertion.get("classification")
    query_name = assertion.get("query")
    expected = assertion.get("expected")

    validation_notes = []
    if classification not in VALID_CLASSIFICATIONS:
        validation_notes.append(f"invalid classification: {classification!r}")
    if not isinstance(query_name, str) or not query_name:
        validation_notes.append("missing query name")
    elif query_name not in QUERY_FILE_TO_NAME.values():
        validation_notes.append(f"unknown query: {query_name}")
    if not isinstance(expected, dict) or not expected:
        validation_notes.append("expected must be a non-empty object")
        expected = expected if isinstance(expected, dict) else {}
    unsupported_expected = sorted(set(expected.keys()) - VALID_EXPECTED_KEYS)
    if unsupported_expected:
        validation_notes.append("unsupported expected keys: " + ", ".join(unsupported_expected))

    if validation_notes:
        return {
            "id": assertion_id,
            "query": query_name or "",
            "classification": classification if classification in VALID_CLASSIFICATIONS else "gating",
            "outcome": "incomplete",
            "expected": expected,
            "actual": {},
            "notes": validation_notes,
        }

    actual = {}
    notes = []
    outcome = "pass"

    if "count" in expected:
        try:
            actual["count"] = count_value(query_name, wrappers)
        except Exception as exc:  # noqa: BLE001
            return {
                "id": assertion_id,
                "query": query_name,
                "classification": classification,
                "outcome": "incomplete",
                "expected": expected,
                "actual": {"error": str(exc)},
                "notes": [f"count unavailable: {exc}"],
            }
        if actual["count"] != expected["count"]:
            outcome = "fail"
            notes.append(f"expected count {expected['count']}, got {actual['count']}")

    if "contains_edges" in expected:
        try:
            actual["contains_edges"] = edge_values(query_name, wrappers)
        except Exception as exc:  # noqa: BLE001
            return {
                "id": assertion_id,
                "query": query_name,
                "classification": classification,
                "outcome": "incomplete",
                "expected": expected,
                "actual": {"error": str(exc)},
                "notes": [f"edge comparison unavailable: {exc}"],
            }
        missing_edges = [edge for edge in expected["contains_edges"] if edge not in set(actual["contains_edges"])]
        if missing_edges:
            outcome = "fail"
            notes.append("missing expected edges: " + ", ".join(missing_edges))

    if "contains" in expected:
        try:
            actual["contains"] = sample_values(query_name, wrappers)
        except Exception as exc:  # noqa: BLE001
            return {
                "id": assertion_id,
                "query": query_name,
                "classification": classification,
                "outcome": "incomplete",
                "expected": expected,
                "actual": {"error": str(exc)},
                "notes": [f"sample comparison unavailable: {exc}"],
            }
        missing_items = [item for item in expected["contains"] if item not in set(actual["contains"])]
        if missing_items:
            outcome = "fail"
            notes.append("missing expected sample values: " + ", ".join(missing_items))

    return {
        "id": assertion_id,
        "query": query_name,
        "classification": classification,
        "outcome": outcome,
        "expected": expected,
        "actual": actual,
        "notes": notes,
    }


def load_manifest(path_text):
    if not path_text:
        return None, None
    manifest_file = Path(path_text)
    if not manifest_file.exists():
        return None, f"manifest file not found: {manifest_file}"
    try:
        payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return None, f"unable to read manifest: {exc}"
    if not isinstance(payload, dict):
        return None, "manifest root must be an object"
    version = payload.get("version")
    if version != 1:
        return None, "manifest version must be 1"
    language = payload.get("language")
    if language != "gdscript":
        return None, "manifest language must be 'gdscript'"
    promotion_target = payload.get("promotion_target")
    if promotion_target != "good":
        return None, "manifest promotion_target must be 'good'"
    minimum_repo_count = payload.get("minimum_repo_count")
    if not isinstance(minimum_repo_count, int) or minimum_repo_count < 1:
        return None, "manifest minimum_repo_count must be a positive integer"
    repos = payload.get("repos")
    if not isinstance(repos, list):
        return None, "manifest repos must be a list"

    def validate_string_list(values, field_name, repo_label):
        if not isinstance(values, list) or not values:
            return f"manifest repo {repo_label!r} field {field_name} must be a non-empty list"
        for item in values:
            if not isinstance(item, str) or not item:
                return f"manifest repo {repo_label!r} field {field_name} must contain non-empty strings"
        return None

    repo_by_label = {}
    for repo in repos:
        if not isinstance(repo, dict):
            return None, "manifest repo entries must be objects"
        label = repo.get("label")
        if not isinstance(label, str) or not label:
            return None, "manifest repo missing label"
        if label in repo_by_label:
            return None, f"duplicate manifest label: {label}"

        for field_name in ("remote", "pinned_commit", "godot_version"):
            value = repo.get(field_name)
            if not isinstance(value, str) or not value:
                return None, f"manifest repo {label!r} missing {field_name}"

        project_subpath = repo.get("project_subpath")
        if project_subpath is not None and (not isinstance(project_subpath, str) or not project_subpath):
            return None, f"manifest repo {label!r} has invalid project_subpath"

        required_for_error = validate_string_list(repo.get("required_for"), "required_for", label)
        if required_for_error:
            return None, required_for_error

        assertions = repo.get("assertions")
        if not isinstance(assertions, list) or not assertions:
            return None, f"manifest repo {label!r} must declare at least one assertion"
        seen_assertion_ids = set()
        for assertion in assertions:
            if not isinstance(assertion, dict):
                return None, f"manifest repo {label!r} assertions must be objects"
            assertion_id = assertion.get("id")
            if not isinstance(assertion_id, str) or not assertion_id:
                return None, f"manifest repo {label!r} has assertion without id"
            if assertion_id in seen_assertion_ids:
                return None, f"manifest repo {label!r} has duplicate assertion id {assertion_id!r}"
            seen_assertion_ids.add(assertion_id)

            query_name = assertion.get("query")
            if not isinstance(query_name, str) or query_name not in QUERY_FILE_TO_NAME.values():
                return None, f"manifest repo {label!r} assertion {assertion_id!r} has invalid query"

            classification = assertion.get("classification")
            if classification not in VALID_CLASSIFICATIONS:
                return None, f"manifest repo {label!r} assertion {assertion_id!r} has invalid classification"

            expected = assertion.get("expected")
            if not isinstance(expected, dict) or not expected:
                return None, f"manifest repo {label!r} assertion {assertion_id!r} must have a non-empty expected object"

            unsupported_expected = sorted(set(expected.keys()) - VALID_EXPECTED_KEYS)
            if unsupported_expected:
                return None, f"manifest repo {label!r} assertion {assertion_id!r} has unsupported expected keys: {', '.join(unsupported_expected)}"

            if "count" in expected and (not isinstance(expected["count"], int) or expected["count"] < 0):
                return None, f"manifest repo {label!r} assertion {assertion_id!r} count must be a non-negative integer"

            for key_name in ("contains", "contains_edges"):
                if key_name in expected:
                    value = expected[key_name]
                    if not isinstance(value, list) or not value:
                        return None, f"manifest repo {label!r} assertion {assertion_id!r} field {key_name} must be a non-empty list"
                    for item in value:
                        if not isinstance(item, str) or not item:
                            return None, f"manifest repo {label!r} assertion {assertion_id!r} field {key_name} must contain non-empty strings"

        repo_by_label[label] = repo
    payload["repo_by_label"] = repo_by_label
    return payload, None


def load_repo_meta(repo_dir):
    return json.loads((repo_dir / "repo-meta.json").read_text(encoding="utf-8"))


def repo_task_state(meta):
    task_state = meta.get("task5") or {}
    indexing_state = (task_state.get("indexing") or {}).get("status", "pending")
    project_resolution_state = (task_state.get("project_resolution") or {}).get("status", "pending")
    overall_status = task_state.get("status", "pending")
    overall_message = task_state.get("message") or ""
    cli_capture = task_state.get("cli_capture") or {}
    cli_mode = cli_capture.get("mode", "unknown")
    cli_note = cli_capture.get("note")
    return indexing_state, project_resolution_state, overall_status, overall_message, cli_mode, cli_note


def summarize_repo_without_manifest(slug):
    repo_dir = run_root_path / slug
    meta = load_repo_meta(repo_dir)
    indexing_state, project_resolution_state, overall_status, overall_message, cli_mode, cli_note = repo_task_state(meta)
    wrappers, query_issues = load_query_wrappers(repo_dir)
    project_id = meta.get("project_id")
    godot_version = meta.get("godot_version")
    qualifies_godot4x = bool(meta.get("qualifies_godot4x"))

    def safe_count(name):
        try:
            return count_value(name, wrappers)
        except Exception:
            return None

    gd_files = safe_count("gd-files")
    gd_classes = safe_count("gd-classes")
    gd_methods = safe_count("gd-methods")
    signal_calls = safe_count("signal-calls")
    gd_inherits_edges = safe_count("gd-inherits")
    gd_deps = safe_count("gd-imports")

    repo_complete = (
        indexing_state == "succeeded"
        and project_resolution_state == "resolved"
        and overall_status == "complete"
        and project_id not in (None, "")
        and not query_issues
    )
    indexing_coverage = bool(repo_complete and qualifies_godot4x and (gd_files or 0) > 0 and (gd_classes or 0) > 0 and (gd_methods or 0) > 0)
    signal_coverage = bool(repo_complete and (signal_calls or 0) > 0)
    imports_coverage = bool(repo_complete and (gd_deps or 0) > 0)
    inherits_coverage = bool(repo_complete and (gd_inherits_edges or 0) > 0)
    notes = [overall_message or "repo complete"]
    notes.extend(query_issues)

    summary_lines = [
        "# GDScript Proof Summary",
        "",
        f"- Repo path: {markdown_code(meta.get('repo_path'))}",
        f"- Artifact slug: {markdown_code(meta.get('artifact_slug'))}",
        f"- Resolved project ID: {markdown_code(project_id)}",
        f"- Git commit: {markdown_code(meta.get('git_commit'))}",
        f"- Godot version: {markdown_code(godot_version)}",
        f"- Approval status: {markdown_code(meta.get('approval_status'))}",
        f"- Qualification status: {markdown_code(meta.get('qualification_status'))}",
        f"- Canonical identity tuple: {escape_cell(json_compact(meta.get('canonical_identity') or {}))}",
        f"- Repo complete: {markdown_code(str(repo_complete).lower())}",
        f"- Overall status: {markdown_code(overall_status)}",
        f"- CLI capture: {cli_mode + (f' — {cli_note}' if cli_note else '')}",
        "",
        "## Coverage contributions",
        f"- Indexing coverage: {markdown_code('yes' if indexing_coverage else 'no')}",
        f"- Signal coverage: {markdown_code('yes' if signal_coverage else 'no')}",
        f"- Imports coverage: {markdown_code('yes' if imports_coverage else 'no')}",
        f"- Inherits coverage: {markdown_code('yes' if inherits_coverage else 'no')}",
    ]
    (repo_dir / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {
        "repo_path": meta.get("repo_path", ""),
        "artifact_slug": meta.get("artifact_slug", slug),
        "summary_relpath": f"{slug}/summary.md",
        "indexing_coverage": indexing_coverage,
        "signal_coverage": signal_coverage,
        "imports_coverage": imports_coverage,
        "inherits_coverage": inherits_coverage,
    }


manifest, manifest_error = load_manifest(manifest_path)


def detect_git_root_and_subpath(repo_path_text):
    repo_path = Path(repo_path_text)
    for candidate in (repo_path, *repo_path.parents):
        git_marker = candidate / ".git"
        if git_marker.exists():
            try:
                relative = repo_path.relative_to(candidate)
            except ValueError as exc:  # noqa: BLE001
                raise ValueError(f"unable to compute repo subpath: {exc}") from exc
            rel_text = relative.as_posix()
            return candidate, "." if rel_text == "" else rel_text
    raise ValueError("unable to detect git root for repo path")


def summarize_repo_with_manifest(slug):
    repo_dir = run_root_path / slug
    meta = load_repo_meta(repo_dir)
    indexing_state, project_resolution_state, overall_status, overall_message, cli_mode, cli_note = repo_task_state(meta)
    wrappers, query_issues = load_query_wrappers(repo_dir)
    label = meta.get("repo_label") or ""
    project_id = meta.get("project_id")
    repo_complete = (
        indexing_state == "succeeded"
        and project_resolution_state == "resolved"
        and overall_status == "complete"
        and project_id not in (None, "")
        and not query_issues
    )

    issues = []
    manifest_repo = None
    if manifest_error:
        issues.append(manifest_error)
    elif not label:
        issues.append("missing repo label in manifest mode")
    else:
        manifest_repo = manifest["repo_by_label"].get(label)
        if manifest_repo is None:
            issues.append(f"label not present in manifest: {label}")

    if not repo_complete:
        issues.append(overall_message or "repo indexing/query stage incomplete")
    issues.extend(query_issues)

    pinned_match = None
    actual_project_subpath = None
    if manifest_repo is not None:
        pinned_commit = manifest_repo.get("pinned_commit")
        actual_commit = meta.get("git_commit")
        pinned_match = bool(pinned_commit and actual_commit and pinned_commit == actual_commit)
        if not pinned_match:
            issues.append(f"pinned commit mismatch: expected {pinned_commit}, got {actual_commit or '<unknown>'}")
        expected_project_subpath = manifest_repo.get("project_subpath")
        if expected_project_subpath:
            try:
                _git_root, actual_project_subpath = detect_git_root_and_subpath(meta.get("repo_path") or "")
            except Exception as exc:  # noqa: BLE001
                issues.append(f"unable to determine project_subpath: {exc}")
            else:
                if actual_project_subpath != expected_project_subpath:
                    issues.append(
                        f"project_subpath mismatch: expected {expected_project_subpath}, got {actual_project_subpath}"
                    )

    results = []
    if manifest_repo is not None and not issues:
        for assertion in manifest_repo.get("assertions", []):
            results.append(compare_assertion(assertion, wrappers))

    gating = [item for item in results if item["classification"] == "gating"]
    informational = [item for item in results if item["classification"] != "gating"]
    if issues or any(item["outcome"] == "incomplete" for item in gating):
        outcome = "incomplete"
    elif any(item["outcome"] == "fail" for item in gating):
        outcome = "fail"
    else:
        outcome = "pass"

    def section_lines(title, items):
        lines = ["", f"## {title}"]
        if not items:
            lines.append("- none")
            return lines
        lines.extend([
            "| Assertion ID | Query | Outcome | Expected | Actual | Notes |",
            "| --- | --- | --- | --- | --- | --- |",
        ])
        for item in items:
            lines.append(
                "| {assertion_id} | {query} | {outcome} | {expected} | {actual} | {notes} |".format(
                    assertion_id=escape_cell(item["id"]),
                    query=escape_cell(item["query"]),
                    outcome=markdown_code(item["outcome"]),
                    expected=escape_cell(json_compact(item["expected"])),
                    actual=escape_cell(json_compact(item["actual"])),
                    notes=escape_cell("; ".join(item["notes"]) if item["notes"] else ""),
                )
            )
        return lines

    summary_lines = [
        "# GDScript Proof Summary",
        "",
        f"- Repo path: {markdown_code(meta.get('repo_path'))}",
        f"- Artifact slug: {markdown_code(meta.get('artifact_slug'))}",
        f"- Manifest label: {markdown_code(label)}",
        f"- Resolved project ID: {markdown_code(project_id)}",
        f"- Git commit: {markdown_code(meta.get('git_commit'))}",
        f"- Manifest pinned commit: {markdown_code((manifest_repo or {}).get('pinned_commit'))}",
        f"- Manifest project_subpath: {markdown_code((manifest_repo or {}).get('project_subpath'))}",
        f"- Detected project_subpath: {markdown_code(actual_project_subpath)}",
        f"- Pinned commit match: {markdown_code('yes' if pinned_match else 'no' if pinned_match is not None else 'unknown')}",
        f"- Godot version: {markdown_code((manifest_repo or {}).get('godot_version') or meta.get('godot_version'))}",
        f"- Approval status: {markdown_code(meta.get('approval_status'))}",
        f"- Qualification status: {markdown_code(meta.get('qualification_status'))}",
        f"- Canonical identity tuple: {escape_cell(json_compact(meta.get('canonical_identity') or {}))}",
        f"- Repo complete: {markdown_code(str(repo_complete).lower())}",
        f"- Outcome: {markdown_code(outcome)}",
        f"- CLI capture: {cli_mode + (f' — {cli_note}' if cli_note else '')}",
        f"- Required for: {escape_cell(', '.join((manifest_repo or {}).get('required_for') or []))}",
    ]
    if issues:
        summary_lines.extend(["", "## Comparability issues"])
        for issue in issues:
            summary_lines.append(f"- {issue}")
    summary_lines.extend(section_lines("Gating assertions", gating))
    summary_lines.extend(section_lines("Informational assertions", informational))
    (repo_dir / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {
        "repo_label": label,
        "artifact_slug": meta.get("artifact_slug", slug),
        "git_commit": meta.get("git_commit", ""),
        "manifest_pinned_commit": (manifest_repo or {}).get("pinned_commit", ""),
        "required_for": ", ".join((manifest_repo or {}).get("required_for") or []),
        "outcome": outcome,
        "issues": issues,
        "gating": gating,
        "informational": informational,
    }


if manifest_mode:
    repo_summaries = [summarize_repo_with_manifest(slug) for slug in slugs]
    run_labels = sorted({repo["repo_label"] for repo in repo_summaries if repo["repo_label"]})
    missing_manifest_labels = []
    minimum_repo_count = None
    if manifest is not None:
        minimum_repo_count = manifest.get("minimum_repo_count")
        missing_manifest_labels = sorted(label for label in manifest["repo_by_label"] if label not in set(run_labels))

    gating_pass = sum(1 for repo in repo_summaries for item in repo["gating"] if item["outcome"] == "pass")
    gating_fail = sum(1 for repo in repo_summaries for item in repo["gating"] if item["outcome"] == "fail")
    gating_incomplete = sum(1 for repo in repo_summaries for item in repo["gating"] if item["outcome"] == "incomplete")
    gating_total = gating_pass + gating_fail + gating_incomplete
    informational_total = sum(len(repo["informational"]) for repo in repo_summaries)
    informational_fail = sum(1 for repo in repo_summaries for item in repo["informational"] if item["outcome"] == "fail")
    aggregate_issues = []
    if manifest_error:
        aggregate_issues.append(manifest_error)
    if missing_manifest_labels:
        aggregate_issues.append("missing manifest repos: " + ", ".join(missing_manifest_labels))
    unlabeled = [repo["artifact_slug"] for repo in repo_summaries if not repo["repo_label"]]
    if unlabeled:
        aggregate_issues.append("unlabeled repos in manifest mode: " + ", ".join(unlabeled))
    if isinstance(minimum_repo_count, int) and len(run_labels) < minimum_repo_count:
        aggregate_issues.append(f"run provided {len(run_labels)} labeled repos but manifest requires at least {minimum_repo_count}")
    if any(repo["outcome"] == "incomplete" for repo in repo_summaries):
        aggregate_issues.append("one or more repos are incomplete")

    if aggregate_issues or gating_incomplete > 0:
        aggregate_outcome = "incomplete"
    elif gating_fail > 0:
        aggregate_outcome = "fail"
    else:
        aggregate_outcome = "pass"

    aggregate_pass = aggregate_outcome == "pass"
    aggregate_note = "; ".join(aggregate_issues) if aggregate_issues else ("all gating assertions passed" if aggregate_pass else "one or more gating assertions failed")

    lines = [
        "# GDScript Proof Aggregate Summary",
        "",
        f"- Run root: {markdown_code(run_root)}",
        f"- Binary: {markdown_code(binary_path)}",
        f"- Build status: {markdown_code(build_status)}",
        f"- Manifest: {markdown_code(manifest_path)}",
        f"- codebase-memory-mcp worktree under test: {markdown_code(worktree_path)}",
        f"- codebase-memory-mcp branch under test: {markdown_code(worktree_branch)}",
        f"- codebase-memory-mcp commit under test: {markdown_code(worktree_commit)}",
        f"- Final outcome: {markdown_code(aggregate_outcome)}",
        f"- aggregate_pass: {markdown_code('true' if aggregate_pass else 'false')}",
        f"- Aggregate note: {escape_cell(aggregate_note)}",
        "",
        "## Assertion totals",
        f"- gating_total: {markdown_code(gating_total)}",
        f"- gating_pass: {markdown_code(gating_pass)}",
        f"- gating_fail: {markdown_code(gating_fail)}",
        f"- gating_incomplete: {markdown_code(gating_incomplete)}",
        f"- informational_total: {markdown_code(informational_total)}",
        f"- informational_fail: {markdown_code(informational_fail)}",
        "",
        "## Repos processed",
        "| Repo label | Artifact slug | Outcome | Required for | Pinned commit | Actual commit | Notes |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for repo in repo_summaries:
        lines.append(
            "| {label} | {slug} | {outcome} | {required_for} | {pinned} | {actual} | {notes} |".format(
                label=markdown_code(repo["repo_label"], empty="missing"),
                slug=markdown_code(repo["artifact_slug"]),
                outcome=markdown_code(repo["outcome"]),
                required_for=escape_cell(repo["required_for"]),
                pinned=markdown_code(repo["manifest_pinned_commit"], empty="unknown"),
                actual=markdown_code(repo["git_commit"], empty="unknown"),
                notes=escape_cell("; ".join(repo["issues"]) if repo["issues"] else "ok"),
            )
        )
else:
    repo_summaries = [summarize_repo_without_manifest(slug) for slug in slugs]
    indexing_coverage = any(repo["indexing_coverage"] for repo in repo_summaries)
    signal_coverage = any(repo["signal_coverage"] for repo in repo_summaries)
    imports_coverage = any(repo["imports_coverage"] for repo in repo_summaries)
    inherits_coverage = any(repo["inherits_coverage"] for repo in repo_summaries)
    aggregate_pass = indexing_coverage and signal_coverage and imports_coverage and inherits_coverage
    aggregate_outcome = "pass" if aggregate_pass else "fail"
    missing_categories = []
    if not indexing_coverage:
        missing_categories.append("indexing_coverage")
    if not signal_coverage:
        missing_categories.append("signal_coverage")
    if not imports_coverage:
        missing_categories.append("imports_coverage")
    if not inherits_coverage:
        missing_categories.append("inherits_coverage")
    aggregate_note = ", ".join(missing_categories) if missing_categories else "none"
    lines = [
        "# GDScript Proof Aggregate Summary",
        "",
        f"- Run root: {markdown_code(run_root)}",
        f"- Binary: {markdown_code(binary_path)}",
        f"- Build status: {markdown_code(build_status)}",
        f"- codebase-memory-mcp worktree under test: {markdown_code(worktree_path)}",
        f"- codebase-memory-mcp branch under test: {markdown_code(worktree_branch)}",
        f"- codebase-memory-mcp commit under test: {markdown_code(worktree_commit)}",
        f"- Final outcome: {markdown_code(aggregate_outcome)}",
        f"- aggregate_pass: {markdown_code('true' if aggregate_pass else 'false')}",
        f"- Missing coverage categories: {markdown_code(aggregate_note, empty='none')}",
    ]

aggregate_summary_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

if manifest_mode:
    print("false")
    print("false")
    print("false")
    print("false")
    print("true" if aggregate_pass else "false")
    print(aggregate_outcome)
    print(",".join(missing_manifest_labels) if 'missing_manifest_labels' in locals() and missing_manifest_labels else "")
    print(aggregate_note)
else:
    print("true" if indexing_coverage else "false")
    print("true" if signal_coverage else "false")
    print("true" if imports_coverage else "false")
    print("true" if inherits_coverage else "false")
    print("true" if aggregate_pass else "false")
    print(aggregate_outcome)
    print(",".join(missing_categories))
    print(aggregate_note)
PY
  then
    rm -f "$summary_fields_file"
    return 1
  fi

  {
    IFS= read -r AGG_INDEXING_COVERAGE
    IFS= read -r AGG_SIGNAL_COVERAGE
    IFS= read -r AGG_IMPORTS_COVERAGE
    IFS= read -r AGG_INHERITS_COVERAGE
    IFS= read -r AGGREGATE_PASS
    IFS= read -r AGGREGATE_OUTCOME
    IFS= read -r AGGREGATE_MISSING_CATEGORIES
    IFS= read -r AGGREGATE_NOTE
  } < "$summary_fields_file"

  rm -f "$summary_fields_file"
}

generate_semantic_parity_reports() {
  if [ -z "$MANIFEST_PATH" ]; then
    rm -f "$SEMANTIC_PARITY_JSON" "$SEMANTIC_PARITY_MD"
    return 0
  fi

  python3 - "$RUN_ROOT" "$SEMANTIC_PARITY_JSON" "$SEMANTIC_PARITY_MD" "$MANIFEST_PATH" <<'PY'
import json
import sys
from pathlib import Path

run_root, json_path, md_path, manifest_path = sys.argv[1:5]
run_root_path = Path(run_root)

manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))

COUNT_QUERY_KEYS = {
    "gd-classes": "gd_classes",
    "gd-methods": "gd_methods",
    "signal-calls": "signal_calls",
    "gd-inherits": "gd_inherits_edges",
    "gd-imports": "gd_deps",
}

EDGE_DETAIL_QUERY = {
    "signal-calls": "signal-call-edges",
    "gd-inherits": "gd-inherits-edges",
    "gd-imports": "gd-import-edges",
    "gd-same-script-calls": "gd-same-script-calls",
}


def load_wrapper(repo_dir: Path, query_name: str):
    path = repo_dir / "queries" / f"{query_name}.json"
    if not path.exists():
        raise FileNotFoundError(f"missing wrapper: {path.relative_to(run_root_path)}")
    return json.loads(path.read_text(encoding="utf-8"))


def rows(wrapper):
    result = wrapper.get("result")
    if not isinstance(result, dict):
        raise ValueError("wrapper result is not an object")
    payload = result.get("rows") or []
    if not isinstance(payload, list):
        raise ValueError("wrapper rows missing")
    return payload


def count_value(repo_dir: Path, query_name: str) -> int:
    if query_name == "gd-same-script-calls":
        return len(edge_values(repo_dir, query_name))
    wrapper = load_wrapper(repo_dir, query_name)
    payload_rows = rows(wrapper)
    if not payload_rows:
        return 0
    first = payload_rows[0]
    if not isinstance(first, list) or not first:
        raise ValueError(f"invalid count row for {query_name}")
    return int(first[0])


def sample_values(repo_dir: Path, query_name: str):
    payload_rows = rows(load_wrapper(repo_dir, query_name))
    values = []
    for row in payload_rows:
        if not isinstance(row, list) or len(row) < 2:
            raise ValueError(f"invalid sample row for {query_name}")
        values.append(f"{row[1]}:{row[0]}")
    return sorted(set(values))


def edge_values(repo_dir: Path, query_name: str):
    detail_query = EDGE_DETAIL_QUERY.get(query_name, query_name)
    payload_rows = rows(load_wrapper(repo_dir, detail_query))
    values = []
    for row in payload_rows:
        if not isinstance(row, list) or len(row) < 6:
            raise ValueError(f"invalid edge row for {detail_query}")
        src_file, src_name, _src_qn, dst_file, dst_name, dst_qn = row[:6]
        if query_name == "gd-imports":
            values.append(f"{src_file}->{dst_file}")
        elif query_name == "gd-same-script-calls":
            if src_file != dst_file:
                continue
            if dst_qn and ".signal." in str(dst_qn):
                continue
            values.append(f"{src_file}:{src_name}->{dst_name}")
        else:
            values.append(f"{src_file}:{src_name}->{dst_file}:{dst_name}")
    return sorted(set(values))


def requirement_payload(outcome, notes, **extra):
    payload = {"outcome": outcome, "notes": notes}
    payload.update(extra)
    return payload


def compare_pair(label, sequential, parallel):
    pair = {
        "comparison_label": label,
        "description": "Additive evidence derived from canonical queries/*.json wrappers; semantic-parity.json does not replace raw wrapper artifacts.",
        "repo_artifacts": {},
        "requirements": {},
        "overall": "incomplete",
    }
    for mode_name, meta in (("sequential", sequential), ("parallel", parallel)):
        if meta is None:
            continue
        pair["repo_artifacts"][mode_name] = {
            "artifact_slug": meta["artifact_slug"],
            "requested_mode": meta.get("requested_mode"),
            "actual_mode": meta.get("actual_mode"),
            "repo_meta": f"{meta['artifact_slug']}/repo-meta.json",
            "summary": f"{meta['artifact_slug']}/summary.md",
            "queries_dir": f"{meta['artifact_slug']}/queries",
            "status": meta.get("status", "pending"),
        }

    if sequential is None or parallel is None:
        note = "both sequential and parallel repo artifacts are required"
        for requirement_id in ("SEM-01", "SEM-02", "SEM-03", "SEM-04", "SEM-05", "SEM-06"):
            pair["requirements"][requirement_id] = requirement_payload("incomplete", [note])
        return pair

    if sequential.get("status") != "complete" or parallel.get("status") != "complete":
        note = "semantic comparison requires complete sequential and parallel wrapper evidence"
        for requirement_id in ("SEM-01", "SEM-02", "SEM-03", "SEM-04", "SEM-05", "SEM-06"):
            pair["requirements"][requirement_id] = requirement_payload("incomplete", [note])
        return pair

    seq_dir = run_root_path / sequential["artifact_slug"]
    par_dir = run_root_path / parallel["artifact_slug"]

    def compare(requirement_id, count_query=None, sample_query=None, edge_query=None, require_non_zero=False):
        notes = []
        data = {}
        try:
            if count_query is not None:
                seq_count = count_value(seq_dir, count_query)
                par_count = count_value(par_dir, count_query)
                data["count"] = {"query": count_query, "sequential": seq_count, "parallel": par_count, "matched": seq_count == par_count}
                if require_non_zero and (seq_count == 0 or par_count == 0):
                    notes.append(f"{count_query} must be non-zero in both modes")
                if seq_count != par_count:
                    notes.append(f"{count_query} differs between modes")
            if sample_query is not None:
                seq_values = sample_values(seq_dir, sample_query)
                par_values = sample_values(par_dir, sample_query)
                data["representative_samples"] = {
                    "query": sample_query,
                    "sequential": seq_values[:5],
                    "parallel": par_values[:5],
                    "matched": seq_values == par_values,
                }
                if not seq_values or not par_values:
                    notes.append(f"{sample_query} must expose representative samples in both modes")
                if seq_values != par_values:
                    notes.append(f"{sample_query} representative samples differ between modes")
            if edge_query is not None:
                seq_values = edge_values(seq_dir, edge_query)
                par_values = edge_values(par_dir, edge_query)
                data["representative_edges"] = {
                    "query": edge_query,
                    "sequential": seq_values[:5],
                    "parallel": par_values[:5],
                    "matched": seq_values == par_values,
                }
                if seq_values != par_values:
                    notes.append(f"{edge_query} representative edges differ between modes")
            return requirement_payload("pass" if not notes else "fail", notes, **data)
        except Exception as exc:  # noqa: BLE001
            return requirement_payload("incomplete", [f"comparison unavailable: {exc}"])

    sem01 = requirement_payload(
        "pass",
        [],
        class_count=compare("SEM-01.class_count", count_query="gd-classes", require_non_zero=True),
        method_count=compare("SEM-01.method_count", count_query="gd-methods", require_non_zero=True),
        class_sample=compare("SEM-01.class_sample", sample_query="gd-class-sample"),
        method_sample=compare("SEM-01.method_sample", sample_query="gd-method-sample"),
    )
    sem01_notes = []
    sem01_outcomes = []
    for key in ("class_count", "method_count", "class_sample", "method_sample"):
        sem01_outcomes.append(sem01[key]["outcome"])
        sem01_notes.extend(sem01[key].get("notes") or [])
    sem01["outcome"] = "incomplete" if "incomplete" in sem01_outcomes else ("fail" if sem01_notes else "pass")
    sem01["notes"] = sem01_notes

    sem02 = compare("SEM-02", count_query="gd-same-script-calls", edge_query="gd-same-script-calls")
    sem03 = compare("SEM-03", count_query="gd-inherits", edge_query="gd-inherits")
    sem04 = compare("SEM-04", count_query="gd-imports", edge_query="gd-imports")
    sem05 = compare("SEM-05", count_query="signal-calls", edge_query="signal-calls")
    pair["requirements"] = {
        "SEM-01": sem01,
        "SEM-02": sem02,
        "SEM-03": sem03,
        "SEM-04": sem04,
        "SEM-05": sem05,
    }

    outcomes = [pair["requirements"][key]["outcome"] for key in ("SEM-01", "SEM-02", "SEM-03", "SEM-04", "SEM-05")]
    overall = "pass"
    if "incomplete" in outcomes:
        overall = "incomplete"
    elif "fail" in outcomes:
        overall = "fail"
    pair["requirements"]["SEM-06"] = requirement_payload(
        overall,
        ["Overall semantic outcome is derived from SEM-01 through SEM-05 counts plus representative samples, not aggregate manifest pass/fail alone."],
    )
    pair["overall"] = overall
    return pair


repos = {}
for repo_meta_path in sorted(run_root_path.glob("*/repo-meta.json")):
    payload = json.loads(repo_meta_path.read_text(encoding="utf-8"))
    label = payload.get("comparison_label") or payload.get("repo_label") or repo_meta_path.parent.name
    requested_mode = payload.get("requested_mode")
    status = ((payload.get("task5") or {}).get("status")) or "pending"
    if not requested_mode:
        continue
    repos.setdefault(label, {})[requested_mode] = {
        "artifact_slug": payload.get("artifact_slug") or repo_meta_path.parent.name,
        "requested_mode": requested_mode,
        "actual_mode": payload.get("actual_mode"),
        "status": status,
    }

reports = {}
manifest_labels = [repo.get("label") for repo in manifest.get("repos", []) if isinstance(repo, dict) and isinstance(repo.get("label"), str)]
for label in manifest_labels:
    pair = repos.get(label, {})
    reports[label] = compare_pair(label, pair.get("sequential"), pair.get("parallel"))

json_payload = {
    "description": "Additive evidence derived from canonical queries/*.json wrappers. semantic-parity.json does not replace raw wrapper artifacts.",
    "semantic_pairs": reports,
}
Path(json_path).write_text(json.dumps(json_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

lines = [
    "# Sequential vs parallel semantic parity",
    "",
    "This additive review surface is derived from canonical `queries/*.json` wrappers and does not replace them.",
    "",
]
for label in manifest_labels:
    report = reports[label]
    lines.extend([
        f"## {label}",
        "",
        f"- Overall semantic outcome: `{report['overall']}`",
        f"- Sequential artifact: `{((report.get('repo_artifacts') or {}).get('sequential') or {}).get('artifact_slug', 'missing')}`",
        f"- Parallel artifact: `{((report.get('repo_artifacts') or {}).get('parallel') or {}).get('artifact_slug', 'missing')}`",
        "",
        "| Requirement | Outcome | Notes |",
        "| --- | --- | --- |",
    ])
    for requirement_id in ("SEM-01", "SEM-02", "SEM-03", "SEM-04", "SEM-05", "SEM-06"):
        requirement = report["requirements"][requirement_id]
        lines.append(f"| {requirement_id} | `{requirement['outcome']}` | {'; '.join(requirement.get('notes') or [])} |")
    lines.append("")
Path(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
}

final_exit_code() {
  [ "$AGGREGATE_OUTCOME" = "pass" ]
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
  local requested_mode
  local actual_mode
  local comparison_label
  for i in "${!REPO_PATHS[@]}"; do
    repo_path="${REPO_PATHS[$i]}"
    repo_label="${REPO_LABELS[$i]}"
    repo_godot_version="${REPO_GODOT_VERSIONS[$i]}"
    requested_mode="${REPO_REQUESTED_MODES[$i]:-auto}"
    actual_mode="${REPO_ACTUAL_MODES[$i]:-pending}"
    comparison_label="${REPO_COMPARISON_LABELS[$i]}"

    repo_name=$(basename -- "$repo_path")
    {
      IFS= read -r slug
      IFS= read -r git_ref
      IFS= read -r git_commit
      IFS= read -r git_branch
      IFS= read -r qualifies
    } < <(collect_repo_metadata_fields "$repo_path" "$repo_label" "$repo_godot_version" "$requested_mode")

    REPO_SLUGS[$i]="$slug"

    REPO_GIT_REFS[$i]="$git_ref"
    REPO_GIT_COMMITS[$i]="$git_commit"
    REPO_GIT_BRANCHES[$i]="$git_branch"

    mkdir -p "$RUN_ROOT/$slug"
    write_repo_meta_json "$RUN_ROOT/$slug" "$repo_path" "$repo_label" "$repo_name" "$slug" "null" "$RUN_TIMESTAMP" "$git_ref" "$git_commit" "$git_branch" "$repo_godot_version" "$qualifies" "$MANIFEST_PATH" "$requested_mode" "$actual_mode" "$comparison_label"
  done
}

parse_args() {
  local repo_path
  local manifest_path
  local raw_value
  local label
  local version

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --manifest)
        if [[ $# -lt 2 ]]; then
          echo "error: --manifest requires a path" >&2
          usage >&2
          exit 2
        fi

        if ! manifest_path=$(parse_manifest_path "$2"); then
          exit 2
        fi

        MANIFEST_PATH="$manifest_path"
        shift 2
        ;;
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

ensure_workspace_state_root() {
  mkdir -p "$STATE_ROOT" "$TMP_ROOT"
}

initialize_workspace_logs() {
  : > "$ENV_FILE"
  : > "$COMMANDS_LOG"
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
    printf '%s\n' "binary_path=$BINARY_PATH"
    printf '%s\n' "state_root=$STATE_ROOT"
    printf '%s\n' "manifest_path=${MANIFEST_PATH:-}"
  } > "$ENV_FILE"

  record_repo_metadata_env
}

setup_workspace() {
  ensure_workspace_root
  ensure_workspace_state_root
  initialize_workspace_logs
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

parse_args "$@"
initialize_workspace_root
setup_workspace
apply_manifest_defaults
expand_manifest_dual_mode_entries
record_workspace_env "$WORKTREE_PATH"
prepare_repo_metadata
build_local_binary || true
process_repo_indexing
write_repo_and_aggregate_summaries
generate_semantic_parity_reports
write_run_index_json

printf 'Proof run root: %s\n' "$RUN_ROOT"

if final_exit_code; then
  exit 0
fi

exit 1
