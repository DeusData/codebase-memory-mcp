#!/usr/bin/env bash
set -euo pipefail

# Run the actual UI audit against isolated source trees. No fixture makes a
# network request, and no exception is granted to arbitrary test filenames.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEST_TMPDIR=$(mktemp -d)
trap 'rm -rf "$TEST_TMPDIR"' EXIT
CASES=0
FAILED=0
FIXTURE=''

new_fixture() {
    CASES=$((CASES + 1))
    FIXTURE="$TEST_TMPDIR/case-$CASES"
    mkdir -p "$FIXTURE/scripts" "$FIXTURE/graph-ui/src" "$FIXTURE/src/ui"
    cp "$ROOT/scripts/security-ui.sh" "$FIXTURE/scripts/security-ui.sh"
    printf 'static const char *bind_address = "127.0.0.1";\n' > "$FIXTURE/src/ui/httpd.c"
    printf '// Local request handlers.\n' > "$FIXTURE/src/ui/http_server.c"
}

source_url() {
    local relative_path="$1" url="$2"
    mkdir -p "$(dirname "$FIXTURE/graph-ui/src/$relative_path")"
    printf 'export const fixtureUrl = "%s";\n' "$url" > "$FIXTURE/graph-ui/src/$relative_path"
}

expect_allowed() {
    local label="$1"
    if bash "$FIXTURE/scripts/security-ui.sh" > "$FIXTURE/result.log" 2>&1; then
        printf 'PASS: %s\n' "$label"
    else
        printf 'FAIL: %s should pass\n' "$label"
        cat "$FIXTURE/result.log"
        FAILED=$((FAILED + 1))
    fi
}

expect_blocked() {
    local label="$1" relative_path="$2"
    if bash "$FIXTURE/scripts/security-ui.sh" > "$FIXTURE/result.log" 2>&1; then
        printf 'FAIL: %s should fail\n' "$label"
        FAILED=$((FAILED + 1))
    elif grep -F "BLOCKED: graph-ui/src/$relative_path:" "$FIXTURE/result.log" | grep -q 'External URL:'; then
        printf 'PASS: %s\n' "$label"
    else
        printf 'FAIL: %s failed for another reason\n' "$label"
        cat "$FIXTURE/result.log"
        FAILED=$((FAILED + 1))
    fi
}

while IFS= read -r reviewed_fixture; do
    new_fixture
    source_url "$reviewed_fixture" 'https://example.invalid/fixture'
    expect_allowed "reviewed URL fixture $reviewed_fixture"
done <<'FIXTURES'
adr/AdrWorkspace.test.tsx
architecture/SourceEvidenceDrawer.test.tsx
architecture/container-topology.test.ts
architecture/repository-guide.test.ts
browser-ai/BrowserChatDock.ask-model.test.tsx
browser-ai/BrowserChatDock.test.tsx
browser-ai/explanation-response.test.ts
browser-ai/file-facts.test.ts
browser-ai/file-outline.ini.test.ts
browser-ai/file-outline.test.ts
settings/model-catalog.test.ts
FIXTURES

new_fixture
source_url 'browser-ai/other.test.ts' 'https://example.invalid/fixture'
expect_blocked 'an arbitrary test is not exempt' 'browser-ai/other.test.ts'

new_fixture
source_url 'browser-ai/BrowserChatDock.tsx' 'https://example.invalid/runtime'
expect_blocked 'the runtime counterpart is not exempt' 'browser-ai/BrowserChatDock.tsx'

new_fixture
source_url 'counterfeit/browser-ai/BrowserChatDock.test.tsx' 'https://example.invalid/fixture'
expect_blocked 'a matching basename in another directory is not exempt' 'counterfeit/browser-ai/BrowserChatDock.test.tsx'

new_fixture
source_url 'browser-ai/model-policy.ts' 'https://huggingface.co/onnx-community/Qwen2.5-Coder-0.5B-Instruct'
expect_allowed 'the pinned model location remains allowed'

new_fixture
source_url 'browser-ai/model-policy.ts' 'https://huggingface.co/unreviewed/model'
expect_blocked 'an unpinned model location remains blocked' 'browser-ai/model-policy.ts'

new_fixture
source_url 'browser-ai/other.ts' 'https://huggingface.co/onnx-community/Qwen2.5-Coder-0.5B-Instruct'
expect_blocked 'the model location is not allowed outside its policy file' 'browser-ai/other.ts'

new_fixture
for index in $(seq 1 25); do
    source_url "runtime/file-$index.ts" 'http://127.0.0.1/fixture'
done
# Choose the last file in the same traversal used by the scanner, so this
# catches a truncated scan independently of filesystem directory ordering.
last_file=$(find "$FIXTURE/graph-ui/src" -type f -name '*.ts' -exec grep -lE 'https?://' {} \; | tail -n 1)
last_relative="${last_file#"$FIXTURE/graph-ui/src/"}"
source_url "$last_relative" 'https://example.invalid/after-twenty'
expect_blocked 'a forbidden runtime URL after the twentieth file is inspected' "$last_relative"

printf '%s cases, %s failed\n' "$CASES" "$FAILED"
[[ "$FAILED" -eq 0 ]]
