#!/usr/bin/env bash
# check-binary-composition.sh — release gate: assert what must NOT be inside a
# shipped artifact.
#
# Microsoft Defender's ML classifier flagged the v0.9.1-rc.1 binaries. The
# hardening pass that followed removed capability or opaque asset bytes — an
# executable stack, an in-process updater, test-only environment seams,
# embedded integration programs, and the in-image frontend bundle. Each of
# those regresses invisibly: one restored #include, one Makefile source-list
# edit, one revived call site, and nothing else in CI notices — the binary just
# quietly restores capability or opaque bytes that this hardening boundary is
# intended to exclude. Their removal reduces attack surface and makes release
# contents independently inspectable; it does not establish which feature, if
# any, caused an opaque third-party ML verdict.
#
# This script is the proof that each removal stayed removed. It asserts only
# NEGATIVE properties (needle absent), plus one canary string we know ships,
# because an absence check aimed at the wrong file — a compressed artifact, a
# stub, a truncated download — would otherwise pass vacuously and read green.
# A missing tool is a hard error for the same reason: a skipped assertion must
# never look like a satisfied one.
#
# Usage: scripts/ci/check-binary-composition.sh <binary-or-dir>...
#   Directories are scanned recursively; format (ELF / Mach-O / PE) is detected
#   per file from its magic bytes and each assertion runs where it is
#   meaningful. Exit 0 = every assertion passed, 1 = at least one failed,
#   2 = usage error, missing tool, or nothing checkable was found (a vacuous
#   run is a failure, not a pass).
set -euo pipefail

case "${1:-}" in
-h | --help)
    sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

# ── Needles ─────────────────────────────────────────────────────────
# Every needle below was grep-verified against the tree it comes from: it
# exists ONLY in the code the assertion is about, so a hit means that code is
# linked in, not that some unrelated string happens to look similar.

# S6: worker/Windows test seams read these env vars. A release binary that
# still honours them lets any process on the box steer the indexer's child
# processes and file placement. Their absence is a release-security boundary;
# no claim is made about whether a third-party classifier weighs them.
SEAM_NEEDLES=(
    'CBM_TEST_WORKER_DESCENDANT_PID_FILE'
    'CBM_TEST_CRASH_ON'
    'CBM_TEST_HANG_ON'
)

# The in-process updater combined network download with replacement of its own
# executable. That dual-use behavior is no longer needed in the daemon; updates
# now run from install.sh out-of-process. These four needles cover both halves
# of what was removed:
# the download base URL, the checksum URL built on top of it, the GitHub API
# release query of the daemon's background version check, and that request's
# Accept header (which survives even if the URL is ever assembled at runtime).
UPDATER_NEEDLES=(
    'releases/latest/download'
    'api.github.com/repos'
    'releases/latest'
    'Accept: application/vnd.github+json'
)

# SQLite loadable extensions = arbitrary code execution through a database
# file. The amalgamation is built with SQLITE_OMIT_LOAD_EXTENSION; if that ever
# drops out, dlopen()/LoadLibrary() re-enters the store layer.
# NOTE: the stripped release candidate can erase non-exported symbol names, so
# the two API names are not a sufficient proof on their own. The dlopen error
# text still works after stripping — it is compiled in only with the feature.
# When the unstripped candidate is inspected, the symbol checks are stronger.
SQLITE_LOADEXT_NEEDLES=(
    'sqlite3_load_extension'
    'sqlite3_enable_load_extension'
    'unable to open shared library ['
)

# The standard artifact must not contain the UI's HTTP server at all: an
# unauthenticated localhost listener plus a process enumerator is unnecessary
# capability in the standard composition. Needles are split across
# both UI translation units and are independent of each other, so a refactor of
# any single one cannot silently disarm the assertion:
#   'HTTP/1.1 %d %s'                  httpd.c response-status writer
#   'Request Header Fields Too Large'  httpd.c status-text table
#   '/api/ui-config', '/api/processes' http_server.c route dispatch
#   'ps -eo …' / '[c]odebase-memory-mcp'  the popen() process enumerator's
#                                     shell pipeline, both halves
# Verified unique to src/ui/ across src/, vendored/ and internal/ — the only
# other occurrences in the repo are tests/test_httpd.c and scripts/, neither of
# which is ever linked into a release binary.
UI_HTTP_NEEDLES=(
    'HTTP/1.1 %d %s'
    'Request Header Fields Too Large'
    '/api/ui-config'
    '/api/processes'
    'ps -eo pid,pcpu,rss,etime,comm'
    '[c]odebase-memory-mcp'
)

# Canary: proves the needle scan can actually see this file's strings. Without
# it, handing the gate a gzip, a stub or a 0-byte file would pass every
# absence assertion. 168+ occurrences in a real artifact, 0 in anything else.
CANARY_NEEDLE='codebase-memory-mcp'

# ── Args ────────────────────────────────────────────────────────────
TARGETS=()
for arg in "$@"; do
    case "$arg" in
    -*)
        echo "FAIL: unknown flag $arg (see --help)" >&2
        exit 2
        ;;
    *) TARGETS+=("$arg") ;;
    esac
done
if [ "${#TARGETS[@]}" -eq 0 ]; then
    echo "FAIL: no binaries or directories given (see --help)" >&2
    exit 2
fi

# ── ELF program-header reader ───────────────────────────────────────
# Resolved lazily: a Windows/macOS-only run must not fail for want of readelf,
# but an ELF in scope with no reader available must fail loudly (exit 2) rather
# than skip the executable-stack assertion.
ELF_READER=''
ELF_READER_KIND=''
resolve_elf_reader() {
    [ -n "$ELF_READER" ] && return 0
    for cand in readelf llvm-readelf /opt/homebrew/opt/llvm/bin/llvm-readelf \
        /usr/local/opt/llvm/bin/llvm-readelf; do
        if command -v "$cand" >/dev/null 2>&1; then
            ELF_READER="$cand"
            ELF_READER_KIND=readelf
            return 0
        fi
    done
    for cand in objdump llvm-objdump /opt/homebrew/opt/llvm/bin/llvm-objdump; do
        if command -v "$cand" >/dev/null 2>&1; then
            ELF_READER="$cand"
            ELF_READER_KIND=objdump
            return 0
        fi
    done
    return 1
}

# Echoes the GNU_STACK flag field ("RWE", "RW", "rwx", "rw-"), empty if the
# header is absent or unparseable.
exec_load_bytes() {
    # Total size of PT_LOAD segments carrying the execute bit. With
    # -z separate-code the executable segment holds only code; without it the
    # linker merges .rodata in, so this number balloons to nearly the whole file.
    case "$ELF_READER_KIND" in
    readelf)
        # NOT strtonum(): that is a gawk extension, and CI's awk is mawk, where
        # it is undefined -- the sum would silently be 0 and this gate would
        # pass vacuously on exactly the artifacts it exists to catch. Emit the
        # hex MemSiz fields and convert in the shell.
        total=0
        for hex in $("$ELF_READER" -lW "$1" 2>/dev/null |
            awk '/^  LOAD/ && $0 ~ /R E/ { print $6 }'); do
            total=$((total + 16#${hex#0x}))
        done
        echo "$total"
        ;;
    objdump)
        echo unsupported
        ;;
    esac
}

gnu_stack_flags() {
    case "$ELF_READER_KIND" in
    readelf)
        "$ELF_READER" -lW "$1" 2>/dev/null |
            awk '/GNU_STACK/ { print $(NF - 1); exit }'
        ;;
    objdump)
        # objdump -p prints "   STACK off ..." and the flags on the next line.
        "$ELF_READER" -p "$1" 2>/dev/null |
            awk '/STACK off/ { getline
                               for (i = 1; i <= NF; i++)
                                   if ($i == "flags") { print $(i + 1); exit } }'
        ;;
    esac
}

# ── Reporting ───────────────────────────────────────────────────────
# PASS and FAIL both go to stdout so the per-assertion sequence stays in order
# in a CI log (stderr would interleave nondeterministically); only the final
# verdict is echoed to stderr, which is what a failed step's tail shows.
pass_count=0
fail_count=0
FAILURES=()

report() { # verdict, assertion-id, file-token, message
    printf '%-4s %-22s %s: %s\n' "$1" "$2" "$3" "$4"
    if [ "$1" = FAIL ]; then
        fail_count=$((fail_count + 1))
        FAILURES+=("$2 $3: $4")
    else
        pass_count=$((pass_count + 1))
    fi
}

assert_absent() { # file, token, assertion-id, needle
    if LC_ALL=C grep -a -q -F -e "$4" "$1"; then
        report FAIL "$3" "$2" "'$4' is PRESENT (must not ship)"
    else
        report PASS "$3" "$2" "'$4' absent"
    fi
}

assert_present() { # file, token, assertion-id, needle, why-pass, why-fail
    if LC_ALL=C grep -a -q -F -e "$4" "$1"; then
        report PASS "$3" "$2" "'$4' present ($5)"
    else
        report FAIL "$3" "$2" "'$4' is MISSING — $6"
    fi
}

# ── Per-file checks ─────────────────────────────────────────────────
detect_format() { # file → elf | macho | pe | other
    magic=$(LC_ALL=C od -An -N4 -tx1 "$1" 2>/dev/null | tr -d ' \n')
    case "$magic" in
    7f454c46) echo elf ;;
    cffaedfe | cefaedfe | feedface | feedfacf | cafebabe | bebafeca) echo macho ;;
    4d5a*) echo pe ;;
    *) echo other ;;
    esac
}

checked_files=0
skipped_files=0

check_file() {
    file="$1"
    # Two path components: the binary is literally named
    # "codebase-memory-mcp", so the parent directory disambiguates the log.
    token=$(printf '%s' "$file" | awk -F/ '{ if (NF > 1) print $(NF - 1) "/" $NF; else print $NF }')
    fmt=$(detect_format "$file")
    if [ "$fmt" = other ]; then
        printf 'skip %-22s %s: not an ELF/Mach-O/PE binary\n' '-' "$token"
        skipped_files=$((skipped_files + 1))
        return 0
    fi

    printf '\n── %s [%s] ──\n' "$token" "$fmt"
    checked_files=$((checked_files + 1))

    # A0 — anti-vacuity canary; every assertion below is an absence check.
    assert_present "$file" "$token" A0-canary "$CANARY_NEEDLE" \
        'the needle scan can read this file' \
        'this is not one of our artifacts, or its strings are unreadable (packed/compressed/truncated) — every absence assertion below would pass vacuously'

    # A1 — executable stack (ELF only). Every Linux artifact of v0.9.1-rc.1
    # shipped GNU_STACK RWE. A writable+executable stack is unnecessary here
    # and weakens exploit mitigations, independently of any scanner verdict.
    if [ "$fmt" = elf ]; then
        if ! resolve_elf_reader; then
            echo "FAIL: no readelf/llvm-readelf/objdump available; cannot assert" \
                "the non-executable stack for $token — refusing to skip it" >&2
            exit 2
        fi
        flags=$(gnu_stack_flags "$file")
        # readelf spells the flags "RWE"/"RW", objdump "rwx"/"rw-" — match both.
        case "$flags" in
        '')
            report FAIL A1-noexec-stack "$token" \
                "no GNU_STACK program header found ($ELF_READER) — without PT_GNU_STACK the loader may fall back to READ_IMPLIES_EXEC"
            ;;
        *E* | *X* | *e* | *x*)
            report FAIL A1-noexec-stack "$token" \
                "GNU_STACK is $flags — the stack is EXECUTABLE (link with -z noexecstack; check .S/asm objects for a missing .note.GNU-stack)"
            ;;
        *)
            report PASS A1-noexec-stack "$token" "GNU_STACK is $flags (no execute bit)"
            ;;
        esac
    else
        printf 'n/a  %-22s %s: executable-stack check is ELF-only\n' A1-noexec-stack "$token"
    fi


    # A1b — read-only DATA must not live in the executable mapping. GNU ld
    # enables -z separate-code by default on x86-64 but NOT on aarch64, so the
    # arm64 binaries shipped ONE R E PT_LOAD spanning the whole image: 259 MB of
    # tree-sitter parse tables mapped executable while amd64 mapped the same
    # bytes R only. Section flags said A, not AX -- the kernel applies SEGMENT
    # permissions, so section flags were never the control. Heuristic on
    # purpose: if executable segments cover most of the file, .rodata is in them.
    if [ "$fmt" = elf ]; then
        exec_bytes=$(exec_load_bytes "$file")
        file_bytes=$(wc -c < "$file" | tr -d ' ')
        if [ "$exec_bytes" = unsupported ]; then
            printf 'n/a  %-22s %s: %s cannot report segment sizes\n' \
                A1b-rodata-noexec "$token" "$ELF_READER_KIND"
        elif [ "${exec_bytes:-0}" -gt 0 ] && [ "$file_bytes" -gt 0 ] &&
            [ $((exec_bytes * 100 / file_bytes)) -gt 60 ]; then
            report FAIL A1b-rodata-noexec "$token" \
                "executable PT_LOAD segments cover $((exec_bytes * 100 / file_bytes))% of the file ($exec_bytes/$file_bytes bytes) — read-only data is mapped executable (link with -z separate-code)"
        else
            report PASS A1b-rodata-noexec "$token" \
                "executable PT_LOAD segments cover $((exec_bytes * 100 / file_bytes))% of the file (read-only data is outside them)"
        fi
    else
        printf 'n/a  %-22s %s: segment-permission check is ELF-only\n' A1b-rodata-noexec "$token"
    fi

    # A2 — test-only seams.
    for needle in "${SEAM_NEEDLES[@]}"; do
        assert_absent "$file" "$token" A2-no-test-seams "$needle"
    done
    # Sweep for seams nobody thought to pin: a new CBM_TEST_* env var added to
    # production code lands here on its first release, not on the next audit.
    # One narrowly validated seam remains in Windows release artifacts by
    # decision: WINDOWS_USER_PATH_RUN_ID redirects the artifact smoke away from
    # the tester's actual user PATH. Crash/hang injectors are test-build-only and
    # are explicitly forbidden above. Anything else is novel and fails.
    seam_allowed='CBM_TEST_WINDOWS_USER_PATH_RUN_ID'
    seam_unexpected=''
    for found in $(LC_ALL=C grep -a -o -E 'CBM_TEST_[A-Za-z0-9_]+' "$file" 2>/dev/null |
        sort -u || true); do
        case " $seam_allowed " in
        *" $found "*) ;;
        *) seam_unexpected="$seam_unexpected $found" ;;
        esac
    done
    if [ -n "$seam_unexpected" ]; then
        report FAIL A2-no-test-seams "$token" \
            "unexpected CBM_TEST_* seam(s):$seam_unexpected (allowlist: $seam_allowed)"
    else
        report PASS A2-no-test-seams "$token" 'no CBM_TEST_* seams beyond the smoke allowlist'
    fi

    # A3 — updater/release URLs.
    for needle in "${UPDATER_NEEDLES[@]}"; do
        assert_absent "$file" "$token" A3-no-updater-urls "$needle"
    done

    # A4 — SQLite loadable-extension surface.
    for needle in "${SQLITE_LOADEXT_NEEDLES[@]}"; do
        assert_absent "$file" "$token" A4-no-sqlite-loadext "$needle"
    done
    # Positive control for A4: the symbol needles can be absent on a stripped
    # candidate even when the feature is compiled in, so that candidate's
    # absence assertion can pass vacuously. SQLite's own compile-option table names
    # every OMIT_* it was built with — when that table is present (it ships
    # unless SQLITE_OMIT_COMPILEOPTION_DIAGS is set) it proves the omission
    # positively rather than by absence.
    if LC_ALL=C grep -a -q -F -e 'sqlite_compileoption_get' "$file"; then
        assert_present "$file" "$token" A4-no-sqlite-loadext 'OMIT_LOAD_EXTENSION' \
            'sqlite reports SQLITE_OMIT_LOAD_EXTENSION' \
            'sqlite was NOT built with SQLITE_OMIT_LOAD_EXTENSION, so the dlopen()/LoadLibrary() extension loader is linked in'
    else
        printf 'n/a  %-22s %s: %s\n' A4-no-sqlite-loadext "$token" \
            "sqlite compile-option table absent; A4 rests on absence needles only"
    fi

    # A5 — UI/HTTP subsystem. One composition ships and it serves the graph UI
    # from embedded assets, so the HTTP server belongs here by construction.
    # CBM_CHECK_UI_ABSENT=1 still enforces absence for any future headless build.
    if [ "${CBM_CHECK_UI_ABSENT:-0}" = "1" ]; then
        for needle in "${UI_HTTP_NEEDLES[@]}"; do
            assert_absent "$file" "$token" A5-no-ui-http "$needle"
        done
    else
        printf 'n/a  %-22s %s: UI-capable artifact, HTTP server ships here by design\n' \
            A5-no-ui-http "$token"
    fi
}

# ── Walk the targets ────────────────────────────────────────────────
for target in "${TARGETS[@]}"; do
    if [ -d "$target" ]; then
        # "! -type d" rather than "-type f": a symlinked or otherwise unusual
        # artifact must produce a visible skip line, never vanish from the run.
        # sort keeps the report order stable across platforms (find order is not).
        while IFS= read -r f; do
            [ -n "$f" ] && check_file "$f"
        done <<EOF
$(find "$target" ! -type d | LC_ALL=C sort)
EOF
    elif [ -f "$target" ]; then
        check_file "$target"
    else
        echo "FAIL: $target is neither a file nor a directory" >&2
        exit 2
    fi
done

printf '\n'
if [ "$checked_files" -eq 0 ]; then
    echo "FAIL: no ELF/Mach-O/PE binary found in the given targets ($skipped_files file(s) skipped)" >&2
    echo "      a gate that checked nothing is not a green gate" >&2
    exit 2
fi
if [ "$fail_count" -ne 0 ]; then
    echo "BINARY COMPOSITION GATE FAILED: $fail_count assertion(s) over $checked_files binary/binaries" >&2
    for f in "${FAILURES[@]}"; do
        echo "  - $f" >&2
    done
    exit 1
fi
echo "BINARY COMPOSITION OK: $pass_count assertion(s) passed over $checked_files binary/binaries"
