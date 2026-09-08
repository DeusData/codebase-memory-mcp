#!/usr/bin/env bash
# Every packaging surface that states a product version must state a version we
# actually released, and the registry-facing ones must state the newest one.
#
# Why this exists: server.json, pkg/npm/package.json and the Chocolatey nuspec
# sat at 0.8.1 while v0.10.8 was the shipped release — nine releases of drift in
# metadata that is published to package registries. It was the third
# version-metadata drift in one month. pkg/go additionally hardcodes its version
# into the release URL it downloads from, so the stale value was not cosmetic:
# `go install` fetched v0.8.1 binaries.
#
# Why there is no single canonical version file to check against: CBM_VERSION is
# injected at BUILD time (-DCBM_VERSION, see Makefile.cbm) and the release is
# dispatched with a version input, so nothing in the tree declares the truth.
# That absence is precisely why these files drifted — there was nothing to
# check them against. The release workflow then institutionalised the drift for
# three of them: publish-registries rewrites pkg/npm/package.json and
# pkg/pypi/pyproject.toml from the dispatch input, and publish-mcp-registry
# rewrites server.json, each with a comment noting the repo copy "can lag". The
# published artifact was therefore correct while the repo told everyone reading
# it a false version, and no surface outside that sync path was corrected at
# all.
#
# So the truth used here is the newest STABLE v-prefixed git tag: the only
# in-repo statement of what actually shipped. Between releases the repo
# legitimately sits at the last released version — no surface is bumped ahead of
# a release — so "every surface equals the newest tag" is stable, not a
# perpetual red.
#
# Two independent assertions, so the gate cannot go vacuous when tags are absent
# (shallow clone, `git archive` tarball, a container that cloned with
# --no-tags). Mutual consistency needs no git at all and therefore ALWAYS runs;
# the tag comparison runs when tags exist and announces itself loudly when they
# do not. A missing truth source degrades this gate, and it says so — it never
# reports a silent pass.
#
# Usage: tests/test_version_metadata_contract.sh
#   Exit 0 = every assertion passed. Exit 1 = at least one violation. A run that
#   could extract no version from a registered surface is a FAILURE, not a pass.

set -euo pipefail

case "${1:-}" in
-h | --help)
    sed -n '2,44p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

failures=0

fail() {
    echo "FAIL: $*" >&2
    failures=$((failures + 1))
}

# ── 1. The truth: the newest STABLE released tag ────────────────────
# Prereleases are excluded (v0.9.1-rc.1): an RC is not what the packaging
# surfaces track. Bare tags are excluded too — the v0.10.7 release incident left
# a tag named "0.10.7" with no v prefix, and the installers resolve
# releases/download/v<version>/..., so only v-prefixed tags describe a release
# a user can actually fetch.
newest_tag=""
if git rev-parse --git-dir >/dev/null 2>&1; then
    # `|| true` is load-bearing: under pipefail a tagless clone makes grep exit
    # 1, and set -e would kill the contract HERE — exiting non-zero with no
    # output at all, in exactly the venue (a --no-tags CI checkout) where this
    # gate most needs to say what it could and could not verify.
    newest_tag=$(
        {
            git tag -l 'v*' 2>/dev/null |
                grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' |
                sed 's/^v//' |
                sort -t. -k1,1n -k2,2n -k3,3n |
                tail -1
        } || true
    )
fi

if [[ -n "$newest_tag" ]]; then
    echo "Truth: newest stable release tag is v$newest_tag"
else
    # Not a skip of the contract — only of the half that needs git history.
    echo "NOTE: no stable v* tags reachable (shallow clone, tarball, or --no-tags)." >&2
    echo "NOTE: the tag comparison is UNAVAILABLE; mutual consistency is still enforced." >&2
fi

# ── 2. Registered surfaces ──────────────────────────────────────────
# "release" = must equal the newest stable tag. Every one of these is
# version-only or carries a checksum we re-pin with it, so bumping is a
# mechanical edit with no external lookup beyond the release's own checksums.txt.
#
# "pin:<v>" = frozen at a version we have NOT re-pinned. These embed a
# per-release sha256 of a published asset, so bumping them means re-pinning
# checksums — a distribution chore, not a metadata edit. They are still gated:
# the version must be internally consistent within the file and must be a
# version that really shipped. Every pin needs a reason below (PIN_REASONS), so
# a frozen surface can never become an unexplained exemption.
SURFACES=(
    "server.json|release"
    "pkg/npm/package.json|release"
    "pkg/pypi/pyproject.toml|release"
    "pkg/pypi/src/codebase_memory_mcp/_cli.py|release"
    "pkg/go/cmd/codebase-memory-mcp/main.go|release"
    "pkg/chocolatey/codebase-memory-mcp.nuspec|release"
    "pkg/chocolatey/tools/chocolateyInstall.ps1|release"
    "pkg/homebrew/Formula/codebase-memory-mcp.rb|pin:0.10.3"
    "pkg/scoop/codebase-memory-mcp.json|pin:0.8.1"
    "pkg/aur/PKGBUILD|pin:0.8.1"
    "pkg/aur/.SRCINFO|pin:0.8.1"
)

# Why each pinned surface is frozen, and what clearing it costs. To bring one
# up to date: download the release's checksums.txt
# (gh release download v<X.Y.Z> --pattern checksums.txt), replace the version
# AND every sha256 it pins, then move the entry to "release" above.
PIN_REASONS=(
    "pkg/homebrew/Formula/codebase-memory-mcp.rb|pins 4 per-asset sha256 (darwin/linux x arm/intel); last re-pinned for v0.10.3"
    "pkg/scoop/codebase-memory-mcp.json|pins the windows-amd64.zip sha256; last re-pinned for v0.8.1"
    "pkg/aur/PKGBUILD|pins sha256sums_x86_64 + sha256sums_aarch64; last re-pinned for v0.8.1"
    "pkg/aur/.SRCINFO|generated from PKGBUILD, so it must move with it, not before it"
)

# Paths that legitimately contain a semver but make no claim about the
# product's current version. Each entry states what its number really is, so an
# unexplained exemption cannot hide here.
EXEMPT=(
    'pkg/winget/manifests/'          # published version-pinned manifests: the directory
                                     # name IS the version, and rewriting a shipped
                                     # manifest would falsify what 0.8.1 actually was
    'pkg/pypi/tests/'                # unit-test fixtures; the literal is arbitrary
                                     # cache-path input, not a published claim
    'pkg/npm/test/'                  # same: publication tests assert shape, not value
    'pkg/pypi/requirements-publish.txt'  # publish-toolchain pins (twine/hatchling)
    'tests/test_version_metadata_contract.sh'  # this file, which names the numbers above
)

# ── 3. Extraction ───────────────────────────────────────────────────
# Two independent claim forms, because a surface can lie in either one and the
# language-count contract learned the hard way that a gate sharing its regex
# with the enumeration can only confirm what the enumeration already saw:
#   declaration  version "X.Y.Z" / "version": "X.Y.Z" / pkgver=X.Y.Z / <version>
#   release URL  releases/download/vX.Y.Z/... , releases/tag/vX.Y.Z , -X.Y.Z-linux
# The Homebrew formula interpolates v#{version} into its URLs and so has no URL
# form at all; that is fine, the anti-vacuity check below is per FILE, not per
# form.
versions_in() {
    local path="$1"
    {
        # shellcheck disable=SC2016  # the literal text $version is what the
        # PowerShell surface writes, so this regex must NOT expand it
        grep -oiE '(pkgver|packageversion|\$version|"version"|<version>|^[[:space:]]*version)[^0-9]{0,16}[0-9]+\.[0-9]+\.[0-9]+' "$path" |
            grep -oE '[0-9]+\.[0-9]+\.[0-9]+$'
        grep -oE 'releases/(download|tag)/v[0-9]+\.[0-9]+\.[0-9]+' "$path" |
            grep -oE '[0-9]+\.[0-9]+\.[0-9]+$'
        grep -oE -- '-[0-9]+\.[0-9]+\.[0-9]+-(linux|darwin|windows)' "$path" |
            grep -oE '[0-9]+\.[0-9]+\.[0-9]+'
        # Bare literal form: pkg/pypi returns a hardcoded fallback from
        # _version() when importlib.metadata cannot resolve the installed
        # distribution, and that string is what `--version` then prints. The
        # anti-vacuity check below caught this one: the declaration form above
        # cannot see it, because the version keyword is in the function NAME.
        grep -oE 'return "[0-9]+\.[0-9]+\.[0-9]+"' "$path" |
            grep -oE '[0-9]+\.[0-9]+\.[0-9]+'
    } | sort -u
}

pin_reason_for() {
    local want="$1" entry
    for entry in "${PIN_REASONS[@]}"; do
        if [[ "${entry%%|*}" == "$want" ]]; then
            printf '%s' "${entry#*|}"
            return 0
        fi
    done
    printf ''
}

# ── 4. Every registered surface ─────────────────────────────────────
release_surface_count=0
checked_values=0

for entry in "${SURFACES[@]}"; do
    path="${entry%%|*}"
    rule="${entry#*|}"

    if [[ ! -f "$path" ]]; then
        fail "registered surface $path does not exist — update SURFACES"
        continue
    fi

    hits=$(versions_in "$path" || true)

    # Anti-vacuity. A surface that states no version is either no longer a
    # surface or has changed wording in a way this contract can no longer read.
    # Either way the silence is a defect, not a pass — the whole point of this
    # gate is that nothing states a version unchecked.
    if [[ -z "$hits" ]]; then
        fail "$path is registered as a version surface but states no version —" \
            "either it lost the claim (drop it from SURFACES) or its wording" \
            "changed and this check went vacuous"
        continue
    fi

    if [[ "$rule" == "release" ]]; then
        release_surface_count=$((release_surface_count + 1))
    else
        pinned="${rule#pin:}"
        reason="$(pin_reason_for "$path")"
        if [[ -z "$reason" ]]; then
            fail "$path is pinned at $pinned with no entry in PIN_REASONS —" \
                "a frozen surface must say WHY it is frozen"
        fi
        if [[ -n "$newest_tag" ]] && [[ "$pinned" == "$newest_tag" ]]; then
            fail "$path is pinned at $pinned, which is now the newest release —" \
                "move it to the \"release\" rule and drop its PIN_REASONS entry"
        fi
    fi

    while IFS= read -r found; do
        [[ -z "$found" ]] && continue
        checked_values=$((checked_values + 1))

        if [[ "$rule" == "release" ]]; then
            # Mutual consistency runs with or without git; the tag comparison
            # only when a tag exists.
            if [[ -n "$newest_tag" ]] && [[ "$found" != "$newest_tag" ]]; then
                fail "$path declares $found, the newest release is $newest_tag"
            fi
        else
            pinned="${rule#pin:}"
            if [[ "$found" != "$pinned" ]]; then
                fail "$path declares $found but is pinned at $pinned —" \
                    "it embeds a per-release sha256, so the version and the" \
                    "checksums must move together (update PIN_REASONS or re-pin both)"
            fi
            if [[ -n "$newest_tag" ]] &&
                [[ "$(printf '%s\n%s\n' "$found" "$newest_tag" |
                    sort -t. -k1,1n -k2,2n -k3,3n | tail -1)" != "$newest_tag" ]]; then
                fail "$path declares $found, which is NEWER than the newest" \
                    "release tag v$newest_tag — it names a release that does not exist"
            fi
        fi
    done <<<"$hits"
done

# ── 5. Mutual consistency, independent of git ───────────────────────
# This is the half that still works in a shallow clone. If the tag comparison
# above was unavailable, this is the only thing standing between the repo and
# silent drift, so it must never be conditional on git.
release_values=""
for entry in "${SURFACES[@]}"; do
    path="${entry%%|*}"
    [[ "${entry#*|}" == "release" ]] || continue
    [[ -f "$path" ]] || continue
    # `|| true` is load-bearing: versions_in ends in a pipeline and runs under
    # pipefail, so a claim form absent from THIS file (server.json has no bare
    # return form) makes it exit non-zero and set -e would kill the contract
    # mid-run — exiting 1 with no message, which reads exactly like a failure
    # that forgot to explain itself.
    release_values="$release_values$(versions_in "$path" || true)
"
done
distinct=$(printf '%s' "$release_values" | grep -vE '^$' | sort -u || true)
distinct_count=$(printf '%s\n' "$distinct" | grep -cE '^[0-9]' || true)

if ((distinct_count > 1)); then
    fail "the release-tracking surfaces disagree with each other: $(
        printf '%s' "$distinct" | tr '\n' ' '
    )"
fi
if ((distinct_count == 0)); then
    fail "no version could be read from ANY release-tracking surface —" \
        "this contract cannot vouch for anything and must not report success"
fi

# ── 6. No UNREGISTERED packaging surface may state a version ────────
# Without this, adding an eighth packaging target reintroduces exactly the drift
# this contract exists to prevent, and every check above would still pass. The
# sweep pattern is deliberately BROADER than the extractors in section 3: if the
# two shared a regex, a claim form the extractors cannot see would be invisible
# to the sweep as well.
while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    for entry in "${SURFACES[@]}"; do
        [[ "$path" == "${entry%%|*}" ]] && continue 2
    done
    for skip in "${EXEMPT[@]}"; do
        [[ "$path" == "$skip"* ]] && continue 2
    done
    fail "$path states a product version but is not registered. Add it to" \
        "SURFACES so it stays in step with the release, or to EXEMPT with a" \
        "note saying what its number actually is."
done < <(
    git grep -IlE '(version|pkgver|releases/(download|tag)/v)[^0-9]{0,16}[0-9]+\.[0-9]+\.[0-9]+|return "[0-9]+\.[0-9]+\.[0-9]+"' \
        -- server.json pkg tests/test_version_metadata_contract.sh 2>/dev/null || true
)

if ((failures > 0)); then
    echo "FAIL: $failures version-metadata contract violation(s)" >&2
    exit 1
fi

if [[ -n "$newest_tag" ]]; then
    echo "Version-metadata contract passed (${#SURFACES[@]} surfaces," \
        "$release_surface_count tracking v$newest_tag, $checked_values value(s) checked)"
else
    echo "Version-metadata contract passed WITHOUT a tag truth source" \
        "(${#SURFACES[@]} surfaces, $checked_values value(s) checked, mutual consistency only)"
fi
