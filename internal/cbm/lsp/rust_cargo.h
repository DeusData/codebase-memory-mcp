/* rust_cargo.h — Cargo.toml parser for the Rust LSP.
 *
 * Per RUST_LSP_FOLLOWUP §A3: we don't run Cargo or build, but we CAN
 * parse `Cargo.toml` and `[workspace] members` to learn:
 *   - the crate name (`[package].name`)
 *   - declared dependencies (`[dependencies]` + `[dev-dependencies]`)
 *   - workspace members + their relative paths
 *
 * The pipeline uses this to map `other_member::foo` → that member's
 * module QN, and to mark calls into known external deps as "external,
 * not local" rather than fully unresolved.
 *
 * The parser is a tiny hand-written TOML subset: handles `[section]`
 * headers, `key = "value"`, `key = { path = "...", … }` (the relevant
 * subset for our needs), arrays `members = ["a", "b"]`. It IGNORES
 * everything it doesn't understand — that's safe because Cargo.toml
 * is much richer than what we use. */

#ifndef CBM_LSP_RUST_CARGO_H
#define CBM_LSP_RUST_CARGO_H

#include "../arena.h"
#include <stdbool.h>

#define CBM_CARGO_MAX_DEPS    256
#define CBM_CARGO_MAX_MEMBERS  64

typedef struct {
    const char* name;       /* declared dependency name */
    const char* path;       /* path = "../foo" if local, else NULL */
} CBMCargoDep;

typedef struct {
    const char* member_name;   /* directory name */
    const char* member_path;   /* relative path inside workspace root */
    const char* package_name;  /* member's own [package].name (NULL until its
                                  Cargo.toml has been merged) — may differ
                                  from the directory name */
} CBMCargoMember;

typedef struct CBMCargoManifest {
    const char* package_name;    /* [package].name, NULL if missing */
    const char* package_version; /* [package].version, NULL if missing */
    bool is_workspace_root;      /* [workspace] section seen */

    CBMCargoDep deps[CBM_CARGO_MAX_DEPS];
    int dep_count;

    CBMCargoMember members[CBM_CARGO_MAX_MEMBERS];
    int member_count;
} CBMCargoManifest;

/* Parse a Cargo.toml-formatted string. The output strings are
 * arena-allocated (so the caller doesn't need to keep `src` alive). */
void cbm_cargo_parse(CBMArena* arena, const char* src, int src_len,
    CBMCargoManifest* out);

/* Convenience: does a given path-prefix look like one of the listed
 * dependency names? Used by the resolver to recognise external crate
 * paths. Comparison hyphen-folds ('-' ≡ '_'): crates.io names are
 * hyphenated (`async-trait`) while Rust path heads are underscored
 * (`async_trait`), so a literal strcmp could never match them. */
bool cbm_cargo_is_known_dep(const CBMCargoManifest* m, const char* head);

/* Find a workspace member by crate name (directory name or, when the
 * member's own manifest has been merged, its [package].name). Returns
 * NULL if absent. Hyphen-folding as above. */
const CBMCargoMember* cbm_cargo_find_member(const CBMCargoManifest* m,
    const char* name);

/* Hyphen-folding name equality: '-' and '_' compare equal, everything
 * else is byte-exact. NULL never matches. */
bool cbm_cargo_name_eq(const char* a, const char* b);

/* Parse a MEMBER crate's own Cargo.toml and merge its [dependencies] /
 * [dev-dependencies] keys into `dst` (duplicates by hyphen-folded name are
 * skipped; capacity capped at CBM_CARGO_MAX_DEPS). Workspace-inheritance
 * entries (`tokio = { workspace = true }`) merge for free since only the
 * key matters, and `local = { package = "real" }` renames store the LOCAL
 * key — the spelling that appears in use paths. Returns the member's
 * [package].name (arena-owned) or NULL. Pure parser — no file I/O; the
 * pipeline driver reads the file and calls this. */
const char* cbm_cargo_merge_member_deps(CBMArena* arena, CBMCargoManifest* dst,
    const char* toml, int toml_len);

#endif /* CBM_LSP_RUST_CARGO_H */
