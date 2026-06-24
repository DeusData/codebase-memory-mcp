/* rust_rustdoc.h — Ingest `cargo +nightly rustdoc --output-format json`
 * output into the Rust LSP type registry.
 *
 * Per the FOLLOWUP-extension Option A: when a project opts in by
 * running rustdoc once, we can ingest the resulting JSON and register
 * every item (struct, trait, fn, method) with accurate signatures.
 * That eliminates the "unseeded external crate" gap because rustdoc
 * already did the macro expansion and type resolution.
 *
 * The schema is rustdoc's "Crate" type. We handle:
 *   - paths   { id → { path: [..], kind: "struct"|"trait"|... } }
 *   - index   { id → Item { name, inner: { Function|Struct|... }, … } }
 *
 * We ignore everything we don't understand. The JSON format is unstable
 * across rustdoc versions; we tolerate missing fields gracefully and
 * just skip items we can't make sense of.
 *
 * Schema reference: https://github.com/rust-lang/rust/blob/master/src/rustdoc-json-types/lib.rs
 */

#ifndef CBM_LSP_RUST_RUSTDOC_H
#define CBM_LSP_RUST_RUSTDOC_H

#include "../arena.h"
#include "type_registry.h"

/* Ingest a rustdoc JSON document into the registry. `crate_qn` is the
 * QN prefix to use for items (e.g. "serde" for crate `serde`). Returns
 * the number of items registered. */
int cbm_rust_rustdoc_ingest(CBMTypeRegistry* reg, CBMArena* arena,
    const char* json, int json_len, const char* crate_qn);

#endif /* CBM_LSP_RUST_RUSTDOC_H */
