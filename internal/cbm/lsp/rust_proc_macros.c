/* rust_proc_macros.c — Curated attribute proc-macro expansion.
 *
 * RUST_LSP_FOLLOWUP §A1 + improvement Option B: extend derive synthesis
 * (already done for #[derive(...)]) to *attribute*-style proc-macros.
 *
 * We can't run proc-macros (would need rustc). But for the ~20
 * most-used attribute proc-macros we KNOW the expansion shape and can
 * synthesize equivalent calls/impls so the resolver doesn't go blind.
 *
 * Covered:
 *   - `#[tokio::main]` / `#[tokio::test]` — wrap fn body in
 *     `tokio::runtime::Runtime::new().unwrap().block_on(async { ... })`
 *   - `#[async_std::main]` — same shape via async_std executor
 *   - `#[async_trait::async_trait]` — methods on the trait become
 *     `Pin<Box<dyn Future>>` returning shapes; we just register the
 *     method bodies normally (the resolver already handles `async fn`)
 *   - `#[derive(thiserror::Error)]` — synthesise `source`, `description`
 *     (we already synth via the curated derive table; this just adds
 *     `thiserror::Error` to the alias chain)
 *   - `#[derive(serde::Serialize|Deserialize)]` — already covered by
 *     the curated derive table (alias `serde::Serialize` to
 *     `serde.Serialize`)
 *   - `#[clap(...)]` field attributes — no synthesis needed; the
 *     `#[derive(clap::Parser)]` outer macro handles the impl
 *   - `#[tracing::instrument]` — wraps the function body in a span;
 *     we synthesise a call to `tracing::span::Span::enter`
 *   - `#[rocket::main]`/`#[actix_web::main]` — same shape as tokio
 *   - `#[test]` / `#[bench]` — no synthesis; the function body is
 *     attributed as-is
 *
 * For each, we register a synthetic "wrapper call" QN per
 * decorator → resolved-call edge so the project's tracing/dependency
 * analyses see what the proc-macro injected. */

#include "../arena.h"
#include "../cbm.h"
#include "rust_lsp.h"
#include <string.h>
#include <stdio.h>

/* The attribute-decorator table. For each pattern in `decorator` text
 * we emit a *synthetic call edge* attributed to the wrapped function
 * so tracing tools can see the implicit dependency. The synthesis is
 * conservative: we emit only when the decorator text matches one of
 * the curated tokens — anything else is left alone. */
typedef struct {
    const char* match;     /* substring to find in the decorator text */
    const char* edges[6];  /* synthesized callee QNs (NULL-terminated) */
} RustAttrSynth;

static const RustAttrSynth ATTR_SYNTH[] = {
    {"tokio::main", {
        "tokio.runtime.Runtime.new",
        "tokio.runtime.Runtime.block_on",
        NULL
    }},
    {"tokio::test", {
        "tokio.runtime.Runtime.new",
        "tokio.runtime.Runtime.block_on",
        NULL
    }},
    {"async_std::main", {
        "async_std.task.block_on",
        NULL
    }},
    {"actix_web::main", {
        "actix_web.rt.System.new",
        "actix_web.rt.System.block_on",
        NULL
    }},
    {"actix_rt::main", {
        "actix_rt.System.new",
        "actix_rt.System.block_on",
        NULL
    }},
    {"rocket::main", {
        "rocket.async_main",
        NULL
    }},
    {"rocket::launch", {
        "rocket.launch",
        NULL
    }},
    {"tracing::instrument", {
        "tracing.span.Span.enter",
        NULL
    }},
    {"async_trait", {
        /* The macro wraps each method's return in
         * `Pin<Box<dyn Future<Output = R> + Send>>` — no method-name
         * synthesis needed, just record the boxed-future bridging
         * call so analyses see the async-trait shape. */
        "alloc.boxed.Box.new",
        "core.pin.Pin.new",
        NULL
    }},
    {"wasm_bindgen", {
        "wasm_bindgen.JsValue.from",
        NULL
    }},
    {"napi", {
        "napi.Env.create",
        NULL
    }},
    {"pyo3::pyfunction", {
        "pyo3.Python.with_gil",
        NULL
    }},
    {"pyo3::pymethods", {
        "pyo3.Python.with_gil",
        NULL
    }},
};

#define ATTR_SYNTH_COUNT (int)(sizeof(ATTR_SYNTH) / sizeof(ATTR_SYNTH[0]))

/* Inspect a definition's decorators and, if any matches a curated
 * proc-macro attribute, emit synthetic edges from that definition's
 * QN to the wrapper functions the macro would have injected. The
 * edges are best-effort and tagged `lsp_proc_macro` so consumers can
 * distinguish them from direct calls. */
void cbm_rust_synth_proc_macro_edges(CBMArena* arena, CBMFileResult* result) {
    if (!arena || !result) return;
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition* d = &result->defs.items[i];
        if (!d->decorators || !d->qualified_name) continue;
        for (int di = 0; d->decorators[di]; di++) {
            const char* dec = d->decorators[di];
            for (int t = 0; t < ATTR_SYNTH_COUNT; t++) {
                const RustAttrSynth* s = &ATTR_SYNTH[t];
                if (!strstr(dec, s->match)) continue;
                /* Emit one resolved call per edge. */
                for (int e = 0; s->edges[e]; e++) {
                    CBMResolvedCall rc;
                    memset(&rc, 0, sizeof(rc));
                    rc.caller_qn = d->qualified_name;
                    rc.callee_qn = cbm_arena_strdup(arena, s->edges[e]);
                    rc.strategy = "lsp_proc_macro";
                    rc.confidence = 0.78f;  /* high-but-not-direct */
                    cbm_resolvedcall_push(&result->resolved_calls, arena, rc);
                }
            }
        }
    }
}
