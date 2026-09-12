# Perl duck-typing, accessor-chain resolution & the cross-language ceiling audit

Status: all shipped work on `main` (through `daaf538c`). Campaign result:
**Mojolicious CALLS 2216 → 4660 (+110%)**, every edge sound.

This note is the sequel to `PERL-CROSS-FILE-INHERITANCE.md` (which took Perl from
2218 → 3331 by teaching the resolver `use Mojo::Base` inheritance + cross-file
inherited dispatch). It records the *next* era — typed-receiver method chains,
structural (duck) typing of typeless accessors, and a four-language real-repo
audit that established where the sound frontier actually ends — plus every
dead-end that was built, measured, and reverted, so the next iteration does not
re-walk them.

> **Citation discipline.** `file:line` anchors drift as the resolver moves.
> Re-grep before cutting code. Strategy names and function names are stable
> enough to grep by symbol.

---

## 1. The shipped ladder (3331 → 4660)

All measured on a **fresh** Mojolicious checkout (see §6 for why "fresh"
matters). Each rung is a separate commit; edge deltas are real-repo `CALLS`.

| commit | lever | Δ CALLS | mechanism |
|---|---|---|---|
| `8a679979` | `has [qw(a b)]` accessors | (foundation) | emit accessor defs from the qw word-list form, not just `has 'x'` |
| `5aeeaeee` / `c6d67197` | `my $c = shift` / list-unpack `$c` → controller | (foundation) | type the Mojolicious controller invocant |
| `7f588ab3` | imported nullary `func->method` (`curfile`) | curfile family | emit a second call-row for a lowercase bareword method-invocant so the LSP edge has a site to attach to |
| `bfbb5f57` / `c231baa8` | receiver-typed chains: colon/dot + `__PACKAGE__` factory | +43 | reconcile colon/dot spelling; resolve `__PACKAGE__->new` factory return |
| `a47e7942` | `my $x = imported_func;` (paren-less factory) | — | type the bound var so it chains |
| `d57a7420` | positional `$c` at any list position | — | around/hook callbacks |
| `363f9584` | **attach @ISA to used-module types** | **+173** | the biggest single lever — see §2 |
| `636d6a79` | seed return-type classes into the @ISA chain-walk | +27 | inherited methods on accessor-chain receivers |
| `dadaff3b` | **per-file structural (duck) typing** | +15 | infer a typeless accessor's return class from its usage method-set — see §3 |
| `daaf538c` | **cross-file duck-typing pre-pass** | **+54** | make the duck inference propagate to all files regardless of processing order — see §4 |

The two structural levers worth understanding deeply are **@ISA-on-used-modules**
(§2) and **duck-typing** (§3–§4). The rest are spelling/site-attachment
plumbing.

---

## 2. The site-attachment invariant (why plumbing commits exist at all)

**An LSP-resolved edge survives only if extraction emitted a matching call-row
at that site.** `pass_parallel.c` (~the resolved-call/CBMCall reconcile) matches
an LSP `resolved_call` to an extracted `CBMCall` row by **site-span +
callee-bare-segment**. If extraction never emitted a call row for the site, the
LSP edge is dropped on the floor — silently.

Consequence: several "resolver" fixes are really *extraction* fixes. E.g.
`curfile->child` (`7f588ab3`): the resolver could type `curfile`'s return, but
extraction emitted no call row for the lowercase-bareword invocant, so there was
nothing to attach to. Fix = emit a second call row with
`requires_lsp_resolution=true` (which makes it resolve **only** via LSP — no
textual fallback, so it is zero-edge-safe when the LSP misses).

**Debugging tip.** When a receiver types correctly in a unit test but produces
no edge on the real repo, suspect site-attachment first: grep the extracted call
rows for the site before touching the resolver.

### 2.1 Colon vs dot spelling (the multi-segment trap)

Used-module types are keyed **colon** (`"Mojo::File"`, as written in `use`).
Return types are stored **dotted** (`"Mojo.File"`, `::`→`.` via
`perl_infer_return_types`). `cbm_registry_lookup_type` is **exact-match**, so a
multi-segment dotted return type never finds its colon-keyed method table. A
single-segment class (`Widget`, where dot==colon) masks this in fixtures — it
only bites on real multi-segment chains. Fix: `perl_class_qn_colon_variant` +
a retry at both typed-receiver lookup sites.

---

## 3. Structural (duck) typing — inferring a typeless accessor's class

Mojo::Base accessors declared `has 'tx'` (no default) have **no static return
type**. `$self->tx->res->finish` therefore dangled. Duck-typing recovers the
type from *how the accessor's result is used*:

> For an untyped accessor `M1`, collect the set of methods called on
> `$self->M1->{...}`. Find the unique in-repo class whose own+inherited method
> table covers **all** of them; require ≥ `PERL_DUCK_MIN` (=3) distinct
> non-universal methods; reduce covering classes to the **base-most** one; if
> exactly one survives (and it isn't the accessor's own module), that is `M1`'s
> return type.

Key helpers (`internal/cbm/lsp/perl_lsp.c`): `perl_duck_is_universal` (excludes
`new`/`isa`/`can`/`tap`/`to_string`/…), `perl_duck_class_defines` (own + @ISA
presence, bounded DFS), `perl_duck_is_a` (descendant check for base-most
reduction), `perl_duck_collect` (walks the AST for `$self->M1->M2`).

**Soundness is the whole point.** The gate is intentionally strict:
- `base_n != 1` (ambiguous) ⇒ zero-edge.
- The canonical rejection is **`tx` itself**: it is polymorphic (HTTP vs
  WebSocket transaction); its usage set spans base-`Mojo::Transaction` methods
  **plus** WebSocket-only `send`/`is_websocket`, which **no single class**
  covers ⇒ `base_n != 1` ⇒ correctly stays zero-edge. Typing `tx`→WebSocket
  would fabricate `send` on HTTP transactions. The gate refusing here is the
  correct-edge guarantee working, not a miss.

Per-file duck-typing (`dadaff3b`) inferred 5 accessors correctly (reactor,
ioloop, Controller::app, Controller::res, Test::Mojo::ua) for +15.

---

## 4. The cross-file pre-pass (`daaf538c`, +54) — a processing-order bug

`dadaff3b` wrote the inferred type into `all_defs[i].return_types` **as a side
effect during the accessor's own file's resolution** (guarded by
`def_module_qn == module_qn`). `cbm_run_perl_lsp_cross` runs **per file** in a
loop (`src/pipeline/pass_lsp_cross.c`; `all_defs` is built once by
`cbm_pxc_collect_all_defs` into a persistent arena). So any file resolved
**before** the accessor's own file never saw the inferred type — e.g.
`Controller::res`→Response was correctly inferred while processing
`Controller.pm`, but the action/test files using `$c->res->code` were resolved
earlier and got nothing. Root cause is **processing order**, not arena lifetime
(the written string `K` is a persistent `def_module_qn`, not scratch).

**Fix:** `cbm_perl_duck_prepass` runs *before* the resolve loop. Phase 1
aggregates `$self`/`$class` accessor-chain method-sets **globally** across all
files (persistent arena), attributing each accessor to its defining-class def
(own or @ISA, via `perl_duck_find_accessor_def`). Phase 2 runs the same
base-most-unique gate and writes return types up front. The existing
return-type-class @ISA seeding then dispatches every file's chains regardless of
order.

> **Critical wiring gotcha.** `index_repository` runs the **parallel** pipeline
> (`src/pipeline/pipeline.c` → `run_parallel_pipeline` → `cbm_parallel_resolve`),
> **not** the sequential `pass_lsp_cross` path. A first cut wired the pre-pass
> only into the sequential path and measured **+0**. The driver
> (`cbm_pxc_perl_duck_prepass_driver`) must be called from the parallel pipeline
> (after `perl_inherit` is built, before `cbm_parallel_resolve`). Both paths now
> call it.

Result: `ua`→Mojo::UserAgent, `app`→Mojolicious, `server`→Mojo::Server::Daemon,
`res`→Mojo::Message::Response, all `perl_method_inherited`, all correct. +54.

---

## 5. The four-language real-repo audit (the GAP answer)

Before assuming "more Perl edges" was the goal, we measured **all four axes** on
real repos for the first time (fresh index; `CALLS`):

| axis | real repo | files | CALLS | notes |
|---|---|---|---|---|
| Java | gson | 264 | 9545 | rich LSP (`lsp_type_dispatch` 2636, `lsp_constructor_synth` 978…) — mature |
| Rust | ripgrep | 110 | 6690 | `suffix_match` 2715 (40 %) — heuristic-heavy |
| Python | Django | 2930 | 62084 | 22026 edges ≥ 0.9 conf; `lsp_method` 10830 — mature |
| Python | Flask | 83 | 1408 | = 794 resolved + 614 Flask-route edges (`callee/url_path/via` — a distinct route-edge category, not unresolved calls) |
| Perl | Mojolicious | ~274 | 4660 | this campaign |

**The load-bearing finding:** on every axis, the low-confidence heuristic edges
(`suffix_match` / `unique_name`, ~40 % of Django/Rust) are **external/stdlib
calls with no in-repo target** — sampled: Django `os.environ.get` (52
candidates, conf 0.02), `threading.Event`; ripgrep `std::env::current_dir`,
`io::Error::new`, `.push`, `.iter()`; gson `delegate.read` (49 candidates —
genuinely polymorphic), `.equals`/`.get`/`.put` (java.util). These are not
under-resolved in-repo calls; they have no in-repo target and cannot be soundly
resolved to one. `candidate_count_penalty` (`src/pipeline/registry.c`)
**deliberately** floors their confidence to ~3/count — the engine's design is
high-recall emission + confidence tagging, consumers threshold. So there is **no
additive coverage lever** on Java/Rust/Python: the unresolved tail is external
by construction.

### Measurement pitfalls hit during the audit (write these down)

- **`query_graph … RETURN r` overflows** on a 62k-edge result (daemon-backed CLI
  fails). Use `RETURN count(r)`, and `WHERE r.strategy='X' RETURN count(r)` /
  `WHERE r.confidence < 0.5 …` for breakdowns.
- **Restricted env breaks the temp-daemon spawn.** The CLI spawns a temporary
  daemon from `/proc/self/exe`; a stripped `env={PATH,ASAN_OPTIONS}` makes it
  fail. Pass the **full** `os.environ` + `ASAN_OPTIONS=detect_leaks=0`.
- **Perl edge callees are stored bare** (`"headers"`, not
  `"Mojo::Message::Request::headers"`), so you cannot grep resolved targets by
  class name from the `CALLS` JSON — count by site or by strategy instead.

---

## 6. Dead-ends built, measured, and reverted (do not re-walk)

Each was implemented cleanly and measured on real Mojolicious; each is kept out
of `main` for the stated reason.

### 6.1 Invocant-name extension → +0
Treat the enclosing sub's invocant (any name, e.g. `$c`, not just literal
`$self`/`$class`) as self-equivalent in `perl_duck_collect`. **+0** on the
framework source: Mojolicious's own lib/tests barely contain controller-*subclass*
actions (`package X; use Mojo::Base 'Mojolicious::Controller'; sub act { my
$c=shift; $c->req->… }`) — that pattern is **user-app** code. The framework's
`$c->req` chains live in `main`/test callbacks where `$c` is a controller
*passed into* the callback, not the enclosing sub's invocant, so attribution
correctly fails.

### 6.2 Per-scope local-variable duck-typing → +5, reverted
Type an untyped local `$c` from its per-scope method-set (same gate). Built
clean, gate 472/0, edges correct-on-Mojolicious — **but reverted.** A guarded
debug probe (1421 bind attempts logged) proved the gate **fires correctly**
(`$c`→Controller, `$t`→Test::Mojo, `$renderer`→Renderer…) yet **every qualifying
local is already typed** by existing assignment/invocant/routing inference, so
the sound mechanism nets 0. The measured +5 came only from a class-seed
activating the pre-existing blanket `my $c=shift`→Controller binding — i.e. the
gain was **not sound-by-construction** (it leans on a binding that could mistype
`$c` in a non-Mojo repo). Held out on discipline grounds.

### 6.3 `$tx`→Mojo::Transaction per-scope → +0 (and a measurement lesson)
The `$tx->req`/`$tx->res` chains (648 + 421 sites) are the largest unresolved
block. A `grep`/`sub`-split estimate suggested "37 scopes" where a
Transaction-*distinctive* method (`connection`/`keep_alive`/`result`/… — none on
Controller) would uniquely type `$tx`→Mojo::Transaction. **The estimate was a
regex artifact** (crude sub-splitting + POD examples). Real tree-sitter `$tx`
scopes carry only ~2 distinct methods (`{kept_alive,res}`, `{on,send}`, …) —
below `PERL_DUCK_MIN` — and `$tx` is already typed where it clears. **+0.**
**Lesson: validate scope counts with the real parser, never a line-regex.**

### 6.4 Why the big chains are structurally unresolvable
`$tx` is sourced from typeless / statically-invisible producers: `$t->tx` (104),
`$ua->get`/`post`/… (monkey-patched, ~50), `$self->tx` (typeless accessor); only
`Mojo::Transaction::HTTP->new` (16, already resolved) and `build_tx` (~29, needs
intra-sub return-var tracking, previously 0-yield) are typed sources. Closing
the rest would require either unsound receiver assumptions or resolving
monkey-patched methods — both violate the correct-edge guarantee.

---

## 7. Conclusion & the standing discipline

The sound **additive** frontier is exhausted at 4660 (+110%). Remaining
unresolved edges are a **structural limit of sound static analysis** — typeless
polymorphic accessors (`tx`), monkey-patched methods (`$ua->get`), polymorphic
dispatch (gson `delegate.read` across 49 impls) — not missing features. The
other three axes are mature with an external-by-construction unresolved tail.

**Precedent set (2026-09-10):** offered the one remaining measurable increment
— the §6.2 `$c` +5 — the maintainer chose to **hold the sound-by-construction
discipline** and reject it. A correct-on-target increment whose *mechanism* is
not sound-by-construction is rejected: **the engine's soundness outranks
marginal edge count.** That is the rule for future iterations here.

Anything past this point needs a real Mojo **application** repo (rich
controller-action `$c` scopes) as the measurement target, or a precision pass
(re-tiering the external heuristic edges) — which is count-neutral/negative and
a separate, maintainer-gated decision.
