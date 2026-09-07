# Perl cross-file inheritance resolution — design, fixes & troubleshooting

Status: foundation shipped to `main` (`30503073`); multi-level dispatch on branch
`worktree-agent-a66a82c759ba61fe8` (this document ships with it).

This note records how Perl inherited-method resolution across files was diagnosed
and fixed, the adversarial-review course-corrections, and the measurement
methodology — so the next iteration (and the equivalent work in other languages)
does not re-walk the same dead ends.

---

## 1. The gap (found by measuring a real repo, not fixtures)

Indexed a real 274-file **Mojolicious** checkout with the production binary and
read the edge histogram via `get_graph_schema` (the Cypher subset rejects
`labels(n)[0]`, multi-column aggregates, and 2-variable `WHERE`, so it is the
wrong tool for edge/label counts):

| repo (files)          | CALLS | INHERITS | DEFINES_METHOD |
|-----------------------|-------|----------|----------------|
| Perl Mojolicious (274)| 2218  | **0**    | 296            |
| Java gson (264)       | 9551  | 131      | 2983           |
| Python flask (83)     | 1408  | 32       | 313            |
| Rust ripgrep (110)    | 6690  | 0¹       | 2153           |

¹ Rust has no inheritance (traits → IMPLEMENTS=142); 0 is correct there.

Perl resolved ~4× fewer CALLS than Java at a similar file count **and emitted
zero INHERITS** where every OO language shows 100+. Two root causes, both
invisible to the synthetic test-suite (which used `use parent`, not the framework
idiom):

1. **`use Mojo::Base 'Parent'` was not recognised as inheritance.** It is the
   dominant modern Perl idiom (the entire Mojolicious ecosystem). Because the
   used module name is `Mojo::Base`, it fell through to Exporter-import handling
   and the quoted parent-class string was treated as an *import*, never `@ISA`.
2. **Cross-file inherited method calls never resolved.** Even for `use parent`,
   `$self->inherited` where the parent lives in another file produced no CALLS
   edge. Mojolicious is one class per file, so this is nearly all real-world
   inheritance.

---

## 2. Foundation fix (shipped in `30503073`)

Three coordinated changes gave **one level** of cross-file inheritance:

- **Recognise `Mojo::Base`** (`perl_lsp.c` `perl_collect_use_statement`): quoted
  string arg → `@ISA` parent; `-base` → inherits `Mojo::Base` itself; other flags
  (`-role`/`-strict`/`-signatures`/…) contribute nothing. Verified against the
  actual tree-sitter AST (parents are `string_literal`, flags are
  `autoquoted_bareword`, args may sit in a `list_expression`).
- **Emit parents as import rows** (`extract_imports.c`
  `perl_collect_inheritance_imports`): `use parent/base/Mojo::Base 'X'` each do a
  compile-time `require` of the parent, so an IMPORTS edge is correct — and it
  lands the parent module in the per-file import map, which the cross-file LSP
  **def filter** (`pass_lsp_cross.c` `cbm_pxc_filter_defs_for_file`) uses to keep
  a module's defs. An `@ISA` parent is a cross-file *dependency* but was not
  previously an *import*, so its defs were filtered out and inherited calls could
  never resolve.
- **Attach the parent's cross-file subs to its type** (`perl_lsp.c`
  `cbm_run_perl_lsp_cross`): the cross pass had registered an `@ISA` parent as a
  bare type with no method table (its subs live in another file, indexed only as
  standalone Functions), so `$self->inherited` could not dispatch up the chain.

### Course-correction that this required (troubleshooting record)

- **The daemon cache masked every measurement.** `index_repository` is
  incremental: unchanged source files serve the *cached* graph even under a new
  binary, so re-indexing the same path showed byte-identical edge counts and made
  a working fix look inert. **Fix: always measure on a fresh copied path**
  (`cp -r repo newdir` then index `newdir`). This single gotcha cost the most
  time; it is now the first rule of the measurement harness.
- **"Exempt Perl from the def filter" (Option A) REGRESSED the real repo.**
  The first attempt widened resolution to the full def universe (like Rust's
  cross-crate exemption). On a fresh Mojolicious index it dropped CALLS 2218→2216
  by widening same-name ambiguity, with no offsetting gain. It was reverted in
  favour of the targeted parent-as-import path (Option B) above, which keeps the
  filter tight. Lesson: a real-repo delta, not a passing fixture, is the gate.
- **`SEMANTICALLY_RELATED` is non-deterministic** (107 vs 96 vs 67 across
  identical fresh indexes) — never treat its count as a regression signal.

---

## 3. Multi-level dispatch (this branch)

Mojolicious inheritance depth: of 95 parented packages, **91 are multi-level**
(depth-2=64, depth-3=16, depth-4=3). One-level resolves the immediate parent's
own methods (covers `$self->render` defined directly in `Mojolicious::Controller`)
but misses grandparent methods (`Mojo::EventEmitter->emit`, …) — most real calls.

### Adversarial review saved a broken design

The first multi-level design routed parent chains through
`def.base_classes → embedded_types`. A senior adversarial review (对拍) returned
**NO-GO** with three independent fatal flaws:

1. `pxc_map_label` drops `Module`-labeled defs *before* the
   `base_classes → embedded_types` join — Perl package defs never even enter
   `all_defs`.
2. Extraction emits **no per-package Perl def** to hang `base_classes` on — only
   one file-level Module def aggregating all packages.
3. The Perl registrar never reads `CBMLSPDef.embedded_types` anyway.

Plus a **cross-language false-edge** hazard: `pass_semantic` resolves bases by
short name with no language scoping, so a Perl `use parent 'Animal'` plus any
Python `class Animal` would mint a bogus INHERITS edge. And a **use-after-realloc**
footgun in a naive chain-walk (`cbm_registry_add_type` reallocs `reg.types`).

### The approved, Perl-isolated design (implemented here)

Zero changes to shared, cross-language passes (`pass_semantic`, `pxc_map_label`,
extraction `base_classes`):

- **`CBMFileResult.perl_isa_parents`** (`cbm.h`): a file's TAGGED `@ISA` parent
  spellings, collected in `extract_imports.c` **only** from inheritance `use`
  statements — an ordinary `use Foo` never appears here (zero-edge guarantee).
- **`CBMPerlInheritIndex`** (`pass_lsp_cross.h`): a project-wide
  `module_qn → [parent module_qns]` map assembled in `pipeline.c`
  (`cbm_perl_build_inherit_index`) from the per-file caches, threaded into
  `cbm_run_perl_lsp_cross`, and freed after `cbm_parallel_resolve` returns.
- **Bounded worklist chain-walk** (`perl_lsp.c` `cbm_run_perl_lsp_cross`): BFS
  over ancestors (`PERL_CHAIN_CAP=256`, `seen` dedup). For each ancestor: resolve
  to a module over the FULL `all_defs`, attach its Function/Method defs to its
  type, look up *its* tagged parents in the index (push unseen), and seed the
  ancestor type's `embedded_types` so the existing `perl_lookup_method` recursion
  walks the rest of the chain.
- **Realloc-safe**: `perl_type_set_methods` (which may realloc `reg.types`) is
  called first, then the type is **re-found by name** before its `embedded_types`
  is written — never a pointer held across the realloc.

### Validation

`scratchpad/probe3lvl` — three files `Dog → Animal → Base`, where `Dog::bark`
calls both `$self->speak` (immediate parent Animal) and `$self->root_method`
(grandparent Base). Baseline binary resolved only the immediate-parent edge; the
multi-level binary resolves **both** on a fresh path:

```
<P>.lib.Dog.bark  ->  <P>.lib.Animal.speak       # immediate parent
<P>.lib.Dog.bark  ->  <P>.lib.Base.root_method   # grandparent (multi-level)
```

Unit test: `perllsp_cross_multilevel_inherited_method` (in `test_perl_lsp.c`).

### Known v1 limitation

`perl_isa_parents` is collected per *file*, not per *package*, so a rare
multi-package file (`package A; use parent 'X'; package B; use parent 'Y';`)
over-approximates (A may see Y). Perl is ~one package per file in practice
(all of Mojolicious), so this is acceptable; per-package tagging is a follow-up.

### Honest real-repo aggregate: no CALLS delta on Mojolicious — receiver typing is the next lever

Multi-level resolves grandparent calls on constructed cross-file chains
(`probe3lvl`), but on the real Mojolicious checkout it adds **0 net CALLS**
(2216 → 2216). The mechanism is correct; the aggregate is flat because the
**dominant limiter on this repo is receiver typing, not chain depth**. A grand-
parent method call only resolves when the receiver is typed to a concrete class
(`my $self = shift`), the full ancestor chain is in-repo, and each hop resolves
unambiguously. In practice most Mojolicious method calls are on parameters whose
class is never inferred (`$c->render`, `$tx->res`, …), so they don't resolve at
*any* depth — and the ones that do are mostly same-package or immediate-parent.
Chain depth was a real correctness gap (grandparent calls previously *could not*
resolve); closing it is necessary but not sufficient. **The next Perl real-repo
lever is receiver typing.**

Receiver census over Mojolicious `lib/*.pm` (6782 method-call sites):

| receiver kind                     | sites | share |
|-----------------------------------|-------|-------|
| untyped `$var` (`$c`,`$tx`,`$ua`…)| 4611  | 68%   |
| `$self` (invocant-typed)          | 1333  | 20%   |
| `Class::` (static)                | 838   | 12%   |

Resolved CALLS (~2216) ≈ the `$self` + static calls; the 68% untyped-receiver
calls don't resolve at *any* inheritance depth. Top untyped receivers are
idiomatic framework objects with predictable types: `$c`→Mojolicious::Controller
(458), `$tx`→Mojo::Transaction (287), `$ua`→Mojo::UserAgent (180),
`$headers`→Mojo::Headers (220), `$app`→Mojolicious (225), `$dom`→Mojo::DOM (179).

**The type-inference infrastructure already exists** and is not the blocker:
`perl_process_assignment` (perl_lsp.c) binds `my $x = RHS` to
`perl_eval_expr_type(RHS)`, which for a method call returns the callee's
`signature->return_types[0]`, and chained calls (`$self->engine->start`) already
type off that. The missing input is **accessor return types**: Perl subs declare
no return type syntactically, so `sub headers {...}` has no signature the registry
can propagate. Two tractable fills, in axis order:
- *symbol-table*: a curated Mojo-ecosystem accessor→return-type table
  (`Mojo::Message::headers → Mojo::Headers`, `Mojo::UserAgent::build_tx →
  Mojo::Transaction`, …) — like the Mojo::Base idiom fix, high value on the whole
  ecosystem, and it feeds the existing assignment/return-propagation path so
  `my $headers = $msg->headers; $headers->add(...)` resolves.
- *engine*: return-type inference from accessor bodies (`return $self->{x}` /
  `has x => ...`) for a repo's *own* classes, generalising beyond curated tables.

---

## 4. Measurement harness (reusable)

1. **Fresh path every time** — `cp -r <repo> <newdir>`; index `<newdir>`; the
   daemon cache keys on path and will otherwise serve a stale graph.
2. **`get_graph_schema` for edge/label histograms**, not Cypher.
3. **Minimal probes are unrepresentative** — a single-file Rust chain gave 0
   CALLS; real-repo CALLS are internal-call-dominated. Validate mechanisms with
   small cross-file probes, but judge impact on a real repo.
4. **`ASAN_OPTIONS=detect_leaks=0`** for CLI queries against sanitized builds.
5. Build discipline: `test-focused` is ONE giant `cc` reading ALL sources — never
   edit a source file while it runs. Launch long builds detached
   (`setsid … ; echo MARK_EXIT:$? >> log`) so they survive session teardown.

---

## 5. Session-continuity note

The multi-level implementation was produced in an isolated git worktree that was
terminated mid-run by an auth expiry while it was still adding debug
instrumentation. Recovery: the worktree diff was reviewed against the adversarial
findings, three `fopen("/tmp/ml_dbg.txt")` debug blocks were removed from
`perl_lsp.c`, the grandparent edge was re-validated on `probe3lvl`, and the
focused suites were re-run before merge. Uncommitted scratch directories
(`mojo_ml/`, `pbig*/`, `pf_*/`, `*.txt` query dumps) are build/measurement
artifacts and are not committed.
