# Resolver Excellence Playbook

**Purpose.** Reverse-engineer the reasoning patterns ("chain-of-thought") that make codebase-memory-mcp's strongest per-language Hybrid LSP resolvers excellent, and hand engineers uplifting the weaker ones (Perl, Python 2+3, Rust) a portable recipe they can apply directly. Companion to `PLAN.md` (adjudicated per-item scope) and `SOP.md` (iteration harness). PLAN.md tells you *what* to build; this tells you *why the strong resolvers reason the way they do* and *which pattern to copy*.

**Scope.** Strongest resolvers mined: `ts_lsp.c` (6042 ln, TS/JS/JSX — richest expression engine), `c_lsp.c` (6130 ln — richest overload/ADL/neg-memo), `rust_lsp.c` (6585 ln — richest trait/UFCS + shared neg-memo), `kotlin_lsp.c` (5494), `cs_lsp.c` (3837). Shared machinery: `type_rep.{c,h}`, `type_registry.{c,h}`, `scope.{c,h}`, `lsp_neg_memo.h`, `lsp_node_iter.h`. Integration: `src/pipeline/lsp_surface.c`, `src/pipeline/pass_lsp_cross.c`. Weak targets: `perl_lsp.c` (1883 ln, **no cross-file at all**), `py_lsp.c` (5308 ln, strong core but thin on perf-memo/framework/narrowing), `rust_lsp.c` stdlib table (`generated/rust_stdlib_data.c`, 1794 ln vs go's 30 630).

> **Citation discipline.** Every `file:line` below was grep/read-confirmed against the working tree on the audit date. PLAN.md's own anchors have already drifted (it cites `cbm_run_perl_lsp` at :1638; it is now `perl_lsp.c:1804`). **Re-grep before you cut code** — resolvers move constantly.

---

## 0. Shared vocabulary primer (read once, referenced everywhere)

Every strong resolver is a thin language-specific shell over four shared abstractions. Master these before the 7 axes.

### 0.1 `CBMType` — the type representation (`type_rep.h:9-149`)
A tagged union with **30 kinds** (`CBMTypeKind`, `type_rep.h:9-43`). The vocabulary is the ceiling on how deep any resolver can reason:
- **Universal:** `UNKNOWN`(0), `NAMED`, `POINTER`, `SLICE`, `MAP`, `FUNC`, `INTERFACE`, `STRUCT`, `BUILTIN`, `TUPLE` (multi-return), `TYPE_PARAM` (generics: `T`,`K`,`V`), `TEMPLATE` (`vector<T>`, `Array<T>`, `Promise<T>`), `ALIAS`.
- **Python-flavored:** `UNION` (`A | B`, sorted-canonical, shared with TS), `LITERAL`, `PROTOCOL` (structural), `MODULE`, `CALLABLE`.
- **TS-specific:** `INTERSECTION`, `TS_LITERAL`, `INDEXED` (`T[K]`), `KEYOF`, `TYPEOF_QUERY`, `CONDITIONAL` (`T extends U ? X : Y`), `OBJECT_LIT`, `INFER`, `MAPPED`.
- **C++:** `REFERENCE`, `RVALUE_REF`.

Constructors are arena-allocated (`cbm_type_named`, `cbm_type_template`, `cbm_type_union`, …, `type_rep.h:152-205`). The load-bearing operations for chaining are **`cbm_type_substitute`** (generic param → concrete, `type_rep.h:235`), **`cbm_type_resolve_alias`** (16-level cycle-guarded, `:230`), and **`cbm_type_deref`/`cbm_type_elem`** (`:208-209`).

### 0.2 `CBMTypeRegistry` — cross-file symbol store (`type_registry.h:87-155`)
Two arrays (`funcs`, `types`) + lazy hash indexes (`cbm_registry_finalize`, `:165`). Key excellence properties:
- **`CBMRegisteredFunc`** (`:29-43`) carries `receiver_type` (NULL ⇒ free function; non-NULL ⇒ method on a type — *this is what makes OO chains resolve*), `signature` (a `FUNC` `CBMType` with real param+return types — *this is what makes `.b().c()` chains resolve*), `type_param_names`, `flags` (`CBM_FUNC_FLAG_*`), `impl_trait_qn` (Rust).
- **`CBMRegisteredType`** (`:46-76`) carries `field_names/field_types`, `method_names/method_qns`, `embedded_types` (base/embedded QNs), `alias_of`, `type_param_names`, `is_interface`, `is_stdlib`, `from_test_file`, plus TS `call_signature`/`index_*`.
- **Tier-2 chaining:** `fallback` pointer (`:98-103`) — a small per-file overlay registry chains to a shared immutable base. **`read_only`** seal (`:146-154`) — set at finalize; `cbm_registry_add_*` hard-return on a sealed registry. This is both a correctness (no data race across parallel workers) and perf (no post-finalize linear-scan tail) invariant.
- **Overload-aware lookups:** `cbm_registry_lookup_method_by_types` (scores overloads by arg-type match, `:225`), `_by_args` (`:214`), `_lookup_method_aliased` (`:208`), plus allocation-free auxiliary iterators (`CBMMethodIter`, `CBMTypeShortIter`, `CBMFreeFuncIter`, `:248-311`).

### 0.3 `CBMScope` — lexical binding frames (`scope.h:9-95`, `scope.c`)
Chunked (`CBM_SCOPE_CHUNK_BINDINGS 16`) parent-linked frames, arena-owned. Beyond ordinary `type`, each binding carries a **`callable_qn`** identity (`scope.h:16`) — the exact QN of a callable value the binding references, kept *separate from* the CBMType so `const f = foo; f()` resolves `f→foo`. Two disciplines matter:
- **Fail-closed binds:** `cbm_scope_bind_checked`/`_bind_callable_checked` return `false` on arena exhaustion (`scope.c:77-89`). The void forms discard that — and a caller who then does a *chain* lookup would see a **parent** binding of the same name and fabricate a shadow that never took effect (`scope.h:70-83`). Callable-proof paths must use the checked form and read the local result.
- **Shadow-correct callable lookup:** `cbm_scope_lookup_callable` returns NULL when a nearer *ordinary* binding shadows a parent's callable (`scope.c:123-137`) — reassignment fails closed instead of leaking a stale alias.

### 0.4 Depth/step caps (the perf-sacred constants)
- `CBM_LSP_MAX_LOOKUP_DEPTH 16` (`scope.h:36`) — alias/MRO/embedded-field traversal bail-to-UNKNOWN.
- `CBM_LSP_MAX_WALK_DEPTH 512` (`scope.h:45`) + `cbm_lsp_max_walk_depth()` (`scope.h:55-65`, env-overridable, relaxed-atomic cached — never `getenv` on the hot path).
- Per-resolver eval caps: see §1 and §5.

### 0.5 The emission surface — `CBMResolvedCall` (`cbm.h:392-402`)
Every resolver's *only* output is a push of `{caller_qn, callee_qn, strategy, confidence, reason, kind, site bytes, source_origin}`. `kind` is `CBM_RESOLVED_INVOCATION` (a CALLS edge) or `CBM_RESOLVED_CALL_REFERENCE` (an explicit callable reference, e.g. passing `foo` by name). See §4.

### 0.6 The `CBMLSPDef` surface — cross-file def interchange (`lsp_surface.c`)
The serialized per-file def record that feeds Tier-2 registries. Fields (codec at `lsp_surface.c:87-104`): `qualified_name`, `short_name`, `label`, `receiver_type`, `def_module_qn`, `return_types`, `embedded_types` (pipe-joined), `field_defs` (`"name:type|name:type"`), `method_names_str`, `signature_param_types[]`, `is_interface`, `lang`, `namespace_name`, `trait_qn`, `is_rust_impl_relation`, `is_abstract`, `from_test_file`, `decorators[]`. Round-trip fidelity + canonical bytes are invariants (`lsp_surface.c:1-27`): the SHA over these bytes is the incremental early-cutoff key.

---

## 1. Type propagation depth

**The engine: one recursive `<lang>_eval_expr_type(ctx, node) → const CBMType*`.** Every strong resolver has exactly one, and it is the spine that makes chained calls resolve. Analogues confirmed:
- **TS: `ts_eval_expr_type` (`ts_lsp.c:2079`)** — the richest.
- **C/C++: `c_eval_expr_type` (`c_lsp.c:1479`) → `c_eval_expr_type_inner` (`c_lsp.c:1498`)**.
- **Rust: `rust_eval_expr_type` (`rust_lsp.c:1459`)** + typed variant `rust_eval_expr_typed(node, expected)` (`:2251`).
- **Py: `py_eval_expr_type` (`py_lsp.c:151`) → `py_eval_expr_type_uncached` (`:1443`)** — already strong.
- **Perl: `perl_eval_expr_type` (`perl_lsp.c:463`)** + `perl_eval_method_call_type`/`_function_call_type`/`_new_type`/`perl_eval_bless` — present but **bless-centric and shallow**.

### 1.1 How chained calls (`a.b().c()`) resolve — the two-node loop
TS is the template (`ts_eval_expr_type`, dispatch on `ts_node_type`):
1. **`member_expression`/`subscript_expression` (`ts_lsp.c:2163-2172`):** recursively `ts_eval_expr_type(object)` → `recv`, then `lookup_member_type(ctx, recv, pname)`. This is the "get the type of the receiver, then look the member up on it" step.
2. **`call_expression` (`ts_lsp.c:2188-2199`):** `ts_signature_for_call(ctx, fn, call_args)` → if `FUNC`, `return_type_of(arena, fn_type)` (`:2062`, collapses single-element return arrays, builds a `TUPLE` for multi-return).

`ts_signature_for_call` (`ts_lsp.c:2005`) is where the receiver-typed method lookup happens: for a `member_expression` fn it evaluates the `object` **once** (`:2031`), calls `ts_lookup_method_for_call` + `ts_method_signature_for_receiver`, and falls back to `lookup_member_type` for callable fields (`:2036`) — *"Reuse the receiver already evaluated above so deep fluent chains remain linear."* Chaining depth is therefore bounded only by the caps in §1.4.

`lookup_member_type_inner` (`ts_lsp.c:1581`) is the member engine and shows the type-vocabulary payoff:
- `BUILTIN` → delegate to wrapper class (`string`→`String`) then recurse (`:1592-1600`).
- **`TEMPLATE` → registry lookup on template name, then `cbm_type_substitute(field/method sig, type_param_names, template_args)` (`:1602-1633`)** — this is how `Array<Foo>.pop()` yields `Foo`, `Promise<T>` unwraps, generic containers chain correctly.
- `OBJECT_LIT` → prop scan (`:1636-1645`); `UNION` → first branch with the member (`:1647`).

**→ apply to Perl/Py/Rust:** Perl's `perl_eval_expr_type` only understands `bless`/`->new`; it has no `member_expression→lookup_member_type` recursion because Perl OO chains (`$obj->a->b`) aren't modeled past one hop. Port the **two-node loop** (eval-object → lookup-member; eval-signature → return_type_of) with `receiver_type`-keyed method lookup. Rust already has the loop (`rust_eval_expr_type` method_call path) but its *stdlib signatures lack return types* (§7), so the chain dies at hop 1 for `Vec::iter().map().collect()`. Py has the loop; verify `cbm_type_substitute` is applied on generic container members the way `ts_lsp.c:1612` does.

### 1.2 Generics / templates / polymorphic returns
Two TS mechanisms, both single-pass and argument-driven:
- **Polymorphic `this` return (`ts_lsp.c:2201-2215`):** when a method's registered return is `TYPE_PARAM "this"` and the fn is a `member_expression`, substitute the *actual receiver type* from the call site → fluent-builder patterns (`.setX().setY()` keep returning the concrete builder).
- **Call-site generic inference (`ts_lsp.c:2217-2256`):** *"the simplest form of typescript-go's inferTypeArguments — single-pass, argument-driven."* Walk params; where a param is `TYPE_PARAM`, evaluate the concrete arg, collect `(param-name → arg-type)`, then `cbm_type_substitute` into the return. Bounded to 8 inferred params (`inf_names[8]`).
- **`await` unwrap (`ts_lsp.c:2267-2277`):** `Promise<T>` (TEMPLATE arg 0) → `T`.

**→ apply to Perl/Py/Rust:** Rust's biggest chaining win is applying `cbm_type_substitute` to iterator-adapter returns from the stdlib table (make `Iterator<Item=T>::map` return `Map<...,Item=U>` at least as `Iterator<Item=U>`). Py: user-generic classes — ensure `type_param_names` on `CBMRegisteredType` are populated so container returns substitute. Perl: no generics; skip — invest the eval budget in OO-chain depth instead.

### 1.3 Unions / narrowing / literals (the "give a useful answer, not a wrong one" kinds)
- **Ternary (`ts_lsp.c:2342-2358`):** `union(a, b)` with UNKNOWN-collapse (if one branch is UNKNOWN, return the other, never a polluted union).
- **Binary `+` (`ts_lsp.c:2359-2374`):** string if either operand is string else number — cheap, correct-enough.
- **Object literal (`ts_lsp.c:2302-2341`):** captures string-keyed prop types into `OBJECT_LIT` so downstream member lookups on locals succeed.
- Flow-sensitive narrowing (`typeof`/`instanceof`) lives in the scope layer — see §2.

**→ apply to Perl/Py/Rust:** Py: model `x if c else y` as `UNION` with UNKNOWN-collapse (mirror `ts_lsp.c:2349-2357`) so attribute lookups on conditionally-typed locals still resolve one branch. Perl: model `$x || $default` like the ternary.

### 1.4 Eval-step caps — how deep before giving up (perf-sacred; see also §5)
Depth alone does **not** bound work — crafted fan-out stays under the depth cap while running for seconds. Every strong engine pairs a **depth cap** with a **per-file work budget**, and TS adds a **positive memo**:
| resolver | depth cap | work budget | positive eval memo |
|---|---|---|---|
| TS | `TS_LSP_MAX_EVAL_DEPTH 64` (`ts_lsp.c:174`, checked `:2089`); `TS_LSP_MAX_MEMBER_DEPTH 64` (`:1565`, checked `:1573`) | `g_ts_type_budget`, **−16 per eval entry** (`:2098-2114`), degrade-to-UNKNOWN + warn-once | **yes** — `ts_memo_get`/`ts_memo_put` by `node.id` (`:2085`, `:2382`); stores only *non-degraded* results (`g_ts_eval_degraded` guard, `:2116-2118`, `:2381`) |
| C | `C_EVAL_DEPTH_LIMIT 256` + `C_EVAL_MAX_STEPS_PER_FILE 10000` — **both** checked in one guard (`c_lsp.c:1486`) | step counter | no |
| Rust | eval recursion + `CBM_RUST_EVAL_STEP_CAP 200000` per file (`rust_lsp.c:4632`, checked in `rust_resolve_calls_in_node` `:4638`) | step counter | yes (`_memo`, 13 sites) |
| Py | `PY_LSP_MAX_EVAL_DEPTH 256` (`py_lsp.c:30`) | budget | yes (`py_eval_expr_type` memoizes, `:151-173`) |
| Perl | `PERL_EVAL_MAX_DEPTH` (recursion guard, `perl_lsp.c:467`) | **none** | **none** |

The TS memo contract is the gold pattern (`ts_lsp.c:2082-2088`): *"Memo hit: O(1), charges no budget, ignores depth — the value came from a completed, non-degraded evaluation of this exact node."* The **degraded-guard** (never memoize a result that hit a cap) prevents caching a wrong-because-truncated answer.

**→ apply to Perl/Py/Rust:** Perl: add a per-file work budget + a `node.id` positive memo before deepening the eval engine — otherwise a deeper engine becomes a DoS. Rust: already capped; fine. Py: fine.

### 1.5 Gap scorecard — Type propagation (5 = gold `ts_lsp.c`)
| target | score | single highest-leverage fix |
|---|---|---|
| **Perl** | **2** | Add the `member_expression→lookup_member_type` two-node chain loop with `receiver_type`-keyed lookup (mirror `ts_lsp.c:2163-2199` + `2005-2036`) so `$obj->a->b` chains; gate it behind a work budget + `node.id` memo (mirror `ts_lsp.c:2082-2114`). |
| **Python** | **4** | Apply `cbm_type_substitute` on generic-container member returns (mirror `ts_lsp.c:1612-1627`) and model ternary as UNKNOWN-collapsing `UNION` (`ts_lsp.c:2349-2357`). |
| **Rust** | **3** | The engine is fine; the *fuel* is empty — give stdlib iterator/`Option`/`Result` methods real return types (§7) so `rust_eval_expr_type`'s method path can chain past hop 1. |

---

## 2. Scope & binding discipline

**One mutator, two identity fields, one fail-closed rule.** All binding funnels through `cbm_scope_bind_value` (`scope.c:45`); it returns `false` on arena exhaustion — *"the shadow did NOT take effect"* (`scope.c:63`) — and the checked wrappers exist so a failed child bind is never mistaken for an inherited parent shadow (`scope.h:70-93`). `callable_qn` is identity metadata on the binding (`scope.h:16`); a nearer ordinary bind clears it (`scope.c:54`) so `cbm_scope_lookup_callable` returns NULL when shadowed (`scope.c:123-137`).

### 2.1 Where things bind during the walk
Each strong resolver pushes a frame per function body and per block, binds params, then binds locals as it descends:
- **TS:** root `cbm_scope_push(...,NULL)` (`ts_lsp.c:3795`); `process_function_body` push (`:3642`) + `bind_parameter` loop (`:3655`) with a JSDoc/signature param-type fallback (`:3672-3675`); `let/const/var` at the `variable_declarator` handler (`:2493`); **destructuring** — `object_pattern` shorthand (`:2505`) / pair via `lookup_member_type` (`:2514`), `array_pattern` tuple/template element (`:2539`).
- **Rust:** all pattern binding funnels through one recursive **`rust_bind_pattern` (`rust_lsp.c:3794`)** covering identifier/ref/mut/tuple/tuple-struct/struct patterns (`:3799-3895`); `let` → `:3943`, `const`/`static` → `:3997-4005`.
- **C / Kotlin:** C uses **explicit `cbm_scope_pop`** for blocks (push `c_lsp.c:4684`, pop `:4752` — the *only* resolver popping blocks explicitly); Kotlin uses **balanced push/pop pairs** for every block construct (e.g. `kotlin_lsp.c:3662/3672`, `4246/4343`) and injects primary-constructor fields into the method scope, skipping ones a param already shadows (`:4254-4258`).
- **The TS/Rust scope-restore idiom:** neither TS nor Rust calls `cbm_scope_pop` at all — they save `CBMScope *saved` and restore by assignment `ctx->current_scope = saved` (`ts_lsp.c:2984,3286,3519,3564,3701`). Either idiom is fine; pick one and be consistent.

### 2.2 Flow-sensitive narrowing (the "smarter than the declared type" layer)
- **TS `extract_narrowing` (`ts_lsp.c:2992`):** `x instanceof Foo` (`:3017`), `typeof x === 'string'` / `!==` with polarity via `out_inverted` (`:3030-3057`), `narrow_discriminated_union` for `x.kind === 'lit'` (`:3070`). The `if_statement` branch pushes a child scope and binds the narrowed var for the truthy branch (`:3516-3517`) or the `else` when inverted (`:3530-3531`); `switch(x.kind)` narrows per case (`:3406-3465`).
- **Rust:** `if let`/`while let` bind via `rust_bind_pattern` (`:4852-4858`); modern `let_chain` shape (`:4860-4889`); `match_arm` is a deliberate best-effort no-op (`:4924-4928`) relying on the per-arm scope push + descent.
- **Py (already good):** `py_walk_if_statement` (`:3037`) → `isinstance` narrow into the consequence scope (`:2847,3092-3093`), `x is None`/`is not None` via `py_strip_none` (`:2908/2965,3098-3103`), **early-return narrowing** applies the positive narrow to the enclosing scope after a terminating guard (`:3117-3130`), PEP 634 `match`/`case` subject narrowing (`:3267-3327`). Documented v1 gap: `else`-branch negation not modeled (`:3111-3112`).

### 2.3 self / this / receiver
`this`→`NAMED(class_qn)` (TS `:3648`); `this`→`pointer(NAMED(class_qn))` (C++ `:4906`); `self`→`NAMED(self_type)` wrapped in `reference` for `&self` (Rust `:5024-5032`); `this`→`ctx->this_type` (Kotlin `:4221`); **`self` *and* `cls`→`NAMED(class_qn)` bound *after* the param walk so the receiver type beats an unannotated param** (Py `:4093-4099`); Perl invocant three ways — signature `sub m($self,…)` (`perl_lsp.c:1162`), classic `my ($self,…)=@_`/`=shift` (`:1107,1127`), Corinna `method` implicit `$self` (`:1189`).

### 2.4 Closures / lambdas — contextual param typing
The excellence move is typing a callback's params from the *expected* signature, not from the callback itself:
- **TS `process_callback_arrow` (`ts_lsp.c:2896`):** contextually types arrow params from the expected `FUNC`'s `param_types` (`:2916-2921` single, `:2962` multi) — `arr.map(x => …)` gives `x` the element type.
- **Rust `closure_expression` (`:4941`):** binds params by priority — explicit annotation > **`ctx->pending_closure_param_type`** hint stashed by the iterator-method resolver for `.map(|x| …)` (`:4649-4692`, consumed `:4942`) > unknown; the hint is cleared immediately so it can't leak into a sibling closure (`:4943`).
- **C++ init-captures** `[name = expr]` bound only when the captured type is known (fail-closed, `c_lsp.c:4728-4729`).

### 2.5 The `callable_qn` alias mechanism (shared strategies `lsp_callable_alias` / `lsp_callable_value_reference`)
- **Rust (the reference):** `let f = foo;` copies `callable_qn` from an in-scope binding (`rust_lsp.c:3952`) or resolves the path to a registered non-receiver function (`:3954-3969`) then `cbm_scope_bind_callable` (`:3972`); reassignment → `cbm_scope_update_callable` (`:3990`) **guarded by `callable_control_flow_depth == 0`** (`:3987`) so a conditional rebind can't leak; call-site dispatch `cbm_scope_lookup_callable` → `lsp_callable_alias` (`:4412-4414`).
- **Kotlin:** `val f = ::foo` → `cbm_scope_bind_callable` (`:3512`); dispatch → `lsp_callable_alias` (`:2931-2934`).
- **Py (verify-then-latch):** wrapper `py_scope_bind_callable` (`:199`) binds then **reads back and disables proof on any mismatch** (`:203-206`).
- **C:** `c_emit_resolved_reference_at` picks `lsp_callable_alias` vs `lsp_callable_value` by comparing source name to target leaf (`c_lsp.c:3842-3843`).
- **TS is the exception:** it does *not* use `cbm_scope_bind_callable` — it emits value-reference edges at the argument occurrence (`resolve_value_references_at`, `ts_lsp.c:2730`, strategies `lsp_ts_*_value_reference`), gated on the name *not* being lexically shadowed.

### 2.6 The zero-edge guarantee, seen from the scope layer
The discipline is uniform: a name that is lexically shadowed, ambiguous, or of unknown type produces **no edge**, never a guess. TS: shadowed names stay USAGE (`ts_lsp.c:1426,1437`, `ts_block_shadowed_reference_usage` `:2781-2782`), ambiguous imports fail closed (`:1927,1960`). Rust: conditional reassignment clears the alias (`callable_control_flow_depth++` `:4844`), operators sound-only *"no edge unless proven"* (`:4705`). Kotlin: *"fails closed instead of borrowing a same-named function"* (`:2927-2930`), refuses to *"fabricate a CALL_REFERENCE"* (`:2780,3230`). **Py's model to copy: a single latch `py_disable_callable_value_proof` (`:187`) tripped from ~20 sites** — allocation failure (*"reduce precision, never fabricate it"* `:113`), decorated defs, duplicate-QN `AMBIGUOUS_BINDING` groups (`:105-134`), and every unverifiable scope mutation.

**→ apply to Perl/Py/Rust:** Perl uses `CBMScope` only *skeletally* — **2** pushes (root `perl_lsp.c:229`, per-sub `:1169`), **zero** pops, **zero** `cbm_scope_contains`, **one** `cbm_scope_lookup` (`:479`); every `my` collapses into the sub frame (nested-block shadowing invisible) and it has **no `callable_qn` and no narrowing at all**. Highest-leverage: (1) push a frame per block/`if`/loop/closure so `my` shadowing works; (2) port Rust's `let f = \&foo` → `cbm_scope_bind_callable` + call-site `lsp_callable_alias` dispatch (`rust_lsp.c:3949-3972`, `:4412-4414`) so coderef dispatch resolves; (3) add `ref($x) eq 'Class'` / `->isa` narrowing on the `extract_narrowing` + child-scope pattern (`ts_lsp.c:2992/3516`). Py is already close to strong — its remaining gaps are narrowing completeness (`else`-negation, per-plain-block scope), lower priority. Rust is strong here (the `callable_control_flow_depth` + `pending_closure_param_type` patterns are references to *copy*, not fix).

### 2.7 Gap scorecard — Scope & binding (5 = gold, split across TS narrowing / Rust patterns / Kotlin balance)
| target | score | single highest-leverage fix |
|---|---|---|
| **Perl** | **1** | Push a lexical frame per block/if/loop/closure (today: 2 pushes total, all `my` collapse to the sub frame) and add `callable_qn` binding for `\&foo` coderefs (mirror `rust_lsp.c:3949-3972`). |
| **Python** | **4** | Model `else`-branch narrowing negation (documented v1 gap `py_lsp.c:3111`) and per-plain-block scopes; its verify-and-latch fail-closed layer (`:187-228`) is already a *reference*. |
| **Rust** | **5** | Reference implementation (`rust_bind_pattern`, `callable_control_flow_depth`, `pending_closure_param_type`). |

---

## 3. Cross-file resolution architecture — the exact template Perl is missing

**This is Perl's #1 gap.** `cbm_run_perl_lsp_cross` is **declared** (`perl_lsp.h:114`) but has **no implementation** in `perl_lsp.c`, and Perl appears **nowhere** in `pass_lsp_cross.c` — not in the capability gate `cbm_pxc_has_cross_lsp` (`pass_lsp_cross.c:954-968`, Perl falls to `default: return false`), not in `cbm_pxc_run_one` (`:1214-1281`), not in the dispatch. Perl is Tier-1 (single-file) only. Below is the exact template to copy.

### 3.1 The three tiers
- **Tier 1 — per-file, no cross registry.** `cbm_run_<lang>_lsp` builds a throwaway per-file registry (stdlib + this file's own defs) and resolves. This is *all Perl has today* (`cbm_run_perl_lsp`, `perl_lsp.c:1804`).
- **Tier 2 — shared sealed base + per-file overlay.** The workhorse. Two functions:
  - **Builder (once per project):** `cbm_ts_build_cross_registry` (`ts_lsp.c:5707`) — init, `cbm_ts_stdlib_register`, reset a def-volume-scaled budget (`:5716`), loop all `CBMLSPDef` filtering by lang (`:5717-5724`), `cbm_registry_finalize`, **`reg->read_only = true`** (`:5726`, the seal). Rust/C/Py mirror this exactly (`rust_lsp.c:6415`+`:6450`; `c_lsp.c:5956`+`:5979`; `cbm_py_build_cross_registry` `py_lsp.c:5186`).
  - **Per-file resolve:** `cbm_run_ts_lsp_cross_with_registry` (`ts_lsp.c:5737`) — builds a **small overlay** registry holding *only this file's own-module defs*, sets **`overlay.fallback = reg`** (`:5752`) so imports/stdlib resolve through the sealed base while local AST-refinement passes mutate only the overlay (`ast_sweep_shapes`, `rebuild_signatures_from_ast`, `convert_signature_type_params`, `apply_jsdoc_signatures`, `infer_implicit_returns`, `:5798-5802`), finalizes the overlay, then `ts_lsp_process_file`.
- **Tier 3 — metadata-driven pure lookup.** No parse, no AST walk (`pass_lsp_cross.c:1361`, e.g. `cbm_go_fast_resolve_qualified_calls`): resolve the Tier-1 `lsp_unresolved` entries against the shared registry, *then* the AST walk for NAMED receivers. Read-only, safe on the sealed registry across parallel workers.

The **overlay + fallback + seal** triad is the load-bearing pattern: it preserves per-file AST-refinement quality (locals get their real shapes) without re-registering every imported module in every file, and the seal keeps the shared base race-free and hash-indexed (no O(files·defs) tail — the "Linux-kernel full-index hang" the `type_registry.h:146-154` comment describes).

### 3.2 Registering a `CBMLSPDef` into a registry (the copy-paste core)
`cbm_run_ts_lsp_cross` (`ts_lsp.c:5810`, the standalone path) shows the label→registry translation: for `label=="Class"|"Interface"` build a `CBMRegisteredType`, parse `embedded_types` (pipe-separated extends list, `:5837-5858`) and `field_defs` (`"name:type"`, `:5860+`) into the parallel arrays. `pxc_build_lsp_def` (`pass_lsp_cross.c:372`) is the upstream `CBMDefinition → CBMLSPDef` converter — note `receiver_type = src->parent_class` (`:390`, NULL ⇒ free function), `is_interface` (`:396`), `return_types = src->return_type` (`:400`), and language-conditional base-QN resolution (`:404-407`).

### 3.3 Fold helpers — recovering structure the flat def stream drops
The extractor emits one flat `CBMDefinition` per struct field / interface method; those rows are *dropped* by `pxc_map_label` unless folded back:
- **`pxc_fold_go_struct_fields` (`pass_lsp_cross.c:431`)** — folds flat `"Field"` defs into their owning struct's `field_defs` (else *"every Go struct registers with zero fields and field-chain calls (`h.svc.Handle`) can never resolve"*, `:421-430`).
- **`pxc_fold_go_interface_methods` (`pass_lsp_cross.c:497`)**, **`pxc_build_rust_impl_relation` (`:556`)** — same idea for interface method sets and Rust `impl Trait for Type` provenance. Both run inside `cbm_pxc_collect_all_defs` (`:651-652`) so one site covers prebuilt + fallback paths.

### 3.4 Import maps (`local_name → semantic import QN`)
`cbm_pxc_build_import_map` (`pass_lsp_cross.c:823`) builds the per-file map from gbuf IMPORTS edges; the resolver receives it as parallel `import_names[]`/`import_qns[]`. Language-specific reattachment matters:
- **Python from-imports (`pxc_import_value_qn` `:729-756`, `pxc_python_import_from_metadata` `:774-818`):** `from target import handler` extracts as `module_path="target.handler"` but the edge targets `Module P.target` — reattach the member QN *only* when the raw metadata proves one unique non-aliased path (else fail closed). `pxc_unique_import_path` (`:675`) returns NULL on ambiguity.
- **Kotlin (`pxc_kotlin_import_from_metadata` `:708`):** keep the source package spelling; the resolver still requires a registered symbol before emitting an edge.

The recurring discipline: **the import map only *proposes* a QN; the resolver emits an edge only after the sealed registry *materializes* that QN.** A wrong candidate cannot earn a semantic edge.

### 3.5 The exact Perl port
Mirror `cbm_run_php_lsp_cross` (`php_lsp.c:4486`, the closest single-file→cross sibling). Concretely:
1. Implement `cbm_run_perl_lsp_cross` in `perl_lsp.c`: parse-or-reuse `cached_tree`, `cbm_registry_init` + `cbm_perl_stdlib_register`, register the caller-supplied `CBMLSPDef[]` as `CBMRegisteredFunc`s (`receiver_type` from `def->receiver_type`), seed the use-map from `import_names/import_qns`, **`cbm_registry_finalize_into` a scratch idx-arena** (not the result arena — see the FastAPI +1.1 GB warning at `type_registry.h:169-172`), then `perl_lsp_process_file`.
2. Add a builder `cbm_perl_build_cross_registry` on the `cbm_ts_build_cross_registry` shape (`ts_lsp.c:5707`) with the `read_only=true` seal.
3. Wire `CBM_LANG_PERL` into `cbm_pxc_has_cross_lsp` (`pass_lsp_cross.c:954`) and add a `case CBM_LANG_PERL` in `cbm_pxc_run_one` (`:1226`).
4. Keep the zero-edge guarantee: `use`→module-path resolution that yields nothing emits nothing. (PLAN.md `perl-cross-file-lsp` has the adjudicated binding corrections — this playbook supplies the *reference architecture* those corrections instantiate.)

### 3.6 Gap scorecard — Cross-file architecture (5 = gold `ts_lsp.c`/`pass_lsp_cross.c`)
| target | score | single highest-leverage fix |
|---|---|---|
| **Perl** | **0** | Implement `cbm_run_perl_lsp_cross` + `cbm_perl_build_cross_registry` on the php/ts template (overlay+fallback+seal), then register `CBM_LANG_PERL` in `cbm_pxc_has_cross_lsp` (`pass_lsp_cross.c:954`) and `cbm_pxc_run_one` (`:1226`). Nothing else moves the Perl needle as much. |
| **Python** | **4** | Cross is wired (`cbm_py_build_cross_registry` `py_lsp.c:5186`); tighten from-import reattachment ambiguity handling to match `pxc_python_import_from_metadata` (`pass_lsp_cross.c:774`). |
| **Rust** | **4** | Cross is wired (`rust_lsp.c:6415`); the gap is table fuel (§7), not architecture. |

---

## 4. Confidence & strategy taxonomy

**Emission is uniform; the confidence *number* and *strategy string* encode the resolver's certainty.** Every resolver funnels through a tiny `<lang>_emit_resolved_call(ctx, callee_qn, strategy, confidence)` that pushes a `CBMResolvedCall` (`cbm.h:392`). Reference implementations:
- **TS: `ts_emit_resolved_call_at` (`ts_lsp.c:249`)**, `ts_emit_resolved_reference` (`:268`, kind `CALL_REFERENCE`), `ts_emit_unresolved_call_at` (`:285`, confidence `0.0f`, strategy `"lsp_unresolved"`, carries a `reason`).
- **Rust: `rust_emit_resolved_call_reason` (`rust_lsp.c:4095`)** → `rust_emit_resolved_call` (`:4116`).
- **C: `c_emit_resolved_call` (`c_lsp.c:3822`)** → `_orig_at` (`:3793`).
- **Perl: `perl_emit_resolved` (`perl_lsp.c:628`)**, `perl_emit_reference` (`:646`).

### 4.1 Two ways to encode confidence
1. **Named macros (Rust — the auditable way):** `rust_lsp.h:43-50` — `CBM_RUST_CONF_DIRECT 0.95` (path/alias hit), `_METHOD 0.95` (inherent), `_UFCS 0.93` (`T::method()`), `_TRAIT_SOLE 0.92` (trait, single impl), `_PROMOTED 0.90` (Deref/blanket), `_OPERATOR 0.88`, `_MACRO_KNOWN 0.85`, `_TRAIT_AMB 0.85` (trait, many impls). The macro name documents *why* the number.
2. **Inline graded floats (C/C#/Py/Kotlin):** e.g. `cs_lsp.c` — `0.95` static-typed (`:2028`), `0.92` inherited/namespace (`:2113`,`:2138`), `0.90` extension/using-static (`:2062`,`:2128`), `0.85` synthetic ctor (`:2205`), `0.65` free-func fallback (`:2168`), `0.98` callable alias (`:2085`). C: `0.95`/`0.90`/`0.85`/`0.80` (`c_lsp.c` confidence histogram).

### 4.2 The confidence ladder (what earns what)
- **0.95** — exact, unambiguous: direct QN/alias hit, inherent method on a known receiver type, constructor. (`lsp_direct`, `lsp_method`, `cs_static_typed`.)
- **0.92-0.93** — one indirection but still unique: UFCS/`Self::new`, sole trait impl, inherited method up a known base chain.
- **0.85-0.90** — real ambiguity resolved by a rule: extension methods, `using static`, Deref/blanket promotion, known-macro mapping, *ambiguous* trait method (many impls).
- **0.55-0.65** — heuristic last resort: free-function short-name fallback (`cs_free_func_fallback 0.65`, `cs_lsp.c:2168`).
- **0.0** — `lsp_unresolved`: emit nothing resolvable, record the raw text + `reason` for observability.

### 4.3 The strategy string — a self-describing provenance tag
Strategy strings are the resolver's chain-of-thought made durable. Rich taxonomies: **C** (22 strategies: `lsp_direct`, `lsp_virtual_dispatch`, `lsp_adl`, `lsp_operator_adl`, `lsp_template_instantiation`, `lsp_smart_ptr_dispatch`, `lsp_base_dispatch`, `lsp_copy_constructor`, …), **Rust** (17: `lsp_trait_dispatch`, `lsp_deref_dispatch`, `lsp_trait_ufcs`, `lsp_cross_crate`, `lsp_short_name_unique`, `lsp_operator_trait`, `lsp_prelude_trait`, …), **Kotlin** (`lsp_kt_extension`, `lsp_kt_safe`, `lsp_kt_delegate_access`, `lsp_kt_lambda_it`, …), **Py** (`lsp_super_init`, `lsp_operator_dunder`, `lsp_dict_dispatch`, `lsp_method_union`, `lsp_generic_method`, …). Two cross-language strategies are shared verbatim across *every* resolver: **`lsp_callable_alias`** and **`lsp_callable_value_reference`** (the `callable_qn` mechanism from §0.3). Naming convention varies — most use `lsp_*`; C#/Perl also use `<lang>_*` (`cs_static_typed`, `perl_method_typed`).

### 4.4 How false edges are avoided (the zero-edge guarantee)
Three mechanical gates, present in every strong resolver's emit function:
1. **Require both endpoints:** `if (!callee_qn || !ctx->enclosing_func_qn) return;` (`ts_lsp.c:251`, `rust_lsp.c:4098`, `perl_lsp.c:630`) — no floating edges.
2. **Require registry materialization:** an import map or scope guess only *proposes* a QN; the edge is emitted only after the sealed registry confirms the target exists (§3.4). Ambiguous imports "deliberately fail closed and remain USAGE" (`ts_lsp.c:1927`,`:1960`).
3. **Unknown receiver ⇒ no edge:** the explicit discipline — `perl_lsp.c:45` (*"if a receiver's type is unknown/unindexed, NO edge is [emitted]"*), `:858`, `:891`, `:902`; `rust_lsp.c:2883`; Kotlin fail-closed on operators (`kotlin_lsp.c:2593`). Perl is *exemplary* here — the guarantee is stated 7× in the file.

### 4.5 Gap scorecard — Confidence & strategy (5 = gold Rust macro table / C 22-strategy set)
| target | score | single highest-leverage fix |
|---|---|---|
| **Perl** | **2** | Only 2 tiers (`PERL_CONF_LITERAL 0.95`, one `0.75`). Introduce a named-macro ladder (mirror `rust_lsp.h:43-50`): distinct confidences for `@ISA`-inherited (0.90), imported (0.95), SUPER:: (0.92), heuristic package-map (0.85) so downstream ranking can discriminate. |
| **Python** | **4** | Rich already (`0.55-0.97`, 18 strategies); minor — promote inline floats to named macros for auditability. |
| **Rust** | **5** | Gold standard for this axis — the macro table *is* the reference. |

---

## 5. Neg-memo & performance — the perf-sacred invariants

Repeated *misses* are the dominant cost: macro-expanded/generated code asks the *same failing question* thousands of times, re-paying the whole resolve ladder each time (`lsp_neg_memo.h:5-9`: *"linux kernel: 4 trait-heavy rust files at ~63 s each"*). Four mechanisms, layered:

### 5.1 Negative memo (`lsp_neg_memo.h`) — cache the misses
Open-addressing 64-bit-key set, arena-backed (dies with the per-file arena). `cbm_negmemo_key(site, a, b)` (`:55`) FNV-1a's a **site tag** + two query strings; `cbm_negmemo_contains`/`_insert` (`:76`,`:107`). **Hard gate:** valid *only* on a **sealed** registry (`reg->read_only`) and *only* for queries whose cascade reads nothing but the registry + query strings (`lsp_neg_memo.h:11-24`). Collision-safe because callers keep their cheap **direct** lookup *before* the memo check (the C-memo pattern) — a colliding real hit is still found; only the expensive miss-ladder is skipped.
- **Rust wiring (the reference):** `ctx->neg_memo` with 4 site tags — `1`=(receiver_qn, method) inherent miss (`rust_lsp.c:2690`), `2`=(trait_qn, method) trait miss (`:2779`), `3`/`4`=macro + macro-arg memos on `ctx->macro_memo` (`:3591`,`:3702`). Check at cascade entry, insert on the miss return (`:2755`,`:2818`).
- **C wiring (bespoke, predates the header):** `c_neg_memo_hash`/`_contains`/`_insert` (`c_lsp.c:2638-2713`) — same design, `malloc`-backed, grow-by-rehash at 70% load. The header comment names it as a migration candidate (`lsp_neg_memo.h:29`).
- **Coverage matrix:** negative memo exists in **only** C (35 refs) and Rust (12). TS, Kotlin, C#, Go, Java, PHP, **Py, Perl = none**.

### 5.2 Build-time index memo (`CBMIdxMemo`, `lsp_neg_memo.h:149-228`)
`cbm_idxmemo_get`/`_put_if_absent` — exact-match string→int map for registration loops ("have I registered this QN, at which index?") in O(1). Rust uses it heavily to distinguish unique vs ambiguous receiver types during registry build (`rust_lsp.c:398-448`, `:5606+`, `:6247+`) — without it, probing the pre-finalize registry linearly is the ~63 s kernel quadratic (`lsp_neg_memo.h:142-148`).

### 5.3 Positive eval memo — cache the hits (§1.4)
TS `node.id`→type memo (`ts_lsp.c:2085`,`:2382`) with the degraded-guard; Rust (13 sites), Py (`py_eval_expr_type`). The invariant: **never memoize a capped/degraded result** (`ts_lsp.c:2116-2118`,`:2381`).

### 5.4 Walk-depth cap + O(1) wide-node iteration
- **Subtree-skip wrapper** (identical shape everywhere): `if (ctx->walk_depth >= CAP) return; ctx->walk_depth++; …; ctx->walk_depth--;` — C (`c_lsp.c:3912`, cap `C_LSP_MAX_WALK_DEPTH 512`), Py (`py_lsp.c:145`, `cbm_lsp_max_walk_depth()`), **Perl (`perl_lsp.c:939`,`:1464`, cap 512 — already correct)**. Past the cap the subtree is skipped: unresolved, not crashed (graceful degradation).
- **`cbm_lsp_collect_children` (`lsp_node_iter.h:24`)** — one O(n) cursor pass into an arena array, because `ts_node_child(node,i)` is O(i) ⇒ the naive loop is O(n²) on a wide root (*"reallyLargeFile.ts: 583K comment lines made the per-file LSP passes run ~133 minutes"*, `lsp_node_iter.h:10-14`). Perl already uses it (`perl_collect_children`, PLAN.md anchor). **Every wide-node loop must use this.**
- **Arena discipline:** per-file index allocations go to a *scratch* arena destroyed after the walk (`cbm_registry_finalize_into`, `type_registry.h:169-172`), never the pipeline-lifetime result arena.

### 5.5 Gap scorecard — Neg-memo & performance (5 = gold C/Rust)
| target | score | single highest-leverage fix |
|---|---|---|
| **Perl** | **2** | Has walk-cap + O(1) children + zero-edge, but **no eval memo and no neg-memo**. When §3 cross-file lands (multi-file ladders), add `CBMNegMemo` on the Rust template (`rust_lsp.c:2690` pattern) gated on the sealed registry; add a `node.id` eval memo when §1 deepens. |
| **Python** | **3** | Has eval memo; **no neg-memo** despite a deep cascade (`lsp_neg_memo.h:29` lists it as a candidate). Add `CBMNegMemo` for method/attr misses on the sealed cross registry. |
| **Rust** | **5** | Reference implementation (shared neg-memo + idxmemo + eval memo + step cap). |

---

## 6. Framework / route / test extraction

**Architectural reframe (the load-bearing finding): routes and test-classification are NOT a resolver concern.** They live in the **extraction layer** — `extract_defs.c`, `service_patterns.c`, `lang_specs.c`. Grepping every `*_lsp.c` for route tokens (`route|router|endpoint|RequestMapping|GetMapping|@app|HttpGet…`) returns **zero** matches. The *only* framework-adjacent thing a resolver touches is **decorator/annotation/derive effects on type & call resolution.** So when you uplift Perl "web routes", you wire `lang_specs.c` + `service_patterns.c`, **not** `perl_lsp.c`.

### 6.1 HTTP routes — two centralized mechanisms
- **Def-level (decorator/annotation → `CBMDefinition.route_path`/`.route_method`, `cbm.h:204-205`):** `decorator_method_name` (`extract_defs.c:1329`, maps `@app.get`/`@router.post`/`@api_route` → verb), `annotation_route_method` (`:1360`, Spring `@GetMapping`/JAX-RS `@GET`), `extract_route_from_decorators` (`:1779`, called for functions `:3782` + methods `:4999`), class-level prefix join `join_route_paths` (`:1821`, applied `:5000-5002`), Razor `cbm_razor_page_route` (`:8040`). Gated by `<lang>_decorator_types[]` in `lang_specs.c`: python (`:214`), ts (`:255`), java (`:352`), kotlin (`:465`), cs (`:399`), php (`:419`), rust `attribute_item` (`:332`). **Go and Perl have no decorator array → no def-level routes.**
- **Call-level (`service_patterns.c` `CBM_SVC_ROUTE_REG` table, `:320-357`):** matches library QNs — `gin/chi/echo/fiber` (Go), `express/fastify/koa` (JS), `flask/FastAPI/starlette` (Py), `actix-web/axum` (Rust), `ktor.routing` (Kotlin), Laravel/Symfony (PHP) — via `cbm_service_pattern_match` (`service_patterns.h:35`). This is the **only** path by which Go and Rust routes surface.

### 6.2 Test detection — a Go-only resolver micro-feature
The flag chain: `CBMDefinition.is_test` (`cbm.h:222`) ← `cbm_is_test_file` (`helpers.c:393`, path/suffix only — `_test.go`, `test_*.py`, `*Test.java`, `t/`+`.t` for Perl at `:450-452`) **or** the only attribute-based detector `rust_def_is_test` (`extract_defs.c:2040`: `#[test]`, `#[tokio::test]`, …). It carries on the shared `CBMLSPDef.from_test_file` (`go_lsp.h:100`) and `CBMRegisteredType.from_test_file` (`type_registry.h:61`). **Consumed in exactly one resolver:** `go_lsp.c:3359` — the sole-implementer interface scan skips a test double so it never shadows the production implementer (`if (cand->from_test_file && !iface_rt->from_test_file) continue;`). Every other resolver (TS/Py/C/Rust/Kotlin/C#/Java/PHP/Perl) reads it **zero** times. There is **no** `@Test`/JUnit/pytest/`testing.T`/`describe` framework detection anywhere — tests are classified only by residing in a test *file*.

### 6.3 Decorator / annotation / derive effects on resolution (the real resolver-level framework work)
- **Rust `#[derive]` — the strongest DI/ORM analog.** Inside `cbm_rust_build_local_registry` (`rust_lsp.c:5597`, block `:5945-6035`): parse `#[derive(Clone, Debug, Serialize, clap, thiserror, …)]`, match a curated `derives[]` table → trait QN, **append the trait QN to `embedded_types` AND synthesize the derived method entries** (`:6000-6031`) so `.clone()`/`.fmt()`/`.eq()` resolve on a derived struct. Plus trait-impl flags `CBM_FUNC_FLAG_RUST_TRAIT_IMPL`/`RUST_ABSTRACT` (`rust_lsp.c:2467,2518,2617,4303`) keep trait methods from being mistaken for inherent.
- **Python decorators → flags (mostly inert today).** `py_register_func_decorators` (`py_lsp.c:56`) maps `@property/@classmethod/@staticmethod/@abstractmethod/@overload/@final` to `CBM_FUNC_FLAG_*` and stores `decorator_qns`. **But only `PROPERTY|OVERLOAD|AMBIGUOUS_BINDING` are ever read** — by `py_func_is_exact_callable_value` (`:83`), the value-binding gate that fails a decorated func closed to USAGE. `CLASSMETHOD/STATICMETHOD/ABSTRACTMETHOD/FINAL` are set but never read; `ASYNC/GENERATOR` are declared (`type_registry.h:17-18`) but never set. The header promise `@property→getter-return` (`type_registry.h:12`) and user-decorator return substitution (`:37-38`) are **documented but unimplemented**. The one real return rewrite that exists: `py_substitute_self` (`py_lsp.c:1277`, `Self`→receiver).
- **Kotlin `decorator_qns` as a DSL side-channel:** stores `"lambda_receiver:<TypeQN>"` (`kotlin_lsp.c:2043`), consumed (`:3928-3960`) to type a trailing scope-function/DSL-builder lambda (`apply{}`, Ktor DSLs).
- **Java/C#/PHP parse annotations then discard them** — Java recognizes `annotation_type_declaration` only as a type (`java_lsp.c:1752`), C# *skips* `attribute_list` (`cs_lsp.c:3316`), PHP *skips* `attribute_group` (`php_lsp.c:820`). A clear, uniform lift opportunity (`@Autowired`/`@Entity`/`@Component` effects unmodeled).
- **Perl:** only `perl_scan_isa_attribute` (`perl_lsp.c:1418`, `:isa(Parent)` inheritance). No DI/route/test.

### 6.4 Reusable vs language-specific
- **Reusable (don't re-implement per resolver — wire into the shared machinery):** the decorator→route pipeline (`extract_defs.c` + `lang_specs.c` `*_decorator_types[]`), the `service_patterns.c` `ROUTE_REG` table, `cbm_is_test_file`, and **the `decorator_qns` field itself** — a generic per-func string side-channel already exploited by Python (flags) and Kotlin (`lambda_receiver:`).
- **Language-specific:** Python decorator→flag gating (`py_lsp.c:56/83`), Kotlin DSL `lambda_receiver` typing, Rust `#[derive]`→trait synthesis, Go test-double exclusion (`go_lsp.c:3359`).

**→ apply to Perl/Py/Rust:** *Perl* — routes/tests are pure extraction-layer wiring: add a `perl_decorator_types[]`/attribute path in `lang_specs.c` (Perl is absent from the decorator list) and Perl route libs (Dancer2/Mojolicious/Catalyst) to `service_patterns.c:320`; map `.t`/`.psgi` and `t/`,`xt/` in `cbm_is_test_file`/`language.c`. *Python* — the highest-value resolver-level lift is making the *already-set* decorator flags do something: implement `@property`→getter-return-type and user-decorator return substitution the header already promises (`type_registry.h:12,37-38`). *Rust* — the `#[derive]`→trait-synthesis pattern (`rust_lsp.c:5945`) is a reference to *emulate* for other langs, not fix; its gap is def-level attribute routes (`#[get("/")]` actix/Rocket), which belong in the extraction layer.

### 6.5 Gap scorecard — Framework/route/test (5 = gold: extraction-layer routes + Rust derive + Go test-double)
| target | score | single highest-leverage fix |
|---|---|---|
| **Perl** | **1** | Extraction-layer wiring: add Perl to `lang_specs.c` decorator/attribute handling + `service_patterns.c:320` route libs (Dancer2/Mojolicious/Catalyst) + `.t`/`.psgi` test mapping in `cbm_is_test_file`. Not a `perl_lsp.c` change. |
| **Python** | **3** | Implement the dormant decorator effects the header already promises — `@property`→getter return (`type_registry.h:12`) and user-decorator return substitution (`:37-38`); wire `ASYNC`/`GENERATOR` (declared, never set). |
| **Rust** | **4** | Strongest resolver-level DI (`#[derive]` synthesis); add def-level attribute-route extraction (`#[get("/")]`) in the extraction layer to match def-level route langs. |

---

## 7. Stdlib table strategy

**The one thing a stdlib entry exists to do: carry a real return type so the *next* `.method()` can be looked up on it.** The payoff site is literally one line — a call resolves to `func_type->data.func.return_types[0]` (`go_lsp.c:613-619`); a method that returns `unknown` (or carries no signature) **dead-ends the chain**. Everything below follows from that.

### 7.1 The feature matrix (grep-derived; macro tables counted by invocation)
| table | lines | entry form | sigs w/ real returns | `unknown` returns | receiver-typed methods | method tables | generics | inheritance |
|---|---|---|---|---|---|---|---|---|
| **go** (gold) | 30 630 | inline | **2 045, 0 unknown** | 0 | 1 551 | 88 interfaces | concretized | — |
| **python** | 23 527 | inline | **0 signatures at all** | n/a | 2 797 (name-only) | 548 | 0 | 501 `embedded_types` |
| **rust** | 1 794 | `ADD_*` macro | 271 real / **750 `unknown`** | 750 | all (via macro) | **0** | **0** | **0** |
| **perl** | 446 | `REG_*` macro | 13 named / 128 scalar / **147 unknown** | 147 | 60 (OO tail only) | 0 | 0 | 0 |
| java | 1 329 | `REG_*` | 346, 0 unknown | 0 | all + ctor/field | via `parents_` | no | `parents_` |
| cs | 1 139 | `REG_*` | 77 + generics | 1 | + `REG_EXTENSION` (LINQ) | via `parents_` | **`REG_GENERIC_TYPE`** | `parents_` |

Each register fn is called once into the shared registry: `cbm_go_stdlib_register` (`go_stdlib_data.c:9`), `cbm_python_stdlib_register` (`python_stdlib_data.c:19`), `cbm_rust_stdlib_register` (`rust_stdlib_data.c:68`), `cbm_perl_stdlib_register` (`perl_stdlib_data.c:95`).

### 7.2 Gold shape — `go_stdlib_data.c` (auto-generated, `:1-7`)
One entry = `memset` + QN/short_name (+ `receiver_type`) + an explicit `ret[]` array + `cbm_type_func`. It carries **both** OO axes:
- **Receiver-typed method with a concrete return** (`go_stdlib_data.c:2189-2200`): `bufio.Writer.Available` → `receiver_type="bufio.Writer"`, `ret[0]=cbm_type_builtin(arena,"int")`.
- **OO-chain seed** (`:3174-3220`): `bytes.NewBuffer` (free fn) → `cbm_type_pointer(cbm_type_named("bytes.Buffer"))`; then `bytes.Buffer.Next` (receiver `bytes.Buffer`) → `cbm_type_slice(cbm_type_builtin("byte"))`. So `bytes.NewBuffer(x).Next(n)` chains end-to-end.
- **Interface method tables** (`:149-155`): `context.Context` with `is_interface=true` + `method_names=[...]` (88 interfaces).

### 7.3 Why the thin tables dead-end
- **Python** (`python_stdlib_data.c:214-234`): **zero `cbm_type_func` calls in the whole file** — methods set `receiver_type` but never `signature`. Strong on *existence + inheritance* (548 method tables, 501 `embedded_types` MRO chains), useless for *chaining* (`parser.parse_args()` yields no type).
- **Rust** (`rust_stdlib_data.c`): `ADD_TYPE` (`:38-45`) sets only QN/short/is_interface — **no method table, no `type_param_names`, no `embedded_types`**. The chainable methods return `cbm_type_unknown()`: `Vec::iter/iter_mut/into_iter` (`:230-242`), `Iterator::map/filter/collect` (`:296-300`), `Option::map/and_then/unwrap` (`:139-149`). **`vec.iter().map().collect()` breaks on the very first hop.** What it gets *right* (the template to extend): self-returning builders + typed leaves — `String::new/to_uppercase/clone`→`String` (`:177,186,190`), `len`→`usize`, `is_empty`→`bool`. Unused rich fields: `impl_trait_qn` + `CBM_FUNC_FLAG_RUST_TRAIT_IMPL` are never populated.
- **Perl** (`perl_stdlib_data.c`): `#define MIXED cbm_type_unknown()` (`:26`); header admits *"Return types are left UNKNOWN … a baseline symbol table"*. Bare builtins carry no receiver + unknown returns (`map/grep/sort/split/keys/values`, `:101-129`); typed leaves exist (`length`→int, `join`→string). The **"earns its keep" tail** is the curated OO chains (~6 types): DBI `connect`→`DBI.db`→`prepare`→`DBI.st`→`rows`→int (`:368-397`), LWP `UserAgent.get`→`HTTP.Response`→`code`→int (`:399-413`).

### 7.4 The generic-return pattern Rust is missing (C# has it)
`cs_stdlib_data.c:720-734` — `REG_EXTENSION` seeds LINQ `Where` with receiver `IEnumerable<T>` → return `IEnumerable<T>` via `cbm_type_template` + `cbm_type_type_param`; `First` → element `T` (`:741-745`). This is exactly the `Vec<T>::iter → Iterator<Item=T>` shape Rust erases.

### 7.5 What makes an entry "earn its keep" (priority order)
**(a) A real return type** (`ret[0]` is `named`/`builtin`/`slice`/`template`, never `unknown`) — the single highest-leverage property, literally what `go_lsp.c:613` reads. **(c) OO/fluent chains** = return-type transitions between real named types (constructor→named, each method→next named). **(b) Receiver-typed methods** (`receiver_type` set) — necessary but not sufficient without (a). **(d) Generics** (`type_param_names` + `cbm_type_template`/`type_param` returns). **(e) Existence + inheritance breadth** (`method_names` + `embedded_types` for supertype walks). Leverage: **(a) ≈ (c) > (b) > (d) > (e)**.

> **Rule of thumb:** every method's `ret[0]` should name a type that itself has registered methods. An entry earns its keep only when its return type is the *receiver* of some other entry. That transitive closure **is** the chain-resolution graph; `unknown`/absent signatures are its cut edges.

### 7.6 Recipe — how to grow a thin table well (Rust first: 750 `unknown`; then Perl's builtin surface)
Work **type-cluster by type-cluster**:
1. **Register the type cluster as real named types first, with generics** — and add the *iterator/adapter* return types the methods will need (Rust has none): seed `core.slice.Iter`, `core.iter.Map`, `core.iter.Filter`; give generic types `type_param_names` (`{"T",NULL}` / `{"K","V",NULL}`), extending `ADD_TYPE` like C#'s `REG_GENERIC_TYPE`.
2. **Kill every `unknown` on a factory/constructor** — chain *entry points*, cheapest fix / highest payoff: `Vec::new/with_capacity/from`→`Vec`, `HashMap::new`→`HashMap`, `Box::new`→`Box` (model: go `bytes.NewBuffer`, perl `DBI.connect`).
3. **Type the adapter methods** that return self or a sibling (the fluent middle): `Vec.iter`→`slice.Iter`, `Iterator.map/filter/take/rev/enumerate`→`Iterator`, `String.to_uppercase`→`String`; thread the generic param via `cbm_type_template`+`cbm_type_type_param` (cs LINQ `Where`).
4. **Type the terminals/accessors** that unwrap the element: `Iterator.collect`→`Vec<T>`, `Vec.get/first/last/pop`→`Option<T>`, `HashMap.get`→`Option<V>`, `Option.unwrap`→`T`.
5. **Keep the typed leaves** (`len`→usize, `is_empty`/`contains`→bool) — correct as-is.
6. **Seed language-specific provenance last:** populate `impl_trait_qn` + `CBM_FUNC_FLAG_RUST_TRAIT_IMPL` for trait-impl methods (inherent-vs-trait disambiguation); add `method_names`/`embedded_types` tables to `REG_TYPE` (perl/rust set none) for supertype method walks like Python.

### 7.7 Gap scorecard — Stdlib tables (5 = gold `go_stdlib_data.c`)
| target | score | single highest-leverage fix |
|---|---|---|
| **Rust** | **2** | Replace the 750 `cbm_type_unknown()` returns on factories/adapters/terminals with real named/template returns (recipe steps 2-4) — and add the iterator/adapter named types they return. This single change lights up nearly all Rust method chains. |
| **Perl** | **1** | Extend the DBI/LWP-style typed-OO tail (`perl_stdlib_data.c:368-413`) to the common CPAN OO surface (Moose accessors, `IO::*`, `JSON`, `Try::Tiny`); type `open`→a handle type and list builtins→list. |
| **Python** | **3** | It has breadth (existence + MRO) but **zero return types** — begin adding `cbm_type_func` signatures to the highest-traffic container/`pathlib`/`str`/`dict` methods so chains resolve past method existence. |

---

## Top 10 cross-language uplift moves

Ranked by gap-closed-per-unit-effort. Each names the strong-resolver source pattern and the target file. Distribution reflects the scorecards: Perl (6) is weakest, Rust (1, but huge) is otherwise strong, Python (3) is mid.

| # | move | source pattern (cite) | target | why it tops the list |
|---|---|---|---|---|
| **1** | Implement `cbm_run_perl_lsp_cross` + `cbm_perl_build_cross_registry` on the overlay+fallback+seal template, then register `CBM_LANG_PERL` in the capability gate + dispatch | `ts_lsp.c:5707`,`:5737`; `php_lsp.c:4486`; wire at `pass_lsp_cross.c:954`,`:1226` | `perl_lsp.c`, `pass_lsp_cross.c` | Perl resolves **zero** cross-file edges today (§3, score 0) — the single biggest gap in the repo. |
| **2** | Replace Rust stdlib's 750 `cbm_type_unknown()` returns on factories/adapters/terminals with real named/template returns, and add the iterator/adapter named types they return | `go_stdlib_data.c:3174-3220`; `cs_stdlib_data.c:720-734` | `generated/rust_stdlib_data.c` | One change lights up nearly *all* Rust method chains — the engine (`rust_eval_expr_type`) is fine, the fuel is empty (§7, score 2). |
| **3** | Give Perl real lexical scoping: push a `CBMScope` frame per block / `if` / loop / closure | `rust_lsp.c:4841` (block push), Kotlin balanced pairs `kotlin_lsp.c:3662/3672` | `perl_lsp.c` | Today only 2 pushes total (`perl_lsp.c:229`,`:1169`); every `my` collapses to the sub frame so shadowing is invisible (§2, score 1). |
| **4** | Port `callable_qn` alias binding for `my $f = \&foo; $f->()` | `rust_lsp.c:3949-3972` (bind) + `:4412-4414` (dispatch `lsp_callable_alias`) | `perl_lsp.c` | Perl has **zero** callable-alias support; coderef dispatch is unresolvable (§2). |
| **5** | Port the `member_expression→lookup_member_type` two-node chain loop for `$obj->a->b` | `ts_lsp.c:2163-2199` + `:2005-2036` | `perl_lsp.c` `perl_eval_expr_type` | Perl's eval engine is bless-only; OO chains die at hop 1 (§1, score 2). |
| **6** | Add a per-file work budget + `node.id` positive eval memo **before** deepening Perl's eval engine | `ts_lsp.c:2082-2114` (budget −16/entry + degraded-guard memo) | `perl_lsp.c` | A deeper eval engine without a budget is a DoS; the memo makes repeated evals O(1) (§1/§5). |
| **7** | Add a `CBMNegMemo` to Python's method/attr miss cascade, gated on the sealed cross registry | `rust_lsp.c:2690-2755` (site-tagged neg-memo) | `py_lsp.c` | Py has a deep cascade but **no neg-memo** (`lsp_neg_memo.h:29` lists it as a candidate); repeated misses re-pay the ladder (§5, score 3). |
| **8** | Introduce a named-macro confidence ladder (inherited / imported / SUPER:: / heuristic-map get distinct tiers) | `rust_lsp.h:43-50` | `perl_lsp.h`, `perl_lsp.c` | Perl emits only 2 tiers; downstream ranking can't discriminate resolution quality (§4, score 2). |
| **9** | Start adding `cbm_type_func` return-type signatures to Python's highest-traffic stdlib methods (`str`/`dict`/`list`/`pathlib`) | `go_stdlib_data.c:2189-2200` (receiver + `ret[]` shape) | `generated/python_stdlib_data.c` | Python's 23K-line table has **0 signatures** — strong on existence, useless for chaining past a method (§7, score 3). |
| **10** | Implement Python's dormant decorator effects (`@property`→getter return, user-decorator return substitution) + apply `cbm_type_substitute` on generic-container members | `type_registry.h:12,37-38` (promised); `ts_lsp.c:1612-1627` (substitute) | `py_lsp.c` | Flags are set but never read; the header's own contract is unmet (§6, score 3). |

**Honorable mention (extraction-layer, not resolver):** wire Perl into `lang_specs.c` `*_decorator_types[]` + `service_patterns.c:320` route table (Dancer2/Mojolicious/Catalyst) + `.t`/`.psgi` test mapping (`cbm_is_test_file`, `helpers.c:393`) — high user-visible value, but it belongs in the extraction layer, not `perl_lsp.c` (§6).

---

## Appendix — citation index (grep-confirmed anchors, audit-fresh)

**Shared machinery:** `type_rep.h:9-149` (CBMType, 30 kinds), `type_registry.h:29-76` (Registered Func/Type), `:98-103` (fallback/Tier-2), `:146-154` (read_only seal), `scope.h:16` (callable_qn), `scope.c:45-89` (bind + fail-closed), `scope.h:36,45,55` (depth caps), `lsp_neg_memo.h:55-140` (neg-memo), `:149-228` (idxmemo), `lsp_node_iter.h:24` (O(n) children), `cbm.h:386-402` (CBMResolvedCall), `lsp_surface.c:87-104` (CBMLSPDef codec).
**Eval engines:** `ts_lsp.c:2079` / `c_lsp.c:1479` / `rust_lsp.c:1459` / `py_lsp.c:151` / `perl_lsp.c:463`. **Chaining helpers:** `ts_lsp.c:1581` (member), `:2005` (signature-for-call), `:2062` (return_type_of). **Caps:** `ts_lsp.c:174,1565,2098`; `c_lsp.c:1476-1477,1486`; `rust_lsp.c:4632,4638`; `py_lsp.c:30`.
**Cross-file:** `ts_lsp.c:5707` (builder), `:5737` (overlay+fallback), `:5810` (standalone); `pass_lsp_cross.c:372` (build_lsp_def), `:431/497/556` (folds), `:823` (import map), `:729-756` (py from-import), `:954` (capability gate), `:1214` (run_one). **Perl absence:** `perl_lsp.h:114` (declared), no impl in `perl_lsp.c`, absent from all of `pass_lsp_cross.c`.
**Emission:** `ts_lsp.c:249`; `rust_lsp.c:4095`; `c_lsp.c:3822`; `perl_lsp.c:628`; `cs_lsp.c:2028-2205` (graded floats); `rust_lsp.h:43-50` (macro ladder). **Neg-memo wiring:** `rust_lsp.c:2690,2779,3591,3702`; `c_lsp.c:2638-2713`. **Walk caps:** `c_lsp.c:3912`; `py_lsp.c:145`; `perl_lsp.c:939,1464`.
**Stdlib:** `go_stdlib_data.c:9,2189,3174,149`; `python_stdlib_data.c:19,214`; `rust_stdlib_data.c:68,38,230,296`; `perl_stdlib_data.c:95,26,368`; `cs_stdlib_data.c:720`; payoff `go_lsp.c:613-619`.
**Framework (extraction layer):** `extract_defs.c:1329,1360,1779,2040`; `service_patterns.c:320-357`; `helpers.c:393`; `lang_specs.c:214-465`; `go_lsp.c:3359` (test-double); `rust_lsp.c:5945-6031` (`#[derive]`); `py_lsp.c:56,83` (decorator flags).
