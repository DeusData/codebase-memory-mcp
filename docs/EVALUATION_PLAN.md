# codebase-memory-mcp — Evaluation Plan (159 Languages)

> **Status:** Working plan document, **for peer review prior to execution**. This is a **plan, not a
> result set** — it defines *how* the next evaluation is run and contains no scores. Execution happens
> **downstream**, after this specification is reviewed; §15 requires a pilot run before the full sweep.
>
> **Single version.** There are no `v1/`, `v8/`, `v(x)/` result directories anymore — that scheme
> is retired. Every run writes to one flat output tree (`eval-results/`) and overwrites the
> previous run. History lives in git, not in versioned folders.

---

## 1. Purpose

Measure how well `codebase-memory-mcp`'s structured knowledge-graph queries answer real developer
questions compared to plain text exploration (Grep / Glob / Read), across **all 159 supported
languages**, and — for the 9 LSP-hybrid languages — how well the deeper capabilities
(**cross-repo intelligence** and **semantic / similarity edges**) actually perform.

Two conditions answer the **same questions** on the **same repository** per language:

| Condition | Tools allowed | Who runs it |
|-----------|---------------|-------------|
| **Graph** (the product) | Only MCP graph tools: `search_graph`, `trace_call_path`/`trace_path`, `query_graph`, `get_code_snippet`, `get_architecture`, `search_code`, `semantic_query` | Hybrid — see §4 |
| **Explorer** (the baseline) | Only `Grep`, `Glob`, `Read` | Backgrounded `Explore` sub-agents |

A third evaluator — an **LLM-as-a-Judge** (§9) — grades both answer sets blind against the actual
source code.

### What changed from the old v8 plan

| Old (v8) | New |
|----------|-----|
| 66 languages | **159 languages** (full `CBM_LANG_*` registry) |
| 5 language *groups* share one 12-question set | **5 bespoke questions per language**, each its own subchapter (§12) |
| Versioned result dirs `v(x)/` | **One** `eval-results/` tree, no versions |
| MCP answered by a budget-capped sub-agent | **Hybrid**: main session orchestrates, graph-only sub-agents answer (§4) |
| Manual grading | **LLM-as-a-Judge**, blinded (§9) |
| No deep capability tests | **Deep-dive block** for the 9 LSP-hybrid languages: cross-repo + semantic/similar (§11) |

---

## 2. Scope: the 159 languages

The supported set is the `CBMLanguage` enum in `internal/cbm/cbm.h` (`CBM_LANG_GO=0` …
`CBM_LANG_CFML`, before `CBM_LANG_COUNT`). The canonical short name used throughout this plan is the
lowercased enum suffix (`CBM_LANG_GO` → `go`, `CBM_LANG_CSHARP` → `csharp`,
`CBM_LANG_COMMONLISP` → `commonlisp`). The complete repo assignment for all 159 is the master table
in §8.

### The 9 LSP-hybrid languages (deep-dive cohort)

These have dedicated hybrid LSP modules under `internal/cbm/lsp/` and therefore type-aware
call/usage resolution, plus they are the only languages where cross-repo and semantic/similar edges
are mature enough to deserve a deep-dive:

`go` · `python` · `typescript` · `java` · `c` · `csharp` · `php` · `kotlin` · `rust`

---

## 3. Question design: 5 bespoke questions per language, mapped to 5 universal dimensions

Each language gets **5 questions written specifically for its repository** (real symbol names, real
files). To keep 159 bespoke languages comparable and aggregatable, every question is tagged with one
of **five universal capability dimensions**. Question *N* of every language always targets
dimension *DN*:

| Dim | Name | What it tests | Primary graph tool |
|-----|------|---------------|--------------------|
| **D1** | Definition / API discovery | Find the public symbols, handlers, exported defs | `search_graph(label=…, min_degree=…)` |
| **D2** | Relationship / call graph | Callers, callees, impl/inheritance, imports, references | `trace_call_path(direction="both")` |
| **D3** | Targeted retrieval | Exact source of one named symbol | `get_code_snippet(qualified_name=…)` |
| **D4** | Architecture / structure | Layering, module/dir map, entry/init points | `get_architecture(aspects=["all"])` |
| **D5** | Cross-cutting / semantic | Domain-pattern search, similarity, config↔code, vocabulary-bridged search | `search_code` / `search_graph(semantic_query=…)` |

This is the backbone that reconciles "fully bespoke" with "comparable": the *wording and targets*
are bespoke; the *dimension* is fixed. Aggregation (§10) rolls up by dimension across all 159
languages, and by language group, without losing per-language specificity. **The dimensions
themselves are not invented — they are anchored to the published software-comprehension literature
and, for the major languages, cross-checked against external repo-level QA benchmarks (§3.1).**

> For config / markup / schema languages (Group E) the dimensions are interpreted structurally:
> D1 = top-level definitions, D2 = cross-file references/includes, D3 = retrieve the largest
> definition, D4 = file/dir organization, D5 = duplication / naming-pattern / config↔code links.
>
> **[CR-7] D5 is NOT aggregated across groups.** Because D5's operational meaning differs by group
> (vector semantic search for code languages vs. duplication/config↔code for Group E), a single
> all-159 D5 rollup would be noise. D5 is reported **within each group only**. D1–D4 remain
> cross-group comparable. Likewise, on languages where a dimension barely applies (e.g. a call graph
> on `csv`/`gitignore`), the chapter says so and that dimension is marked **N/A** rather than forcing
> an unnatural question — N/A questions are excluded from that language's mean, not scored 0.

### 3.1 External validity — questions grounded in the literature, not LLM-invented

A peer reviewer raised the central validity threat for any benchmark whose questions are
model-generated: **what claim do they have to being *typical* developer questions?** We answer this on
two levels.

**(a) D1–D5 are anchored to a citable taxonomy of real developer questions.** They map onto the
canonical catalogue of questions programmers actually ask during software-evolution tasks — **Sillito,
Murphy & De Volder, "Questions Programmers Ask During Software Evolution Tasks" (FSE 2006 / IEEE TSE
2008)**: 44 question types in 4 groups. This is supplemented by **LaToza & Myers, "Hard-to-answer
Questions about Code" (2010)** and **Ko et al., "Information Needs in Collocated Software Development
Teams" (ICSE 2007)**, which emphasise exactly the cross-file/relational questions the graph is built
to answer. Mapping:

| Sillito group | Representative Sillito questions | Our dimension |
|---|---|---|
| 1 — Finding initial focus points | "Which type/function represents X?", "Where is this defined?" | **D1** Definition/API discovery |
| 2 — Building on focus points | "What calls this? What does it call? What implements this interface?" | **D2** Relationship/call graph |
| (reading a focus point) | "What does this code actually do?" (the definition itself) | **D3** Targeted retrieval |
| 3 — Understanding a subgraph | "How are these objects/layers related? How does control reach here?" | **D4** Architecture/structure |
| 4 — Questions over groups of subgraphs | "Where are the cross-cutting concerns / similar code / config↔code links?" | **D5** Cross-cutting/semantic |

So each question's *phrasing and target* are bespoke per language, but its *type* belongs to an
externally-validated set — not a taxonomy we made up. This is the defensible external-validity story
for reviewers.

**(b) For the major languages we reuse / compare against published repo-level QA benchmarks** rather
than relying solely on our own authored questions:

| Benchmark | Scale / langs | Why relevant | Use here |
|---|---|---|---|
| **SWE-QA** (2025, [arXiv 2509.14635](https://arxiv.org/abs/2509.14635)) | 576 repo-level Q-A pairs from 11 repos; categories incl. intention understanding, **cross-file reasoning, multi-hop dependency** | Closest published set to *our* setting (repo-level, relational) | **Primary external set** — near drop-in for the major languages; report Graph vs Explorer against it |
| CoReQA (2025, [arXiv 2501.03447](https://arxiv.org/abs/2501.03447)) | repo-level QA from issues/comments, 176 repos, 4 langs | Independent repo-level QA | Cross-check |
| CodeRepoQA (2024, [arXiv 2412.14764](https://arxiv.org/abs/2412.14764)) | 585k entries from issue conversations, 5 langs | Large dev-knowledge QA | Reference / sampling |
| RepoQA (2024, [arXiv 2406.06025](https://arxiv.org/abs/2406.06025)) | "needle function" long-context retrieval, 50 repos, 5 langs | Retrieval reference | D3-style retrieval baseline |
| CodeQA / CS1QA / CoSQA | snippet-level | Criticised by newer work as too fine-grained | Cited as the *contrast* that motivates structural retrieval |

**Adopted policy (the reviewer's proposal):**
1. **Anchor D1–D5 to Sillito** (the mapping above) for defensible external validity — stated in every
   chapter's framing, not just here.
2. **For the major languages** (the 9 LSP-hybrid + the most popular Group A–D languages), **reuse and
   compare against SWE-QA** where the repo overlaps or a close analogue exists, so part of the score
   rests on an independent, peer-published question set — not only our authored questions.
3. **Generate the multilingual remainder ourselves, but from independent ground truth (LSP symbol /
   reference data and git history) — never from the model.** Question *targets* come from LSP/git
   facts, keeping authoring independent of the system under test. This reinforces the §12
   symmetric-authoring rule and [CR-1].

> This makes the benchmark's question provenance auditable: each question is (i) a Sillito-typed
> developer question, and (ii) for major languages, corroborated by or drawn from SWE-QA; (iii) where
> self-authored, seeded from LSP/git ground truth rather than the model under test.

---

## 4. Execution model (sequential, main-channel)

**One language at a time, in the main channel, never concurrently.** The CBM/graph side of the
evaluation — indexing and answering the 5 questions with graph tools — runs **entirely in the main
channel**. The **only** subagent used during the run is the **Explorer** baseline (it just
Grep/Glob/Reads — it never indexes — so it adds no indexing load). There are **no teams, no graph
subagents, and never two indexes building at once.**

> **Why sequential, not parallel.** Indexing is memory-heavy (a single large repo can use many GB of
> RSS; two cold indexes at once risk OOM — see the indexer memory baseline). Running languages
> concurrently would make the per-language index-time and resource numbers non-comparable and
> could exhaust the box. Sequential is slower but **guarantees clean, comparable, resource-safe
> measurements**. That trade-off is accepted deliberately.

**Per-language loop (strictly ordered):**

```
for each language L in order (skip if manifest says done):
  1. CLONE/locate L's repo (§6); record resolved commit SHA
  2. COLD-INDEX L in the main channel  ── TIME IT (clone+index) → key metric (§5)
       (only one index exists at this point; previous language's index already deleted in step 8)
  3. RECORD graph stats: per-node-type + per-edge-type histogram, zeros kept (§7, §12)
  4. GRAPH condition: the MAIN CHANNEL answers L's 5 questions using ONLY graph tools
       → write eval-results/L-graph.md  (with BENCH markers + metrics)
  5. EXPLORER condition: spawn ONE Explore subagent (Grep/Glob/Read only) on L's repo,
       answering the same 5 questions → it writes eval-results/L-explorer.md, then exits
       (this is the only subagent; it runs while/after step 4 — no indexing, no contention)
  6. WRITE L's per-language report incl. index time + full histogram (§10.4)
  7. JUDGE L (blind, §9) — see note below
  8. DELETE L's index from ~/.cache/codebase-memory-mcp so the next language starts clean,
       then mark L done in manifest.json and move to L+1
```

- **No graph subagent.** The graph answers are produced in the main channel. This is the literal
  "use the product the way a developer does" condition (the main session driving graph tools per the
  Code-Discovery priority order), and it keeps indexing single-threaded.
- **Explorer subagent** (`subagent_type="Explore"`, Grep/Glob/Read only, no `team_name`, no
  `SendMessage`) writes its file as its last action and exits. One at a time, never overlapping the
  next language.
- **Index lifecycle = cold every time.** Deleting L's index in step 8 means L+1 is always a cold
  first-run index — no cache reuse, honest first-run cost. Only one project DB exists at any moment.
- **[CR-4] Atomic completion** for the Explorer file: write + `sync`, then a sentinel
  `eval-results/L.explorer.done`; the main channel waits on the sentinel, never a half-written file.
- **[CR-8] Checkpoint / resume.** `eval-results/manifest.json` records per language
  `{done, index_ms, sha, files, started, finished}`. The loop **skips languages already `done`**, so
  a dropped/context-exhausted session resumes mid-sweep instead of restarting at language 1.

> **Judging (§9) and the no-subagent rule.** The resource concern is about *concurrent indexing*; the
> judge never indexes, so it poses no contention. To preserve blinding (the main channel knows which
> answer it just authored), judging is a **separate downstream phase run after answers are collected
> and the index is already deleted** — i.e. it never overlaps indexing. The judge reads the
> anonymized A/B files (§9.1) with the mapping withheld. This is the one deliberate exception to
> "main channel only"; if you would rather the main channel also judge (accepting non-blind grading),
> say so and §9 changes accordingly.

> **The cross-repo deep-dive (§11.1) is the one case two indexes coexist** — caller + callee for an
> LSP pair. It is still done **sequentially in the main channel**, one LSP language at a time, and
> both indexes are deleted before the next deep-dive. Two small service indexes is well within
> resource limits; 159 concurrent is not.

---

## 5. Metrics captured per language, per condition

> **Headline per-language metric: Index creation time.** For every language the report **always shows
> the time to build the graph** = repo clone time + **cold index time** (ms/s), measured in the main
> channel in step 2 of the §4 loop on a clean cache (previous index already deleted). This is a
> first-class, always-displayed number — it is the cost a developer pays before they can ask a single
> question, and it is the metric most sensitive to repo size and grammar/LSP performance. It is
> recorded in `manifest.json` (`index_ms`), in each chapter's stats block, and in `SUMMARY.md`.

| Metric | How |
|--------|-----|
| **Index time (clone + cold index)** | **KEY METRIC.** Wall-clock to clone + cold-index the repo in the main channel (§4 step 2), always shown per language |
| **Quality** | LLM-judge score 0.0–1.0 per question (§9), averaged over the 5 (N/A dims excluded) |
| **Tokens (narrow)** | Input+output tokens during the answering phase only, between `<!-- BENCH_START -->` (before Q1) and `<!-- BENCH_END -->` (after Q5) markers |
| **Tokens (full-session)** | **[CR-3]** Total tokens the agent consumed end-to-end (incl. orientation: directory listings, initial probes, dead-end reads, formatting). This is the cost a real adopter pays. |
| **Wall-clock** | Delta between the two markers |
| **Tool calls** | Total tool invocations during answering (Graph: per MCP tool; Explorer: Grep/Glob/Read counts) |
| **Zero-result rate** | Questions where the primary tool returned nothing |

**[CR-3] Two token ratios are reported, clearly labeled:** `Token Ratio (answering)` and
`Token Ratio (full-session)`. The full-session ratio is the headline honesty metric — the narrow one
isolates per-question efficiency. The narrow metric excludes only agent spawn/teardown and judging;
it does **not** hide the Explorer's orientation cost (that lives in the full-session metric).

**[CR — asymmetry disclosure]** The two conditions are *not* perfectly symmetric: the Graph agent has
a tool playbook (§12 hints) while the Explorer must orient itself. This is a real product advantage,
not a measurement artifact — but the report states it plainly so readers don't mistake the comparison
for "identical difficulty."

**[CR — zero-result handling]** A zero-result answer is graded by the judge as **FAIL (0.0)** for that
question (there is no claim to verify and the developer got nothing usable) — it is *not* dropped.
N/A dimensions (§3) are different: those are excluded from the mean entirely.

Derived per language: `Token Ratio = Explorer tokens / Graph tokens`,
`Speed Ratio = Explorer time / Graph time`, `Quality Δ = Graph score − Explorer score`.

---

## 6. Phase 0 — Repository setup

```bash
scripts/clone-bench-repos.sh /tmp/bench
```

Repos are cloned shallow (`--depth 1`). Shared repos use symlinks (§8 marks them).
**Selection criteria** for the repo assigned to each language (§8):

1. **Real, popular OSS** — never local samples or private repos (≥1k stars where the ecosystem
   allows; for niche languages, the canonical project for that language).
2. **Substantial, idiomatic content in the target language** — the repo's primary or a large
   secondary language must be the one under test (verified by file count / LOC).
3. **Permissive enough to clone** (public; license noted is not required but availability is).
4. **Bounded size** — prefer repos that index in minutes, not hours; for giant projects
   (linux, llvm, nixpkgs, chromium) a documented **subdirectory** is used instead of the whole tree.
5. **Stable** — pinned by shallow clone at run time; the run records the resolved commit SHA.

Rows flagged **⚠️** in §8 are lower-confidence picks (niche language, sourcehut-only mirror, or
size/subset concerns) that should be validated before the first full run.

---

## 7. Phase 1 — Cold indexing (sequential, interleaved with eval)

Indexing is **not** a bulk upfront pass. Per §4, each language is indexed **just-in-time**, evaluated,
then its index is **deleted** before the next language — so **only one project DB ever exists** and
every index is genuinely cold. The skeleton:

```bash
# Start clean: no leftover DBs, fresh results tree
rm -f ~/.cache/codebase-memory-mcp/*.db
mkdir -p /tmp/eval-results

for lang in $ALL_LANGS; do                      # ALL_LANGS = full 159-name list (§2 / §8)
  manifest_done "$lang" && continue             # [CR-8] skip already-completed languages

  # --- step 2: cold index in the main channel, TIMED (key metric) ---
  t0=$(now_ms)
  scripts/benchmark-index.sh ~/.local/bin/codebase-memory-mcp "$lang" /tmp/bench/"$lang" /tmp/eval-results
  index_ms=$(( $(now_ms) - t0 ))                 # clone+index wall-clock → manifest + report (§5)

  # --- step 3: record per-type histograms (zeros back-filled) ---
  #   node-types.json, edge-types.json  (every label + all 32 edge types, zeros kept, §7 below)

  # --- steps 4-7: GRAPH answers (main channel) + EXPLORER answers (one Explore subagent)
  #               + per-language report + (deferred, blind) judge ---

  # --- step 8: delete THIS language's index so the next is cold, then mark done ---
  rm -f ~/.cache/codebase-memory-mcp/*.db
  manifest_mark_done "$lang" "$index_ms"
done
```

**Index time is captured and always reported** (`index_ms` = clone + cold index), per §5.

**Index mode:** the 9 LSP-hybrid languages are indexed in **`full`** mode (all files **+
similarity/semantic edges**) so the semantic/similar deep-dive (§11) has data; `fast` mode skips
similarity/semantic and must not be used for the deep-dive cohort. Cross-repo edges are built in a
separate `cross-repo-intelligence` pass (§11) — the one place two indexes briefly coexist (§4).

Per-language metrics recorded: file count, LOC, cold index time (ms), project name, resolved commit
SHA, and — **not just totals** — a **per-type breakdown**:

- `node-types.json` — a histogram of **node count by label** (`Function`, `Method`, `Class`,
  `Interface`, `Type`/`Enum`/`Struct`, `Field`, `Variable`, `Route`, `Module`, `Section`, `Macro`,
  `File`, `Folder`) + total.
- `edge-types.json` — a histogram of **edge count by type, with one entry for every one of the 32
  edge types, including those that came back `0`**. The canonical set is the indexer's own
  `ALL_EDGE_TYPES[]` (26 intra-repo types — `tests/test_lang_contract.c`): `CALLS`, `ASYNC_CALLS`,
  `HTTP_CALLS`, `GRPC_CALLS`, `GRAPHQL_CALLS`, `TRPC_CALLS`, `DEFINES`, `DEFINES_METHOD`, `IMPLEMENTS`,
  `INHERITS`, `OVERRIDE`, `DECORATES`, `IMPORTS`, `HANDLES`, `CONFIGURES`, `DEPENDS_ON`, `USAGE`,
  `DATA_FLOWS`, `SEMANTICALLY_RELATED`, `SIMILAR_TO`, `TESTS`, `TESTS_FILE`, `INFRA_MAPS`,
  `FILE_CHANGES_WITH`, `CONTAINS_FILE`, `CONTAINS_FOLDER` — **plus the 6 cross-repo types** from the
  cross-repo pass (`CROSS_HTTP_CALLS`, `CROSS_ASYNC_CALLS`, `CROSS_GRPC_CALLS`, `CROSS_GRAPHQL_CALLS`,
  `CROSS_TRPC_CALLS`, `CROSS_CHANNEL`) and a total. The writer **emits the full 32-type list and
  back-fills missing types with `0`** rather than recording only the types that appeared.

These come straight from `query_graph` (`MATCH (n) RETURN labels(n), count(*)` and
`MATCH ()-[r]->() RETURN type(r), count(*)`) / `get_graph_schema` / `cbm_store_count_edges_by_type`,
then **reconciled against the canonical 32-edge / node-label lists so every type is present**. A
`query_graph` aggregation only returns types that exist, so a present-but-zero row is impossible to
get from the query alone — the metrics writer must add the zeros. **Zero-count types are the point**:
`CALLS=0` is a `CALLS_MISSING` finding (§10.2), `IMPLEMENTS`/`INHERITS=0` means no OO-relation edges
formed, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language is a semantic-index gap —
each only visible because the row is kept at `0`.

---

## 8. Master repository assignment (all 159)

Groups: **A** = Class-based OOP & Contracts · **B** = Systems & Low-level · **C** = Dynamic &
Scripting · **D** = Functional & Formal · **E** = Config/Data/Markup/Schema/Build/Template/HDL/
Shader/Docs (sub-tag in *Notes*). **[LSP]** marks the 9 deep-dive languages. **⚠️** = validate before run.

| # | Language | Grp | Repository | Notes |
|---|----------|-----|------------|-------|
| 1 | go | B | go-chi/chi | **[LSP]** standard chapter; deep-dive uses OTel pair (§11) |
| 2 | python | C | httpie/cli | **[LSP]** |
| 3 | javascript | C | expressjs/express | HTML symlinks here |
| 4 | typescript | C | trpc/trpc | **[LSP]** |
| 5 | tsx | C | shadcn-ui/ui | CSS symlinks here |
| 6 | rust | B | meilisearch/meilisearch | **[LSP]** TOML symlinks here |
| 7 | java | A | spring-projects/spring-petclinic | **[LSP]** SQL/XML symlink here |
| 8 | cpp | B | nlohmann/json | CUDA shares concepts |
| 9 | csharp | A | ardalis/CleanArchitecture | **[LSP]** |
| 10 | php | A | laravel/framework | **[LSP]** (koel/koel alt) |
| 11 | lua | C | awesomeWM/awesome | — |
| 12 | scala | A | playframework/play-samples | — |
| 13 | kotlin | A | JetBrains/Exposed | **[LSP]** |
| 14 | ruby | A | sinatra/sinatra | — |
| 15 | c | B | redis/redis | **[LSP]** Makefile symlinks here |
| 16 | bash | C | bash-it/bash-it | — |
| 17 | zig | B | tigerbeetle/tigerbeetle | — |
| 18 | elixir | D | phoenixframework/phoenix | — |
| 19 | haskell | D | jgm/pandoc | — |
| 20 | ocaml | D | ocaml/dune | — |
| 21 | objc | A | AFNetworking/AFNetworking | — |
| 22 | swift | A | Alamofire/Alamofire | — |
| 23 | dart | A | felangel/bloc | — |
| 24 | perl | C | mojolicious/mojo | — |
| 25 | groovy | A | spockframework/spock | — |
| 26 | erlang | D | ninenines/cowboy | — |
| 27 | r | C | tidyverse/dplyr | — |
| 28 | html | E:markup | ← symlink javascript | benchmarked as Group E |
| 29 | css | E:markup | ← symlink tsx | benchmarked as Group E |
| 30 | scss | E:markup | twbs/bootstrap | — |
| 31 | yaml | E:config | kubernetes/examples | k8s/kustomize symlink here |
| 32 | toml | E:config | ← symlink rust | — |
| 33 | hcl | E:iac | terraform-aws-modules/terraform-aws-eks | — |
| 34 | sql | E:data | dbt-labs/jaffle_shop | ⚠️ small; alt symlink java |
| 35 | dockerfile | E:build | docker-library/official-images | — |
| 36 | clojure | D | clojure/clojure | — |
| 37 | fsharp | D | giraffe-fsharp/Giraffe | — |
| 38 | julia | D | SciML/DifferentialEquations.jl | — |
| 39 | vimscript | C | SpaceVim/SpaceVim | — |
| 40 | nix | C | nix-community/home-manager | (nixpkgs too large) |
| 41 | commonlisp | D | lem-project/lem | — |
| 42 | elm | D | elm/compiler | — |
| 43 | fortran | B | fortran-lang/stdlib | (cp2k too large) |
| 44 | cuda | B | NVIDIA/cuda-samples | — |
| 45 | cobol | B | OCamlPro/gnucobol | ⚠️ test corpus |
| 46 | verilog | E:hdl | YosysHQ/picorv32 | (yosys too large) |
| 47 | emacslisp | C | magit/magit | — |
| 48 | json | E:data | SchemaStore/schemastore | — |
| 49 | xml | E:markup | ← symlink java | — |
| 50 | markdown | E:docs | github/docs | — |
| 51 | makefile | E:build | ← symlink c | — |
| 52 | cmake | E:build | kitware/CMake | — |
| 53 | protobuf | E:schema | googleapis/googleapis | — |
| 54 | graphql | E:schema | graphql/graphql-js | (has .graphql) |
| 55 | vue | E:markup | vuejs/core | — |
| 56 | svelte | E:markup | sveltejs/svelte | — |
| 57 | meson | E:build | mesonbuild/meson | — |
| 58 | glsl | E:shader | repalash/Open-Shaders | — |
| 59 | ini | E:config | ← symlink python | .cfg/.ini |
| 60 | matlab | B | chebfun/chebfun | ⚠️ many .m |
| 61 | lean | D | leanprover-community/mathlib4 | (subset) |
| 62 | form | D | vermaseren/form | ⚠️ .frm examples |
| 63 | magma | D | defeo/ss-isogeny-software | ⚠️ rare; validate |
| 64 | wolfram | D | WolframResearch/WolframLanguageForJupyter | — |
| 65 | solidity | A | OpenZeppelin/openzeppelin-contracts | — |
| 66 | typst | E:docs | typst/packages | — |
| 67 | gdscript | C | godotengine/godot-demo-projects | — |
| 68 | gleam | D | gleam-lang/stdlib | — |
| 69 | powershell | C | PowerShell/PowerShell | (modules subset) |
| 70 | pascal | B | castle-engine/castle-engine | — |
| 71 | dlang | B | dlang/phobos | — |
| 72 | nim | B | nim-lang/Nim | — |
| 73 | scheme | D | gambit/gambit | ⚠️ verify .scm coverage |
| 74 | fennel | C | bakpakin/Fennel | — |
| 75 | fish | C | fish-shell/fish-shell | (.fish functions) |
| 76 | awk | C | e36freak/awk-libs | ⚠️ niche |
| 77 | zsh | C | ohmyzsh/ohmyzsh | — |
| 78 | tcl | C | tcltk/tcllib | — |
| 79 | ada | B | AdaCore/Ada_Drivers_Library | — |
| 80 | agda | D | agda/agda-stdlib | — |
| 81 | racket | D | racket/racket | (subset) |
| 82 | odin | B | odin-lang/Odin | (core lib) |
| 83 | rescript | D | rescript-lang/rescript-core | — |
| 84 | purescript | D | purescript-halogen/purescript-halogen | — |
| 85 | nickel | D | tweag/nickel | (.ncl stdlib) |
| 86 | crystal | A | crystal-lang/crystal | (stdlib) |
| 87 | teal | C | teal-language/tl | — |
| 88 | hare | B | harelang/hare | ⚠️ sourcehut mirror |
| 89 | pony | A | ponylang/ponyc | (packages) |
| 90 | luau | C | luau-lang/luau | ⚠️ tests/*.luau |
| 91 | janet | C | janet-lang/spork | — |
| 92 | sway | A | FuelLabs/sway-applications | — |
| 93 | nasm | B | cirosantilli/x86-bare-metal-examples | — |
| 94 | assembly | B | pret/pokered | (.asm disassembly) |
| 95 | astro | E:markup | withastro/astro | (examples/docs) |
| 96 | blade | E:template | monicahq/monica | (.blade.php) |
| 97 | just | E:build | casey/just | (justfiles) |
| 98 | gotemplate | E:template | prometheus-community/helm-charts | (.tpl) |
| 99 | templ | E:template | a-h/templ | (examples) |
| 100 | liquid | E:template | Shopify/dawn | (.liquid theme) |
| 101 | jinja2 | E:template | sovereign/sovereign | (.j2) |
| 102 | prisma | E:schema | prisma/prisma-examples | (schema.prisma) |
| 103 | hyprlang | E:config | end-4/dots-hyprland | ⚠️ dotfiles |
| 104 | dotenv | E:config | motdotla/dotenv | ⚠️ small fixtures |
| 105 | diff | E:data | void-linux/void-packages | (.patch) |
| 106 | wgsl | E:shader | gfx-rs/wgpu | (.wgsl examples) |
| 107 | kdl | E:config | zellij-org/zellij | (.kdl config) |
| 108 | json5 | E:config | json5/json5 | (tests) |
| 109 | jsonnet | E:config | grafana/jsonnet-libs | — |
| 110 | ron | E:config | ron-rs/ron | ⚠️ tests/examples |
| 111 | thrift | E:schema | apache/thrift | (test/*.thrift) |
| 112 | capnp | E:schema | capnproto/capnproto | (*.capnp) |
| 113 | properties | E:config | ← symlink java | (.properties) |
| 114 | sshconfig | E:config | mathiasbynens/dotfiles | ⚠️ few files |
| 115 | bibtex | E:docs | JabRef/jabref | ⚠️ test .bib |
| 116 | starlark | E:build | bazelbuild/rules_go | (.bzl) |
| 117 | bicep | E:iac | Azure/bicep-registry-modules | (*.bicep) |
| 118 | csv | E:data | vega/vega-datasets | ⚠️ data, not code |
| 119 | requirements | E:config | huggingface/transformers | (requirements*.txt) |
| 120 | hlsl | E:shader | microsoft/DirectX-Graphics-Samples | (.hlsl) |
| 121 | vhdl | E:hdl | VUnit/vunit | ⚠️ examples |
| 122 | systemverilog | E:hdl | lowRISC/ibex | (.sv) |
| 123 | devicetree | E:config | u-boot/u-boot | (.dts subset) |
| 124 | linkerscript | E:config | zephyrproject-rtos/zephyr | (.ld subset) |
| 125 | gn | E:build | flutter/buildroot | ⚠️ (.gn/.gni) |
| 126 | kconfig | E:build | buildroot/buildroot | (Config.in/Kconfig) |
| 127 | bitbake | E:build | openembedded/meta-openembedded | (.bb) |
| 128 | smali | E:data | JesusFreke/smali | ⚠️ tests/*.smali |
| 129 | tablegen | E:data | llvm/llvm-project | ⚠️ subset (lib/Target/X86 *.td) |
| 130 | ispc | B | ispc/ispc | (examples *.ispc) |
| 131 | cairo | A | OpenZeppelin/cairo-contracts | — |
| 132 | move | A | econia-labs/econia | (Move) |
| 133 | squirrel | C | albertodemichelis/squirrel | (samples/*.nut) |
| 134 | func | A | ton-blockchain/token-contract | ⚠️ FunC |
| 135 | regex | E:data | (fixture corpus) | ⚠️ no natural repo — see §8.1 |
| 136 | jsdoc | E:docs | lodash/lodash | (JSDoc-heavy JS) |
| 137 | rst | E:docs | sphinx-doc/sphinx | (.rst) |
| 138 | beancount | E:data | beancount/beancount | ⚠️ examples |
| 139 | mermaid | E:docs | mermaid-js/mermaid | (demos/*.mmd) |
| 140 | puppet | E:iac | puppetlabs/puppetlabs-apache | (.pp) |
| 141 | po | E:docs | django/django | (locale/*.po) |
| 142 | gitattributes | E:config | alexkaratarakis/gitattributes | (collection) |
| 143 | gitignore | E:config | github/gitignore | (collection) |
| 144 | slang | E:shader | shader-slang/slang | (tests/examples) |
| 145 | llvm_ir | B | llvm/llvm-project | ⚠️ subset (.ll tests) |
| 146 | smithy | E:schema | smithy-lang/smithy | (examples) |
| 147 | wit | E:schema | bytecodealliance/wit-bindgen | (.wit) |
| 148 | tlaplus | D | tlaplus/Examples | (.tla) |
| 149 | pkl | E:config | apple/pkl-pantry | (.pkl) |
| 150 | gomod | E:config | ← symlink go | (go.mod) |
| 151 | apex | A | trailheadapps/ebikes-lwc | (Apex .cls) |
| 152 | soql | E:data | ← symlink apex | (embedded SOQL) |
| 153 | sosl | E:data | ← symlink apex | (embedded SOSL) |
| 154 | kustomize | E:iac | kubernetes-sigs/kustomize | (examples) |
| 155 | k8s | E:iac | ← symlink yaml | (apiVersion manifests) |
| 156 | pine | C | pinecoders/pine-utils | ⚠️ niche |
| 157 | qml | E:markup | lirios/lirios | ⚠️ (.qml) |
| 158 | cfscript | A | ortus-solutions/coldbox-platform | (.cfc) |
| 159 | cfml | E:template | ← symlink cfscript | (.cfm) |

### 8.1 Languages without a natural OSS repo

`regex` indexes regular-expression grammar fragments and has no idiomatic standalone project. It is
evaluated against a small curated **fixture corpus** (a directory of representative `.regex`/pattern
files) committed under `tests/eval-fixtures/regex/`, treated as its "repo". Any other ⚠️ row that
cannot be validated falls back to the same fixture-corpus approach and is noted as a coverage
caveat in the final report (never silently dropped).

---

## 9. LLM-as-a-Judge

Grading is done by an **LLM judge agent**, not by hand. This scales to 159 × 5 = 795 questions and
removes human grader drift, at the cost of needing careful bias controls.

### 9.1 What the judge sees

Per question, the judge agent receives:
1. The question text and its dimension (D1–D5).
2. **Two anonymized answers**, labeled `Answer A` and `Answer B` in **randomized order** — the judge
   is **not told which is Graph vs Explorer** (blind). The order is recorded so scores can be
   de-anonymized after grading.
3. **Ground-truth access**: the judge has read-only `Grep`/`Read`/`get_code_snippet` on the actual
   repo to *verify claims*, so it grades against reality, not plausibility. **[CR-9] Verification
   depth scales with the answer**: the judge checks **≥30% of the distinct symbols/paths the answer
   cites (minimum 5)**, not a flat 3–5 — so a long answer isn't rubber-stamped on a tiny sample. The
   **Completeness** sub-score must cite the *enumeration it compared against* (e.g. "answer listed 3
   of the 12 handlers found in `handlers/`"), so Completeness is grounded in a count, not intuition.

### 9.2 Rubric — three sub-scores per answer, each 0.0–1.0

| Sub-score | Question |
|-----------|----------|
| **Correctness** | Do the named symbols / paths / line numbers actually exist and match? (verify a sample) |
| **Completeness** | Does it cover the full scope the question asked, or only a fragment? |
| **Specificity** | Concrete names/paths/lines vs vague prose? |

Per-answer score = mean of the three. The judge must **cite the evidence it checked** for each
sub-score (e.g. "verified `Cart.checkout` exists at `cart.go:88` ✓; claimed 12 handlers, found 11").

### 9.3 Grade bands & per-question score

| Grade | Score | Meaning |
|-------|-------|---------|
| PASS (P) | ≥ 0.80 | correct, complete, specific |
| PARTIAL (/) | 0.40–0.79 | useful but incomplete or partly wrong |
| FAIL (F) | < 0.40 | no useful or fundamentally wrong answer |

### 9.4 Bias controls

- **Blind A/B labeling + randomized order** (above) so the judge can't favor "the graph one".
- **Position-bias check**: a fraction of questions are judged twice with A/B **swapped**; if the
  preference flips, the question is flagged and re-judged.
- **Verification-required**: a claim the judge cannot confirm in source caps Correctness at 0.5.
- **[CR-2] Single disclosed judge model (decided).** One judge model, **named in `SUMMARY.md`**, runs
  all passes with a fixed prompt. Because the Graph/Explorer answers are written by a Claude-family
  agent, a same-family judge carries a documented **10–25% self-preference inflation**. Two things
  follow and are **mandatory**:
  1. **Prefer a judge from a *different* family than the answer-writing agent** (e.g. answers written
     by Claude → judge with a non-Claude model) to minimize self-preference at no extra panel cost.
  2. If the judge *is* same-family, the report must carry an explicit **self-preference caveat** and
     must not present the score as bias-free.
  (A cross-family panel was considered and deferred — see §16.3 fork B. It remains the stronger option
  if cross-provider access is available at execution time; the plan does not block on it.)
- **3 independent passes**, per-question score = **median**; disagreement spread > 0.4 flags the
  question for manual spot-check. *Caveat recorded in the report: 3 passes of one model measure the
  model's consistency, not its bias — they tighten variance, they do not remove self-preference.*
- **Fixed prompt** across all languages for comparability.
- The judge never sees token/time/tool-call metrics — those are scored mechanically (§5), not by the LLM.

### 9.5 Judge output (`eval-results/<lang>-judged.json`)

```json
{
  "language": "go", "repo": "...", "commit": "...",
  "questions": [
    {"dim": "D1", "graph": {"correctness": 1.0, "completeness": 0.8, "specificity": 1.0, "score": 0.93, "grade": "P",
                            "evidence": "verified 11/12 handlers exist ..."},
     "explorer": {"correctness": 0.8, "completeness": 1.0, "specificity": 0.6, "score": 0.80, "grade": "P",
                  "evidence": "..."},
     "judge_spread": 0.10}
  ],
  "graph_score": 0.0, "explorer_score": 0.0
}
```

---

## 10. Aggregation & final report

One run → one report tree under `eval-results/` (no versions). Each `<lang>-judged.json` plus the
mechanical metrics (§5) roll up into:

### 10.1 `eval-results/SUMMARY.md` (shareable, factual, no internal TODOs)

- **Overview**: avg Graph vs Explorer quality, avg tokens, avg time, ratios.
- **Quality by language** (159 rows): group, repo, nodes, edges, Graph score, Explorer score, tier.
- **Graph composition / edge-type coverage matrix** (159 rows × every node label + every edge type):
  the per-type counts from §7, **with explicit `0`s kept**. This matrix is where systemic extraction
  gaps surface at a glance — e.g. a column of `CALLS = 0` across a family of grammars, or
  `IMPLEMENTS = 0` for languages that clearly have interfaces. A heat-map of zeros across languages is
  one of the most actionable artifacts for the dev team and feeds §10.2 directly.
- **Token efficiency by language**: Graph vs Explorer tokens, % reduction, tool calls.
- **Quality by dimension (D1–D5)**: avg Graph vs Explorer across all languages — shows *which kinds*
  of questions the graph wins/loses.
- **Quality by group (A–E)**.
- **Tier distribution** (§10.3).
- **Deep-dive results** for the 9 LSP languages (§11 recall/precision).

### 10.2 `eval-results/IMPROVEMENTS.md` (internal)

For every language where Graph < Explorer, or any FAIL: the failing dimension(s), a 5–15 line code
sample read from the actual repo, and a root-cause tag from
`{LABEL_MISMATCH, EXTRACTION_GAP, CALLS_MISSING, QUERY_STRATEGY, PARSE_ERROR}` with the lang-spec
file (`internal/cbm/lang_specs.c`) and the tree-sitter node type that would fix it. Plus a
priority-ordered recommendations list (severity × languages affected).

### 10.3 Language tier

| Tier | Graph score | Meaning |
|------|-------------|---------|
| A | ≥ 0.67 | strong, competitive with Explorer |
| B | 0.50–0.66 | good, identifiable gaps |
| C | 0.42–0.49 | partial |
| D | < 0.42 | weak; Explorer preferred |

### 10.4 Per-language report (`eval-results/<lang>.md`)

The 5-question table (dim, MCP grade, Exp grade, calls, tokens), totals, ratios, tier, the **full
graph-stats histogram** (`node-types.json` + `edge-types.json` from §7, rendered as the two-column
table from the §12 template — every label and every edge type with its count, zeros included), and a
Failure Analysis section for any FAIL. Deep-dive languages append their §11 block.

---

## 11. Deep-dive: the 9 LSP-hybrid languages

Beyond the 5 standard questions, each of `go, python, typescript, java, c, csharp, php, kotlin, rust`
gets a **deep-dive block** with two extra capability tests. These exist only for LSP-hybrid
languages because only they have type-aware resolution and full-mode similarity/semantic edges.

### 11.1 Cross-repo intelligence

**Capability under test:** `index_repository(mode="cross-repo-intelligence", target_projects=[…])`
matches Routes/Channels across two indexed projects to create `CROSS_HTTP_CALLS`,
`CROSS_ASYNC_CALLS`, `CROSS_CHANNEL` (and gRPC/GraphQL/tRPC) edges; `get_architecture` surfaces a
`cross_repo_links` summary.

**Setup:** each LSP language is given a **caller/callee repo pair** that genuinely calls across a
service boundary. The richest realistic source is the **OpenTelemetry Demo**
(`open-telemetry/opentelemetry-demo`), a polyglot system whose services call each other via
gRPC/HTTP. Each pair = the service written in language *X* + the service it calls; the two service
directories are indexed as **separate projects**, then the cross-repo pass is run.

| Lang | Caller repo/service | Callee repo/service | Source |
|------|---------------------|---------------------|--------|
| go | otel-demo `checkout` (Go) | otel-demo `product-catalog` (Go) | OTel Demo (gRPC) |
| python | otel-demo `recommendation` (Py) | otel-demo `product-catalog` (Go) | OTel Demo (gRPC) |
| typescript | otel-demo `frontend` (TS/Next) | otel-demo `cart` / `checkout` | OTel Demo (gRPC/HTTP) |
| java | otel-demo `ad` (Java) | otel-demo `feature-flag` / others | OTel Demo (gRPC) |
| csharp | otel-demo `accounting`/`cart` (.NET) | otel-demo `checkout` (Go) | OTel Demo (gRPC) |
| php | otel-demo `quote` (PHP) | otel-demo `shipping` (Rust) | OTel Demo (HTTP) |
| kotlin | otel-demo `fraud-detection` (Kotlin) | otel-demo `checkout` (Go) | OTel Demo (Kafka/gRPC) |
| rust | otel-demo `shipping` (Rust) | otel-demo `quote` (PHP) | OTel Demo (HTTP) |
| c | ⚠️ **gap** — no C service in OTel Demo | candidate: `redis/redis` ↔ `redis/hiredis` | protocol, not HTTP routes — validate whether CROSS edges form; if not, mark as a documented capability gap |

> **Honest caveat:** "one genuine cross-repo-call pair per language" is hard. OTel Demo cleanly
> covers 8 of 9 (it natively exercises Go/Python/TS/Java/C#/PHP/Kotlin/Rust). **C has no service in
> the demo** and Redis↔hiredis uses the RESP wire protocol (not HTTP routes), so cross-repo edges
> almost certainly **will not** form — this is reported as a documented capability gap, not hidden.

> **[CR-5] Gating validation (do this FIRST).** Before authoring any cross-repo question, run
> `index_repository(mode="cross-repo-intelligence")` on **one** OTel pair (checkout→product-catalog)
> and confirm `CROSS_HTTP_CALLS`/`CROSS_ASYNC_CALLS` edges actually form when two sub-directories of
> the *same monorepo* are indexed as separate projects. This is the load-bearing assumption for the
> entire deep-dive. **If edges do not form**, the fallback is to use genuinely separate repos
> (independently published client + server, e.g. a service repo + its generated client SDK repo) or
> to report cross-repo as "not evaluable from monorepo splits" — the plan does not proceed on an
> unvalidated assumption.

> **[CR — ground truth, Alt D]** The cross-repo **ground truth** (X1 actual-call set) is built from
> the OTel Demo's own service-topology docs and integration tests where available, not solely from
> the question author reading the caller's stubs — this avoids the "author writes both the answer key
> and grades against it" circularity.

**Cross-repo deep-dive questions (per pair):**

- **X1 (recall):** "List every cross-service call from the caller into the callee — (caller symbol →
  callee route/handler)." Compare against a **ground-truth set** built by manually reading the
  caller's client stubs / request sites. Metric: **recall = found / actual**, **precision =
  correct / found**.
- **X2 (architecture):** "From `get_architecture`, does `cross_repo_links` correctly summarize the
  caller→callee relationship (count, direction)?" Graded P/ /F by the judge against ground truth.

### 11.2 Semantic / similarity edges

**Capability under test:** `full`/`moderate` index mode builds the two semantic edge types —
**`SIMILAR_TO`** (near-duplicate / structural similarity, simhash-seeded) and
**`SEMANTICALLY_RELATED`** (vector/embedding relation) — plus a semantic vector index;
`search_graph(semantic_query=[…])` returns vocabulary-bridged cosine matches in `semantic_results`.
The deep-dive checks both that the **edges exist** (their histogram counts are non-zero, §7) and that
they are **correct/complete** (S1/S2 below).

**Setup:** the language's standard single repo (§8), indexed in **`full`** mode.

**Semantic/similarity deep-dive questions:**

- **S1 (vocabulary bridging):** Pick a concept present under a *synonym* (e.g. the repo says
  "publish"/"emit"; query `semantic_query=["send","dispatch"]`). Did the right functions surface in
  `semantic_results` even though the literal token differs? Metric: **hit@k** against a ground-truth
  set of the real synonym-named functions.
- **S2 (near-duplicate recall):** **[CR-6]** Ground truth is **not** a hand-picked 3–5 pairs (a
  sample that small has no statistical standing and hand-built clone sets are known to be heavily
  mislabelled). Instead: scope to **Type-1/2 near-exact duplicates** (where verification is
  tractable), build the candidate set **reproducibly from the indexer's own simhash output** over the
  full repo, and **independently confirm** each pair by normalized-token diff. Target a
  **≥20-pair** ground-truth set per language (fewer only if the repo genuinely has fewer — noted).
  Metric: **recall** over that confirmed set + **false-positive rate** on a sample of returned pairs.

> "Did we catch *all* of them?" is the point: recall is reported **against a reproducible,
> independently-confirmed ground-truth set** (simhash-seeded, token-diff-verified), so a high score
> means real coverage — not cherry-picking, and not circular (the seed is not graded by the same
> `SIMILAR_TO`/`SEMANTICALLY_RELATED` edges it tests; pairs are re-confirmed by token diff).

### 11.3 Deep-dive output & scoring

Each deep-dive block writes `eval-results/<lang>-deepdive.json`:

```json
{"language": "go",
 "cross_repo": {"pair": ["checkout","product-catalog"], "actual": 7, "found": 6, "recall": 0.86,
                "precision": 1.0, "X2_grade": "P"},
 "semantic":  {"S1_hit_at_5": 0.8, "S2_recall": 0.6, "S2_false_positive_rate": 0.1}}
```

Aggregated into a **Deep-Dive section** of `SUMMARY.md`: a 9-row table of cross-repo recall/precision
and semantic hit@k / duplicate-recall, so regressions in these high-value features are visible
run-over-run (git diff of `SUMMARY.md`).

---

## 12. Per-language chapter template

Every one of the 159 languages gets its **own subchapter** in §14 with this exact structure. The 9
LSP languages additionally include the §11 deep-dive block.

```markdown
### <NN>. <language> — <Group> [LSP?]

**Repo:** <org/repo> (`/tmp/bench/<lang>`)   **Symlink:** <yes→source / no>
**Indexed in:** full | (fast for non-LSP)    **Why this repo:** <1 line tying repo to §6 criteria>

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "<question with this repo's real targets>"
2. **[D2 Relationship]** "<…>"
3. **[D3 Retrieval]** "<… name a real symbol in this repo>"
4. **[D4 Architecture]** "<…>"
5. **[D5 Cross-cutting/Semantic]** "<… domain pattern / config↔code / similarity for this repo>"

**Expected graph tools (hint, not a script):** D1→search_graph(...), D2→trace_call_path(...), ...

**Graph stats (filled at index time).** Report **every node label and every edge type on its own
row, with its count — including a `0` where the index produced none.** Never group types and never
omit a zero row: a `0` is critical information (it pinpoints exactly which construct the grammar/LSP
failed to extract for this language). The canonical sets are the indexer's own
`ALL_EDGE_TYPES[]` (26 intra-repo types, `tests/test_lang_contract.c`) **+** the 6 cross-repo types
from the cross-repo pass = **32 edge types**, and the `get_graph_schema` node labels.

_Node-type histogram:_

| Node label | Count |
|---|---|
| Function | _ |
| Method | _ |
| Class | _ |
| Struct | _ |
| Interface | _ |
| Trait | _ |
| Enum | _ |
| Type | _ |
| Field | _ |
| Variable | _ |
| Route | _ |
| Module | _ |
| Section | _ |
| Macro | _ |
| File | _ |
| Folder | _ |
| **Total nodes** | _ |

_Edge-type histogram (all 32 edge types listed every time, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Zero rows are mandatory, not optional. `CALLS = 0` flags broken call-extraction (a `CALLS_MISSING`
> signal, §10.2); `IMPLEMENTS`/`INHERITS`/`OVERRIDE = 0` flags missing OO relations;
> `SIMILAR_TO`/`SEMANTICALLY_RELATED = 0` on a full-mode LSP language flags a semantic-index gap. The
> `CROSS_*` rows are `0` for non-LSP languages (no cross-repo pass) and carry real counts only for the
> 9 LSP deep-dive pairs (§11.1).

**Output of the analysis for this language:**
- `eval-results/<lang>-graph.md`    — 5 answers from the Graph condition (+ BENCH markers, metrics)
- `eval-results/<lang>-explorer.md` — 5 answers from the Explorer condition
- `eval-results/<lang>-judged.json` — LLM-judge scores (§9.5)
- (LSP only) `eval-results/<lang>-deepdive.json` — §11.3

**How this language's result aggregates:** its 5 D-scores feed the by-dimension and by-group
rollups (§10); its tier is computed from the mean Graph score; (LSP only) its deep-dive feeds the
Deep-Dive section.
```

> **[CR-1] Symmetric authoring (the most important validity rule).** Questions must reference *real*
> identifiers, but **how those identifiers are discovered determines fairness.** If every question
> were authored by browsing the graph, the test would only ever ask about symbols the graph already
> indexed successfully — invisibly hiding the graph's misses. So authoring is split by dimension:
>
> | Dim | Authored by | Rule |
> |-----|-------------|------|
> | **D1, D3** | **Grep-first author** | Find the target symbols by **text search only** (`grep`/`Read`), never via the graph. Guarantees the symbol is findable both ways, so the graph gets no head start. |
> | **D2, D4** | Graph-first author | May use `trace_call_path`/`get_architecture` to pick a meaningful central symbol/layer. |
> | **D5** | Either | D5 is **openly graph-favoring** (semantic/similarity/config↔code) and is labeled as such in the report — not presented as neutral. |
>
> A symbol used in a D1/D3 question that the graph later fails to return is a *finding*, not a bug in
> the question — that's exactly the gap symmetric authoring is designed to expose.
>
> **Pinning.** During authoring, the repo's resolved commit SHA is recorded and baked into
> `clone-bench-repos.sh`, so the run indexes the *same* HEAD the questions were written against.
>
> §14 contains two fully-worked exemplars now; the remaining 157 are generated against their cloned
> repos (§15) following this authoring split.

---

## 13. Reproducibility

```bash
# 1. Clone all 159 repos (shallow; skip existing)
scripts/clone-bench-repos.sh /tmp/bench

# 2. Cold index all 159 (LSP cohort in full mode)
rm -f ~/.cache/codebase-memory-mcp/*.db
mkdir -p /tmp/eval-results
for lang in $ALL_LANGS; do
  scripts/benchmark-index.sh ~/.local/bin/codebase-memory-mcp "$lang" /tmp/bench/"$lang" /tmp/eval-results
done

# 3. Cross-repo pass for the 9 LSP pairs (index each service dir, then cross-repo-intelligence)
#    index_repository(mode="cross-repo-intelligence", target_projects=[callee])  — see §11.1

# 4. Graph phase   — spawn graph-only sub-agents per language  (writes <lang>-graph.md)
# 5. Explorer phase— spawn Explore sub-agents per language     (writes <lang>-explorer.md)
# 6. Judge phase   — spawn blind LLM-judge per language        (writes <lang>-judged.json)
# 7. Deep-dive     — 9 LSP languages (writes <lang>-deepdive.json)
# 8. Aggregate     — SUMMARY.md + IMPROVEMENTS.md + per-language reports (no version dir)
```

`scripts/clone-bench-repos.sh` and `scripts/benchmark-index.sh` must be extended from 66 → 159
languages (and the symlink/subset rules in §8 added). That script change is part of executing this
plan, tracked in §15.

---

## 14. Per-language chapters

> Two annotated exemplars below establish the format (one LSP with deep-dive, one standard). The
> **complete, QA-reviewed set of all 159 chapters is [Appendix A](#appendix-a--per-language-chapters-159)**
> at the end of this document. Each chapter is a *draft specification* — its 5 questions are finalized
> grep-first against the pinned commit at execution time per the §12 authoring split.

### 1. go — B (Systems) **[LSP]**

**Repo:** go-chi/chi (`/tmp/bench/go`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** popular (~19k★), idiomatic Go, a router library whose
public API, middleware chain, and tree-routing internals exercise D1–D5 cleanly.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "I'm auditing chi's public surface. List every exported function/method
   on the `Mux` and `Router` types, and the package-level constructors (`NewRouter`, `NewMux`)."
2. **[D2 Relationship]** "Trace `Mux.ServeHTTP` both ways: what does it call to route a request
   (down to `tree.FindRoute`), and who calls it?"
3. **[D3 Retrieval]** "Show me the full source of `(*node).findRoute` in `tree.go` — I need to
   understand the radix-tree walk before changing it."
4. **[D4 Architecture]** "Describe chi's layering: how do `Mux`, `Router`, `middleware`, and `tree`
   relate, and where is the request entry point?"
5. **[D5 Cross-cutting/Semantic]** "Find all middleware constructors (the `middleware` package's
   `func(http.Handler) http.Handler` wrappers) — where is cross-cutting behavior added?"

**Expected graph tools:** D1→`search_graph(label="Method", qn_pattern=".*Mux.*", min_degree=1)`;
D2→`trace_call_path(function="...Mux.ServeHTTP", direction="both")`;
D3→`get_code_snippet(qualified_name=".../tree.(*node).findRoute")`;
D4→`get_architecture(aspects=["all"])`;
D5→`search_graph(label="Function", file_pattern="middleware/*.go")`.

**Deep-dive (LSP):**
- **Cross-repo:** pair = OTel-Demo `checkout` (Go) → `product-catalog` (Go), indexed as two projects.
  - *X1 recall*: enumerate gRPC calls from checkout into product-catalog (`GetProduct`,
    `ListProducts`, …); ground truth from checkout's client stubs. Report recall/precision.
  - *X2*: does `get_architecture.cross_repo_links` show checkout→product-catalog with correct count?
- **Semantic/similarity** (chi, full index):
  - *S1*: `semantic_query=["dispatch","route"]` should surface `findRoute`/`routeHTTP` though the
    literal token differs — hit@5 vs ground truth.
  - *S2*: known near-duplicates (e.g. the per-method `Get/Post/Put/...` registration wrappers in
    `mux.go`) — do `SIMILAR_TO`/`SEMANTICALLY_RELATED` recover the family? recall + false-positive check.

**Output:** `eval-results/go-graph.md`, `go-explorer.md`, `go-judged.json`, `go-deepdive.json`.
**Aggregates into:** D1–D5 rollups, Group B, Go tier, and the Deep-Dive section.

---

### 19. haskell — D (Functional)

**Repo:** jgm/pandoc (`/tmp/bench/haskell`)   **Symlink:** no
**Indexed in:** fast (non-LSP)   **Why this repo:** large, popular (~33k★), heavily idiomatic
Haskell with many readers/writers, typeclasses, and a clear module hierarchy — strong D1–D5 coverage.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the exported top-level functions of `Text.Pandoc.App` and the
   reader/writer entry points (`readMarkdown`, `writeHtml5`, …) that form pandoc's public API."
2. **[D2 Relationship]** "Trace `convertWithOpts` both ways: what reader/writer pipeline does it
   call, and who invokes it from the executable's `main`?"
3. **[D3 Retrieval]** "Show the source of the `Reader` data type / the `getReader` function in
   `Text.Pandoc.Readers` — I need its full definition."
4. **[D4 Architecture]** "Describe pandoc's module structure: how do `Readers`, `Writers`,
   `Definition` (the AST), and `App` relate from input to output?"
5. **[D5 Cross-cutting/Semantic]** "Find pandoc's typeclass instances and the AST-walking functions
   (`walk`, `query` over the `Pandoc`/`Block`/`Inline` types) — where is the document transformed?"

**Expected graph tools:** D1→`search_graph(label="Function", file_pattern="src/Text/Pandoc/App*.hs")`;
D2→`trace_call_path(function="convertWithOpts", direction="both")`;
D3→`get_code_snippet(qualified_name="Text.Pandoc.Readers.getReader")`;
D4→`get_architecture(aspects=["all"])`;
D5→`search_code("instance ")` + `search_graph(name_pattern=".*walk.*|.*query.*")`.

**Output:** `eval-results/haskell-graph.md`, `haskell-explorer.md`, `haskell-judged.json`.
**Aggregates into:** D1–D5 rollups, Group D, Haskell tier. (No deep-dive — not an LSP language.)

---

## 15. Execution checklist (to run this plan)

**Gate 0 — de-risk before bulk authoring (do these first):**
- [ ] **[CR-5]** Validate cross-repo edges form on ONE OTel pair (checkout→product-catalog) indexed
      as two projects. If not, switch to genuinely-separate repos or report cross-repo as a gap.
- [ ] **[Fork A]** Decide scope: **pilot** (9 LSP + ~10 representative others) vs full 159 now.
- [ ] **[Fork B]** Decide judge: cross-family panel vs single disclosed model.

**Build-out:**
- [ ] Extend `scripts/clone-bench-repos.sh` to all 159 (symlinks + subset rules from §8); pin SHAs.
- [ ] Extend `scripts/benchmark-index.sh` `ALL_LANGS` to 159; force `full` mode for the LSP cohort.
- [ ] Add manifest-based **skip/resume** (§4 CR-8) and the `.done` sentinel protocol (§4 CR-4).
- [ ] Validate every **⚠️** repo pick (availability, language content, size).
- [ ] Build the `regex`/fixture-corpus directories (§8.1).
- [ ] Generate per-language chapters (§12 template) against cloned repos using the **symmetric
      authoring split** (CR-1): D1/D3 grep-first, D2/D4 graph-first, D5 labeled graph-favoring.
- [ ] **[Ext. validity, §3.1]** Map each chapter's 5 questions to their Sillito group; for the major
      languages, wire in **SWE-QA** items (and CoReQA/RepoQA where useful) as an independent question
      set and report Graph/Explorer against them; seed self-authored multilingual questions from
      LSP/git ground truth, not the model.
- [ ] Confirm the C cross-repo pair forms CROSS edges or document the gap (§11.1).

**Run:**
- [ ] Run Phases B–E with checkpointing; produce `eval-results/SUMMARY.md` (name the judge model[s]),
      `IMPROVEMENTS.md`, per-language reports, and the Deep-Dive section.

---

## 16. Challenger review

> Per project policy, this plan's methodology is reviewed by the `challenger` agent before it is
> finalized. The full report is inserted here verbatim. Fixes adopted from it are marked
> **[CR-n]** at their section; two strategic forks (§16.1) remain open decisions.

### 16.1 Challenger-driven changes adopted

| # | Finding | Change | Where |
|---|---------|--------|-------|
| CR-1 | Questions authored from graph-discovered symbols bias D1/D3 toward graph-visible code | **Symmetric authoring**: D1 & D3 questions are authored *grep-first* (symbols found by text search, never via the graph); D2 & D4 graph-first; D5 is openly graph-favoring and labeled as such | §12 |
| CR-2 | Single-family judge → 10–25% self-preference inflation | **Single disclosed judge model, preferring a non-Claude family**; bias caveated; cross-family panel kept as an execution-time upgrade (§16.3) | §9.4 |
| CR-3 | Excluding Explorer orientation tokens flatters the Token Ratio | Report **both** a narrow (answering-phase) and a **full-session** token metric | §5 |
| CR-4 | File-existence polling can read a half-written file | **Atomic completion**: agent writes output, fsyncs, then writes a `<lang>.<phase>.done` sentinel; the orchestrator polls the sentinel only | §4 |
| CR-5 | "OTel sub-dir = project" may not form CROSS edges | **Gate**: validate CROSS edges on one OTel pair *before* authoring the rest of the deep-dive | §11.1 |
| CR-6 | 3–5 hand-picked duplicate pairs have no statistical standing | **S2 ground truth from the indexer's own simhash clusters** (reproducible), scoped to Type-1/2 near-exact dupes; minimum 20-pair set | §11.2 |
| CR-7 | D5 means different things per group → cross-group rollup is noise | **D5 is reported within-group only**, never aggregated across all 159 | §3, §10 |
| CR-8 | No checkpoint/resume; one dropout restarts from zero | **Completion manifest** (`eval-results/manifest.json`); every phase skips already-`.done` languages | §4, §13 |
| CR-9 | Verification samples 3–5 of N claims → Completeness ungrounded | Judge verification sample **scales with claim count** (≥30% of cited symbols, min 5); Completeness must cite the enumeration it compared against | §9.1 |

### 16.2 Full challenger report (verbatim)

> The report below is reproduced in full, unedited, as required by project policy.

#### Challenge Review: EVALUATION_PLAN.md — codebase-memory-mcp 159-Language Benchmark

##### What Looks Good

The three-pass median judge with disagreement-spread flagging (§9.4) is a sound baseline for LLM-as-a-judge consistency. Documenting the C cross-repo gap explicitly rather than papering over it (§11.1 caveat) is intellectually honest. Tying judging to ground-truth verification with capped Correctness at 0.5 for unverifiable claims (§9.4) is the right instinct, even if the implementation has holes (see below). The per-language tier system (§10.3) gives actionable output rather than a single aggregate number.

##### Assumptions to Verify

**Assumption 1: the "bespoke questions written against real symbols" guarantee neutrality.**
The plan states questions are written after Phase 0/1 once `search_graph`/`get_architecture` confirm actual identifiers (§12, authoring note). The person writing the questions therefore already used the graph to discover the symbols. This systematically biases D1, D2, and D3 questions toward symbols the graph indexed successfully. A function the graph missed — because of a parse error, label mismatch, or LSP gap — cannot appear in the question, because the author would never have discovered it that way. The Explorer is then asked to find things the graph already confirmed it can find. Consequence: quality gaps for underserved symbols are invisible in the data.

**Assumption 2: "identical overhead cancels out" justifies excluding spawn/teardown tokens (§5).**
This only holds if overhead is truly equal. The Explorer is an `Explore` sub-agent (open-ended, unlimited tool calls, no instructions about how to be efficient). The Graph agent is a graph-only agent with a narrow, well-specified tool set. The Explorer will consume more context just orienting itself — directory listings, initial Glob probes, dead-end reads. Excluding this overhead hides a real cost that a developer deciding "should I use the MCP?" would pay. The token ratio reported will favor Graph artificially, independently of quality.

**Assumption 3: file-existence polling is reliable at scale (§4).**
The plan detects agent completion by polling for `<lang>-graph.md`/`<lang>-explorer.md` existence. A file can be partially written when the polling check fires (write syscall in progress). There is no described fsync barrier, atomic rename, or completion-marker protocol (e.g., writing a `.done` sentinel after a final `fsync`). At 159 × 3 agents per batch, a race here corrupts the input to the judge phase silently. The plan notes the v8 lesson about `SendMessage` killing agents mid-write — this is the exact same failure mode, shifted from shutdown to detection.

**Assumption 4: "one OTel Demo sub-directory = one project" validly exercises cross-repo intelligence.**
This is the most critical structural assumption. The cross-repo feature (`CROSS_HTTP_CALLS`, `CROSS_ASYNC_CALLS`) is designed for separately maintained codebases with independent deploy cycles. Indexing two service directories from the same monorepo as "separate projects" is an artificial split. The monorepo may share proto definitions, a common `pb.go` directory, or Bazel build rules that the indexer would normally traverse. Whether the CROSS edges even form in this configuration has not been validated — the plan asks for it (§15) but doesn't answer it. If they don't form, 8 of 9 deep-dives measure nothing meaningful.

**Assumption 5: the 3-pass judge is immune to self-preference bias.**
The plan specifies "one judge model, fixed prompt" (§9.4) but does not name the judge model, and the two conditions' answers were presumably written by Claude sub-agents. If the judge is from the Claude family, self-preference bias of 10–25% has been empirically demonstrated across Llama/Claude/GPT pairs (Zheng et al., 2024; Panickssery et al., 2024). Three passes of the same biased model gives a precise median of a biased number, not an unbiased estimate.

##### Alternatives Worth Considering

**Alternative A: Tiered evaluation — deep on 9 LSP + stratified sample of Group E (not all 159).**
795 hand-written questions across 159 languages is an enormous authoring investment. Most of the actionable signal lives in: (a) the 9 LSP languages where the graph has type-aware edges, (b) the ~20 popular languages where regressions would be noticed by users, and (c) a representative handful of Group E config/data languages. The other ~110 niche languages (fennel, squirrel, smali, beancount, pine, hyprlang…) will score similarly to each other and to a naive Grep baseline because the graph cannot index them structurally beyond token counting. A stratified sample of 30–40 languages would likely yield the same tier distribution with 1/4 the cost and would be rerunnable on demand. Trade-off: loses the "covers all 159" marketing claim. But a poorly-executed 159-language sweep is weaker evidence than a rigorous 40-language one.

**Alternative B: Symmetric question authoring — half written from grep discovery, half from graph discovery.**
To remove the symbol-discovery bias identified in Assumption 1, split the 5 questions: questions for D1 and D3 (concrete retrieval) are written by a Grep-first author who finds symbols by text search without touching the graph, and questions for D2 and D4 are written by a graph author. D5 is intentionally graph-favoring (semantic/similarity) — acknowledge that openly rather than presenting it as neutral. Trade-off: doubles authoring complexity per language but removes the most serious validity threat.

**Alternative C: Use a heterogeneous judge panel (different model families).**
Run three passes with three different judge models (e.g., GPT-4o, Claude, Gemini) rather than three passes with the same model. Per-question score is the cross-family median. Self-preference bias cancels rather than compounds. This is a well-established pattern in adversarial evaluation (MT-Bench, Chatbot Arena). Trade-off: 3× judge API cost, requires cross-provider API access. The cost is real but the bias problem is also real — a single-model judge's 3-pass median is not a substitute.

**Alternative D: Use the OTel Demo's own integration-test fixtures as cross-repo ground truth.**
The OTel Demo ships its own integration tests and service topology documentation, which manually enumerate which service calls which endpoint. This is a pre-existing, publicly defensible ground-truth source that avoids the "author reads the code and writes the ground truth" circularity problem in §11.1's X1 metric. Trade-off: ground truth scope is bounded by what the integration tests assert, which may miss some call paths.

**Alternative E: Use BigCloneBench or a validated corpus for S2 near-duplicate ground truth.**
The plan proposes "3–5 known near-duplicate / copy-pasted function pairs found by manual read or simhash dump" as the S2 ground truth. This is a sample of 3–5 pairs per language, constructed by the same team running the evaluation. The academic literature has demonstrated that hand-built near-duplicate ground truth is systematically biased and mislabelled even at scale (Krinke 2022, arXiv 2505.04311 — 93% mislabelling rate for weak Type-3/4 clones in BigCloneBench). A 3-pair sample has no statistical standing whatsoever. Better alternatives: use simhash-computed clusters on the full repo as pseudo-ground-truth (at least reproducible), or scope S2 to only exact/near-exact duplicates (Type-1/2) where manual verification is tractable.

##### Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Questions authored from graph discovery bias D1/D2/D3 toward graph-visible symbols | High | High | Symmetric authoring (Alt B) or explicit caveat in methodology |
| Single-family judge produces self-preference inflation of 10–25% | High | High | Cross-family judge panel (Alt C) |
| File-existence polling causes silent partial-write corruption at scale | Medium | High | Atomic sentinel file (`.done`) written only after the output file is flushed |
| OTel monorepo sub-dirs do not form CROSS edges; 8/9 deep-dives are void | Medium | High | Validate a single pair before committing to this design; document fallback |
| C cross-repo pair (redis/hiredis, RESP protocol) produces 0 CROSS edges | High | Medium | Already flagged — treat as documented gap; consider using a WASM/Wasm-C host if a genuine C HTTP service pair can be found |
| 159-language sweep is not completable in one session without checkpointing | High | Medium | Add explicit checkpoint/resume logic to the script; describe failure-recovery in §13 |
| ~30 flagged ⚠️ repos unavailable, too small, or wrong language on run day | Medium | Medium | Validate all ⚠️ rows before authoring questions; fallback fixture corpus per §8.1 |
| Shallow clone at run time produces a different HEAD than during question authoring | Medium | Medium | Pin repos by commit SHA during authoring; bake SHA into `clone-bench-repos.sh` |
| 3-pass median of same judge hides variance; passes are correlated not independent | Medium | Medium | Cross-family panel or acknowledge limitation explicitly in §9 |
| Explorer spawn overhead excluded but material; Token Ratio misleads | Medium | Medium | Include full-session token cost as a second metric; label the narrow metric clearly |

##### Operational Concerns

**Scale / execution feasibility.**
159 × (clone + cold index + graph agent + explorer agent + 3 judge passes) = at minimum 159 × 5 agent invocations = 795 sub-agent sessions, plus 159 × 3 = 477 judge passes. Cold indexing alone for 159 repos, some requiring "full" mode (semantic edges, vector index), will be measured in hours for languages like Haskell/pandoc, Python/httpie, or any repo with millions of lines. The plan gives no clock estimate, no session-continuation strategy if the main session times out, and no checkpoint format beyond "the file exists." One session dropout at language 87 means re-running from language 1 unless someone adds explicit resume logic. This is the single largest operational risk.

**The Explorer "unlimited tool calls" vs Graph "fixed tool set" asymmetry.**
The Graph agent knows exactly which tools to call: `search_graph` → `trace_call_path` → `get_code_snippet`. It has a clear playbook (the "expected graph tools" hint in §12). The Explorer has no playbook — it is a general `Explore` sub-agent that must orient itself, guess directory structure, and iteratively refine searches. These are not symmetric difficulty conditions. The Graph agent benefits from structured orientation that is invisible to the evaluation because the tool responses come pre-structured. This is not a flaw in the product — it is a real advantage. But the plan presents it as a neutral "same questions, same time window" comparison, which it is not.

**The D5 dimension is structurally not neutral.**
D5 is defined as "Cross-cutting / Semantic" with the primary tool listed as `search_code` / `search_graph(semantic_query=…)`. For Group E (config/markup/schema) languages, the plan reinterprets D5 as "duplication / naming-pattern / config↔code links." This re-interpretation is ad hoc and dimension-specific — the D5 graph tool advantage does not transfer to these languages in the same way. Aggregating D5 scores across Group A–E languages with such different operational definitions will produce a meaningless cross-group rollup.

**No described failure threshold for an individual run.**
If the Graph agent returns zero results on D2 (zero-result rate flagged in §5), does the run for that question still count? Is a zero-result a FAIL automatically, or does the judge still grade it? The rubric says Correctness is capped at 0.5 for unverified claims, but a zero-result has no claim to verify. This edge case likely produces a judge confusion artifact at scale.

**The judge's ground-truth verification is token-expensive and shallow.**
§9.1 says the judge verifies a "sample 3–5 cited symbols/files/lines per question." At 795 questions × 3 judge passes = 2,385 judge invocations, each doing live Grep/Read on the actual repo, this is non-trivial API cost and time. More importantly, sampling 3–5 symbols out of an answer that may cite 20–30 means 85%+ of the answer's claims go unverified. An answer that correctly identifies 3 of 12 handlers will pass the Correctness sample check at 1.0 (all 3 sampled claims correct) while a Completeness-aware judge would score it low. The plan splits Correctness and Completeness into separate sub-scores, which partially addresses this — but the verification step only feeds Correctness, leaving Completeness graded purely on the LLM's intuition about "full scope."

##### Research Findings

- Self-preference bias (10–25% inflation) in same-family LLM judges: arXiv 2410.21819; "Quantifying and Mitigating Self-Preference"; FutureAGI bias mitigation survey
- Frontier models exceed 50% error rates on advanced bias tests: Adaline LLM-as-a-Judge reliability
- Position bias (judge flips preference when A/B swapped, even GPT-4): LLM-as-a-Judge biases (sebastiansigl.com)
- Near-duplicate benchmark ground truth — BigCloneBench 93% mislabelling rate, "BigCloneBench Considered Harmful": arXiv 2505.04311; Krinke IWSC22
- Benchmark over-optimism from design choice multiplicity: arXiv 2106.02447
- Code search: grep vs. semantic divergence; only 20–30% solutions overlap: GrepRAG arXiv 2601.23254
- OTel Demo services and languages (confirmed): OpenTelemetry Demo Services
- Multi-agent file-polling reliability risks, event-driven vs. polling: Multi-Agent Reliability Patterns
- Redis/hiredis uses RESP (not HTTP routes), so CROSS edges cannot form: Redis RESP Protocol; Hiredis GitHub

##### Questions for the Developer

1. **Question authoring source of truth (§12 authoring note):** When you write "questions must cite real symbols, so they are filled in during Phase 0/1" — do you mean you will use the graph to discover those symbols, or will you independently verify them with Grep? If graph-first, you have the bias I described. What is your plan to ensure D1/D3 questions target symbols that Grep can also find?
2. **Judge model identity (§9.4):** What model will be the judge? If it is any Claude model, the same-family self-preference effect applies to every Claude-written Graph and Explorer answer. Have you considered a cross-family judge rotation, or at minimum disclosing the judge model in the report so readers can calibrate?
3. **CROSS edge formation in OTel sub-dirs (§11.1, §15):** Before writing 157 more language chapters, have you actually run `index_repository(mode="cross-repo-intelligence")` on two OTel service sub-dirs and confirmed that CROSS_HTTP_CALLS edges form? This is the load-bearing question for the entire deep-dive block. What is the fallback plan if they don't?
4. **Session continuity (§13):** What happens when the main session context window fills up or hits the usage limit at language 94? Is there a described checkpoint format — e.g., a manifest of completed languages that `clone-bench-repos.sh` can consult to skip already-done languages — or does the whole run restart from zero?
5. **D5 cross-group comparability (§3, §8):** You aggregate D5 scores across all 159 languages. But D5 for Go means `semantic_query=["dispatch","route"]` surfacing functions from a vector index. D5 for gitignore means "naming-pattern / config↔code links." These are different operations using different graph tools. Do you actually intend the cross-language D5 rollup in §10.1 to be meaningful, or is it cosmetic?
6. **S2 ground truth (§11.2):** "3–5 known near-duplicate function pairs" — how will you construct this set for each of the 9 LSP languages? Will you use the simhash output the indexer already produces, or is this a manual read? A 3-pair sample with no inter-rater agreement cannot support a recall claim. What is the minimum ground-truth size you consider credible?
7. **Token exclusion policy (§5):** If a developer is deciding whether to adopt codebase-memory-mcp, they pay the full session cost, including agent spawn, orientation, and formatting. Why should the reported "Token Ratio" exclude the Explorer's orientation cost? Would you consider reporting both the narrow metric and the full-session metric?
8. **The 159-vs-tiered question (over-engineering check):** The plan acknowledges ~30 ⚠️ repos need validation before questions can be written. For languages like `regex` (fixture corpus), `csv` (data, not code), `dotenv` (a handful of fixture files), `sshconfig` (few files), what meaningful D1–D5 question set can be written? Is the signal from these languages worth the cost of the infrastructure to collect it?

##### Recommendation

The evaluation's most serious flaw is not operational — it is that the bespoke questions will almost certainly be written from graph-discovered symbols (because the authoring process says "do Phase 0/1 first"), making the graph condition a partial answer to its own test. Combined with a single-family judge that carries 10–25% self-preference bias, the result will be a well-executed benchmark that systematically overstates the graph's advantage. Fix these two issues — symmetric question authoring and a cross-family judge panel — before running the full 159-language sweep, or the SUMMARY.md will not be a credible external artifact. The 159-language scope is also premature given that ~30 repos need validation and session-continuity infrastructure does not yet exist; a pilot run on the 9 LSP languages plus 10 representative Group E languages would de-risk the methodology before committing to 795 hand-written questions.

### 16.3 Strategic forks — resolved

- **Fork A — scope: RESOLVED → full 159, specified in this document; execution is downstream.**
  This document is a **specification to be peer-reviewed before any run**. It therefore enumerates all
  159 languages with complete chapters (§14). The challenger's pilot recommendation is preserved as
  an **execution-time** safeguard: §15 Gate 0 requires a pilot (9 LSP + ~10 representative others) to
  validate the methodology *before* the downstream team commits to the full sweep. So: the *plan*
  covers 159; the *first run* should still be a pilot.
- **Fork B — judge: RESOLVED → single disclosed model** (§9.4), preferring a non-Claude family to
  limit self-preference, with the bias caveat stated in the report. A cross-family panel remains a
  documented upgrade path if cross-provider access exists at run time.



---

# Appendix A — Per-language chapters (159)

> Each chapter is a *draft specification* (per §12): repo + 5 dimension-tagged questions
> (symmetric-authoring split), the per-type graph-stats block (all 32 edge types, zeros
> kept), and — for the 9 LSP languages — the cross-repo + semantic/similarity deep-dive.
> Final question symbols are confirmed grep-first against the pinned commit at execution.

---

### 1. go — B (Systems & Low-level) **[LSP]**

**Repo:** go-chi/chi (`/tmp/bench/go`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** ~19k-star, dependency-free idiomatic Go HTTP router with a non-trivial radix-tree core and a real middleware ecosystem — substantial yet self-contained, matching the plan's "popular + idiomatic + buildable in isolation" repo-selection criteria for the Systems group.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the public API entry points for constructing and configuring a router: where are `NewRouter`, the `Router` interface, and the `Mux` type defined, and what is each one's file?" (all three are grep-findable exported identifiers — `NewRouter` and the `Router` interface in `chi.go`, the `Mux` type in `mux.go` — so plain text search can recover them too)
2. **[D2 Relationship]** "Trace the call graph around route registration: what does `(*Mux).handle` call on the way to inserting into the radix tree (expect a path to `(*node).InsertRoute` in `tree.go`), and who calls `(*Mux).handle` upstream (expect `Method`, `MethodFunc`, and the verb helpers `Get`/`Post`/...)?"
3. **[D3 Retrieval]** "Retrieve the full body of `(*node).findRoute` in `tree.go` — the radix-tree lookup that resolves a request path to a handler." (`findRoute` is a real, grep-findable method name)
4. **[D4 Architecture]** "Describe the package/file organization: the root `chi` package vs. the `middleware/` subpackage, and how the routing core (`mux.go`, `tree.go`, `chain.go`, `context.go`) is separated from the optional middleware stack."
5. **[D5 Cross-cutting/Semantic]** "*(graph-favoring)* Across the `middleware/` package, surface the handler-wrapping middleware constructors that share the `func(http.Handler) http.Handler` shape — e.g. `Logger`, `Recoverer`, `RealIP`, `Compress`, `Throttle` — using semantic similarity rather than a literal substring, to find the cross-cutting 'wrap an http.Handler' pattern even where the token 'middleware' never appears in the function name."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="NewRouter|Router|Mux", label="Function|Interface|Struct"); D2->trace_call_path(name="(*Mux).handle", direction=both); D3->get_code_snippet(qualified_name="github.com/go-chi/chi/v5.(*node).findRoute"); D4->get_architecture(project="go"); D5->search_code/semantic_query(["wrap","http.Handler","middleware","intercept"]).

**Deep-dive (LSP):**
- **Cross-repo:** Pair = OpenTelemetry Go checkout (`otel` Go services) -> `product-catalog` (Go), linked via **gRPC**.
  - **X1 (recall):** From the caller's generated client stubs (the `*Client` interfaces in the OTel demo's `genproto`/`pb` package, e.g. `ProductCatalogServiceClient.GetProduct`, `.ListProducts`, `.SearchProducts`) enumerate every cross-service call route caller->callee. Ground truth = the stub method set cross-checked against the OTel demo service-topology docs (checkout/frontend -> product-catalog edges). Report **recall = found/actual** and **precision** (found-correct / found-total). Target: recall >= 0.7 on the gRPC stub edges; precision penalizes spurious links to non-product-catalog services.
  - **X2:** Does `get_architecture.cross_repo_links` summarize the caller->callee link with a **count** and **direction** (caller=OTel checkout, callee=product-catalog)? Graded **P** (count+direction both correct) / **partial** (link present but count or direction wrong/missing) / **F** (no cross_repo_link emitted).
- **Semantic/similarity** (go-chi/chi indexed in FULL mode):
  - **S1 (vocabulary bridging):** `semantic_query=["extract","path parameter","url variable"]` should surface chi's parameter-access API where the literal tokens differ — ground-truth set = `URLParam`, `URLParamFromCtx`, `RouteContext`, `(*Context).URLParam`. Metric **hit@5** against that synonym ground-truth set (a "wildcard/route variable" vocabulary that does not literally match the function names).
  - **S2 (near-duplicate recall):** Ground truth built reproducibly from the indexer's own simhash output over chi (>= 20 Type-1/Type-2 clone pairs, each token-diff confirmed). Prime candidates: the near-identical verb-helper methods (`Get`/`Post`/`Put`/`Delete`/`Patch`/`Head`/`Options`/`Connect`/`Trace` on `*Mux`, all thin wrappers over `handle`/`Method`) and the parallel `*Func` registration variants. Metric **recall** (simhash-seeded pairs recovered) + **false-positive rate** (flagged pairs whose token-diff exceeds the Type-2 threshold).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/go-graph.md`, `go-explorer.md`, `go-judged.json`, `go-deepdive.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the go tier, and the Deep-Dive section.

---

### 2. python — C (Dynamic & Scripting) **[LSP]**

**Repo:** httpie/cli (`/tmp/bench/python`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** ~36k-star, widely-used CLI HTTP client — idiomatic, substantial (multi-package: `cli/`, `output/`, `plugins/`, `manager/`) Python that exercises decorators, dynamic dispatch and plugin entry points, matching the plan's "popular + idiomatic + non-trivial size" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "In `httpie/client.py`, locate the definitions of `build_requests_session` and `collect_messages`, and report their fully-qualified names, file, and line range. (Both are top-level `def`s — grep-findable as `def build_requests_session` / `def collect_messages`.)"
2. **[D2 Relationship]** "Trace the call graph around `httpie.core.program` in both directions: which functions does `program` call (e.g. `collect_messages` in `client.py`), and which functions reach `program` (expected inbound: `main` -> `raw_main` -> `program`)?"
3. **[D3 Retrieval]** "Retrieve the full source of the `HTTPResponse` class in `httpie/models.py` (grep-findable as `class HTTPResponse`), including its `iter_body`, `iter_lines`, `metadata`, and `version` members."
4. **[D4 Architecture]** "Describe the package/module structure of the `httpie` package: the top-level modules (`core.py`, `client.py`, `models.py`, `sessions.py`, …) versus the sub-packages (`cli/`, `output/`, `plugins/`, `manager/`, `internal/`, `legacy/`), and how the entry layer (`__main__.py` -> `core.main`) sits above them."
5. **[D5 Cross-cutting/Semantic]** "*(graph-favoring)* Across `httpie/client.py`, surface the functions that assemble/normalize an outgoing request — including the request-building helpers whose names do NOT contain the token 'header', namely `make_request_kwargs`, `make_send_kwargs`, and `make_send_kwargs_mergeable_from_env` (each merges or normalizes headers/body into the request kwargs) — and group them with the explicitly-named header helpers (`make_default_headers`, `finalize_headers`, `transform_headers`, `apply_missing_repeated_headers`). Plain grep for 'header' recovers the four header-named helpers but UNDER-recalls the `make_*_kwargs` mergers that do header/body normalization without the literal token; semantic similarity should bridge that vocabulary gap."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="build_requests_session|collect_messages", label="Function"); D2->trace_call_path(qualified_name="httpie.core.program", direction="both"); D3->get_code_snippet(qualified_name="httpie.models.HTTPResponse"); D4->get_architecture(project="python"); D5->search_code/search_graph(semantic_query=["build http request","merge request headers and body"]).

**Deep-dive (LSP):**
- **Cross-repo:** pair = OTel **recommendation service (Python)** -> **product-catalog service (Go)** via gRPC (OpenTelemetry demo topology).
  * **X1 (recall):** enumerate every cross-service caller->callee route the graph reports for recommendation->product-catalog; ground truth = the gRPC client stub usage in the recommendation service. The recommendation server holds a `ProductCatalogServiceStub` and invokes exactly ONE method on it — `ListProducts` (called twice inside `get_product_list`); it does NOT call `GetProduct` (that edge belongs to other services such as frontend/checkout, not recommendation). So the single ground-truth edge is recommendation -> product-catalog via `ListProducts`. Cross-checked against the OTel demo service-topology docs. Metric: recall = found/actual, plus precision (spurious links / total reported). Target: recall >= 0.8, precision >= 0.9.
  * **X2:** does `get_architecture.cross_repo_links` summarize the recommendation->product-catalog link with correct **count** and **direction** (Py caller -> Go callee, not reversed)? Graded P (count+direction correct) / partial (link present but count or direction off) / F (link absent).
- **Semantic/similarity** (httpie/cli, indexed FULL):
  * **S1 (vocabulary bridging):** `semantic_query=["serialize response to disk","save body to file"]` should surface the download/streaming writers (e.g. `Downloader` in `httpie/downloads.py`, write/stream helpers) although the literal tokens differ from "download". Metric: hit@5 against a synonym ground-truth set ({download, write, stream-to-file} <-> their real symbols). Target hit@5 >= 0.8.
  * **S2 (near-duplicate recall):** ground truth built reproducibly from the indexer's simhash output (>=20 Type-1/2 clone pairs, each confirmed by token-diff) — e.g. the parallel `iter_body`/`iter_lines` overrides across `HTTPResponse`/`HTTPRequest`/`HTTPMessage`. Metric: recall over the seeded pair set + false-positive rate (reported pairs not in ground truth). Target recall >= 0.7, FP rate <= 0.2.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/python-graph.md`, `python-explorer.md`, `python-judged.json`, `python-deepdive.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the python tier, and the Deep-Dive section.

---

### 3. javascript — C (Dynamic & Scripting)

**Repo:** expressjs/express @ `4.x` (`/tmp/bench/javascript`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The most-depended-upon Node.js web framework (tens of millions of weekly npm installs); its small, idiomatic `lib/` core (factory function, prototype-extended `app`/`req`/`res`, a layered router) is a compact yet representative sample of real-world dynamic JavaScript, matching the plan's "popular + substantial + idiomatic" repo-selection criteria. Branch pinned to `4.x` because the questions exercise the in-tree `lib/router/` subpackage; on the default branch (`master`, Express 5.x) the router was extracted to an external `router` package and `lib/router/` no longer exists, which would invalidate Q2/Q4 symbols.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the `createApplication` factory function exported by `lib/express.js`, and list the public application methods defined on the `app` prototype in `lib/application.js` (e.g. `app.use`, `app.listen`, `app.set`, `app.handle`). All are grep-findable identifiers (`function createApplication`, `app.use = function`, ...)."
2. **[D2 Relationship]** "Map the request-dispatch call graph in both directions around the router prototype's `handle` method [verify: source symbol is `proto.handle` in `lib/router/index.js`, not literally `Router.prototype.handle`]: which application entry point reaches it (e.g. `app.handle`) and which layer/route methods it in turn invokes (e.g. `Layer.prototype.handle_request` [verify], `Route.prototype.dispatch` [verify]). These router symbols exist on the `4.x` branch only."
3. **[D3 Retrieval]** "Retrieve the full source of the `res.sendFile` method defined on the response prototype in `lib/response.js` (a single, well-known named symbol; grep-findable as `res.sendFile = function`)."
4. **[D4 Architecture]** "Describe the module/directory architecture of the `lib/` core: how `express.js`, `application.js`, `request.js`, `response.js`, and the `router/` subpackage (`index.js`, `route.js`, `layer.js`) [verify: `lib/router/` is 4.x-only] are organized and depend on one another."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find functions semantically related to HTTP content negotiation / response serialization across files — e.g. `res.json`, `res.jsonp`, `res.send` (in `lib/response.js`) and `req.accepts` (in `lib/request.js`) — that share behavior despite living in different prototypes, and surface the `lib/utils.js` helpers they reuse (e.g. content-type/etag helpers). Labeled graph-favoring: relies on semantic similarity + cross-file reuse, not a single grep token."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="createApplication|app\\.(use|listen|set|handle)"); D2->trace_call_path(name="Router.prototype.handle", direction="both") [verify exact indexed name; fall back to proto.handle / router.handle]; D3->get_code_snippet(qualified_name="res.sendFile"); D4->get_architecture(scope="lib/"); D5->search_code("HTTP content negotiation and response serialization helpers") + search_graph for cross-file reuse of lib/utils.js helpers.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/javascript-graph.md`, `javascript-explorer.md`, `javascript-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the javascript tier.

---

### 4. typescript — C (Dynamic & Scripting) **[LSP]**

**Repo:** trpc/trpc (`/tmp/bench/typescript`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** ~37k-star, end-to-end-typesafe RPC monorepo (pnpm workspaces) whose heavy use of generics, builder patterns, and cross-package re-exports makes it an idiomatic, substantial TypeScript stress test — matching the plan's "popular + idiomatic + large enough to expose resolution gaps" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the public API entry points of the server core: locate the definition of the `initTRPC` builder, the `TRPCError` class, and the `callProcedure` function in `packages/server`. Report their qualified names, kind (function/class), and defining file." (all three are well-known, grep-findable tokens)
2. **[D2 Relationship]** "Trace the call graph around `callProcedure` (direction=both): what invokes it on the request path (e.g. `resolveResponse` / `resolveHTTPResponse` [verify]) and what it calls inward (input parsing, the resolver, `getErrorShape` [verify])? Report immediate callers and callees with depth 2."
3. **[D3 Retrieval]** "Retrieve the full source of `getErrorShape` [verify] from `packages/server` (the function that maps a thrown error to the wire-format error object). Return the exact body with its line range." (fallback symbol if absent: `getTRPCErrorFromUnknown` [verify])
4. **[D4 Architecture]** "Describe the monorepo's package/dir organization: enumerate the `packages/*` workspaces (`server`, `client`, `react-query`, `next`, `react` [verify]) and the dependency direction between `@trpc/client` and `@trpc/server`. Which package defines the link abstraction (`httpBatchLink`, `httpLink`)?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find all functions that *construct or normalize a tRPC error from an unknown thrown value* even when they don't literally contain the substring 'getErrorShape' — i.e. semantic siblings of error-shaping (`getTRPCErrorFromUnknown` [verify], `getErrorFromUnknown` [verify], `TRPCError` construction sites). Rank by semantic similarity to the concept 'convert thrown value into typed RPC error'."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="initTRPC|TRPCError|callProcedure", label="Function|Class"); D2->trace_call_path(qualified_name=".*callProcedure", direction="both", depth=2); D3->get_code_snippet(qualified_name=".*getErrorShape"); D4->get_architecture(project="typescript"); D5->search_code(semantic_query=["convert thrown value into typed RPC error","normalize unknown error to TRPCError"]).

**Deep-dive (LSP):**
- **Cross-repo:** Pair = OpenTelemetry demo **frontend (TS/Next)** -> **cart/checkout** services via gRPC/HTTP.
  * **X1 (recall):** From the frontend's generated client stubs + OTel service-topology docs, enumerate the ground-truth caller->callee routes (e.g. `frontend -> CartService.GetCart`, `frontend -> CartService.AddItem`, `frontend -> CheckoutService.PlaceOrder` [verify]). Run cross-repo trace and report recall = found/actual and precision = correct/returned over that GT set.
  * **X2:** Inspect `get_architecture.cross_repo_links`: does it summarize the frontend->cart/checkout link with a correct edge **count** and **direction** (frontend as source)? Graded **P** (count+direction right) / **partial** (one wrong) / **F** (missing or reversed).
- **Semantic/similarity (trpc/trpc, indexed in FULL mode):**
  * **S1 (vocabulary bridging):** `semantic_query=["middleware chain","interceptor pipeline","request hook"]` should surface the procedure-middleware machinery (`createMiddlewareFactory` [verify], `middlewares` pipeline in `procedureBuilder` [verify]) although the literal token may be `middleware`/`pipe`. Metric: **hit@5** against a synonym GT set {middleware factory, procedure pipe, resolver wrapper}.
  * **S2 (near-duplicate recall):** Build GT reproducibly from the indexer's simhash output (>=20 Type-1/2 clone pairs, token-diff confirmed) — expected hot spots: the repeated `httpLink`/`httpBatchLink` request-builder bodies and the per-package `index.ts` re-export barrels. Metric: **recall** = detected/GT-pairs and **false-positive rate** over flagged pairs.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/typescript-graph.md`, `typescript-explorer.md`, `typescript-judged.json`, `typescript-deepdive.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the typescript tier, and the Deep-Dive section.

---

### 5. tsx — C (Dynamic & Scripting)

**Repo:** shadcn-ui/ui (`/tmp/bench/tsx`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred React/tsx codebases on GitHub; a substantial, idiomatic pnpm monorepo (`apps/v4` Next.js app + a large `registry/` of real TSX components), matching the plan's criterion of popular, non-trivial, idiomatic repos per language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the `cn` class-name helper (in `apps/v4/lib/utils.ts` [verify]) and the `buttonVariants` export created with `cva` in the button component — return their declaring files and signatures. Both are plain grep-findable identifiers (`export function cn`, `buttonVariants = cva`)."
2. **[D2 Relationship]** "For the `Button` component (`apps/v4/registry/new-york-v4/ui/button.tsx` [verify]), show the full relationship view (direction=both): what it calls/imports (e.g. `cn`, `Slot`, `cva`) and which TSX files consume `Button`."
3. **[D3 Retrieval]** "Retrieve the complete source of `buttonVariants` — the `cva(...)` call defining the variant/size class maps — as a single focused snippet. (A single well-known named symbol, grep-findable.)"
4. **[D4 Architecture]** "Describe the monorepo's top-level architecture: the `apps/v4` Next.js app vs the `registry/` component tree vs the `lib/` helpers [verify], and how the directories are organized."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Semantically find the components that follow the `cva`-based variant pattern (a `*Variants` export consumed via `cn(...)` + a `VariantProps` typed prop) across the registry, surfacing this cross-cutting idiom rather than a single literal match."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="cn|buttonVariants"); D2->trace_call_path(name="Button", direction="both"); D3->get_code_snippet(qualified_name=".*buttonVariants"); D4->get_architecture(project="tsx"); D5->search_code/semantic_query("cva variant pattern with VariantProps and cn").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/tsx-graph.md`, `tsx-explorer.md`, `tsx-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the tsx tier.

---

### 6. rust — B (Systems & Low-level) **[LSP]**

**Repo:** meilisearch/meilisearch (`/tmp/bench/rust`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** A ~50k-star, production search engine that is large, idiomatic, and workspace-structured (multi-crate Cargo workspace with async Tokio actix-web HTTP, LMDB storage, and a custom query engine) — exercises the plan's "popular + substantial + idiomatic" repo-selection criteria for systems Rust.

> Repo note: this tree ships TOML symlinks (Cargo manifest / config symlinks). The crate `.rs` sources themselves are not symlinked, so D1–D5 target real source files; the symlink caveat only affects manifest discovery and is flagged where relevant.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the request-handler function `search_with_url_query` and the index-document handler `add_or_replace_documents` in the `meilisearch` crate's routes module — return their signatures and defining files." (Both are concrete `async fn` route handlers under `crates/meilisearch/src/routes/indexes/`; grep-findable by exact name.)
2. **[D2 Relationship]** "Show the full inbound+outbound call neighborhood of the milli search entry point: which HTTP handlers reach it and which lower-level functions (e.g. `execute_search` / ranking-rule evaluation) it calls. In milli, search is driven by the `Search` builder (`Search::new(...).execute()`), not a method on `Index` — resolve the actual entry symbol." [verify: confirm whether the entry is `milli::Search::execute` or `milli::search::new::execute_search`; do not assume a `milli::Index::search` method exists]
3. **[D3 Retrieval]** "Retrieve the complete source of the `SearchQuery` struct (the deserialized search-request parameters type defined in the `meilisearch` crate's search module, `crates/meilisearch/src/search/`)." (One named symbol; `SearchQuery` is a real, grep-findable struct.)
4. **[D4 Architecture]** "Produce the workspace-level architecture: the crate boundaries (`meilisearch`, `milli`, `index-scheduler`, `meilisearch-auth`, `meilisearch-types`, `dump`) and the dependency direction between the HTTP layer, the scheduler, and the storage/query engine."
5. **[D5 Cross-cutting/Semantic]** "**[graph-favoring]** Without using the literal token `ranking`, surface the functions that implement result ordering / relevancy scoring (e.g. ranking-rule application, sort, proximity/typo scoring) via semantic query." (Openly graph-favoring: relies on embedding/semantic match, not exact-token grep.)

**Expected graph tools (hint, not a script):** D1->`search_graph(name_pattern="search_with_url_query|add_or_replace_documents", lang="rust")`; D2->`trace_call_path(qualified_name="<resolved milli search entry>", direction="both")`; D3->`get_code_snippet(qualified_name="...::SearchQuery")`; D4->`get_architecture(project="rust")`; D5->`search_code(semantic_query="result ordering relevancy scoring proximity")` / `search_graph(semantic_query=...)`.

**Deep-dive (LSP):**
- **Cross-repo:** **N/A as a true cross-*repo* HTTP edge — meilisearch is a single, standalone search engine; it ships no second service that issues HTTP calls into it, so there is no `caller-repo -> callee-repo` link to score.** Instead, the LSP cross-boundary capability is exercised **intra-workspace, cross-crate**, which is the realistic analogue for this monorepo:
  - **X1 (cross-crate recall):** enumerate every cross-crate call `caller-crate::fn -> callee-crate::fn` on the HTTP->engine path (e.g. `meilisearch::routes::indexes::search::* -> index_scheduler::* -> milli::*`), computing `recall = found/actual` and `precision`. Ground truth built from the `meilisearch` route handlers' resolved `use` paths plus the workspace `Cargo.toml` dependency graph (which crate may depend on which). Expected ≥1 directed cross-crate edge from the `meilisearch` HTTP layer into `index-scheduler` and from `index-scheduler` into `milli`.
  - **X2:** check whether `get_architecture.cross_repo_links` (or the cross-crate dependency summary) reports the `meilisearch -> index-scheduler -> milli` chain with edge **count** and **direction**; graded **P** (count+direction correct) / **partial** (link present, count or direction wrong) / **F** (absent).
- **Semantic/similarity:** Repo = `meilisearch/meilisearch`, indexed **full**.
  - **S1 (vocabulary bridging):** `semantic_query=["fault tolerance", "retry on failure", "idempotent reprocessing"]` should surface the index-scheduler's task retry / batch re-run functions even though the literal tokens differ; metric **hit@5** vs a hand-built synonym ground-truth set (5–8 target functions in `index-scheduler`).
  - **S2 (near-duplicate recall):** ground truth built reproducibly from the indexer's own **simhash** output — collect ≥20 Type-1/Type-2 clone pairs (e.g. the many near-identical per-route `analytics` aggregator impls and serde `Deserr` boilerplate), each token-diff confirmed; metric **recall** (pairs recovered / 20) + **false-positive rate** (reported pairs that fail token-diff confirmation).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/rust-graph.md`, `rust-explorer.md`, `rust-judged.json`, `rust-deepdive.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the rust tier, and the Deep-Dive section.

---

### 7. java — A (Class-based OOP & Contracts) **[LSP]**

**Repo:** spring-projects/spring-petclinic (`/tmp/bench/java`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** The canonical Spring Boot reference app (~7k stars, used in official Spring docs and countless tutorials); small but idiomatic layered OOP (controllers, repositories, JPA entities, interfaces) that exercises Group A's class/interface/contract dimensions, matching the plan's "popular + idiomatic + substantial-enough" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the public methods (controller request-mapping handlers) declared on the class `OwnerController`, including `processCreationForm` and `showOwner`, with their declaring file and line range." (grep-findable: the literal token `OwnerController` and `processCreationForm` appear verbatim in `src/main/java/org/springframework/samples/petclinic/owner/OwnerController.java`.)
2. **[D2 Relationship]** "Trace the full call graph (both directions) around `OwnerRepository.findById` — which controller methods invoke it (inbound) and what JPA/Spring Data plumbing it reaches (outbound)?" Expect the inbound callers inside `OwnerController` to be `showOwner` and the private helper `findOwner` (note: `processUpdateOwnerForm` calls `OwnerRepository.save`, not `findById`); `initUpdateOwnerForm` reaches `findById` only via `findOwner` [verify].
3. **[D3 Retrieval]** "Retrieve the exact source of the method `Owner.addVisit` (the `Owner` aggregate's method that attaches a `Visit` to a `Pet` by id — signature `addVisit(Integer petId, Visit visit)`)." Single named symbol, grep-findable as `addVisit` in `owner/Owner.java`.
4. **[D4 Architecture]** "Describe the package/module layout of the application: the domain packages (`owner`, `vet`, `system`, `model`), how entity base classes (`BaseEntity`, `NamedEntity`, `Person`) are layered, and where the `*Controller` / `*Repository` pairs live."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find all repository interfaces that extend Spring Data's `JpaRepository`/`Repository` contract across packages (`OwnerRepository`, `VetRepository`, `PetTypeRepository`) without grepping for the literal `extends JpaRepository` — surface them by their structural role as persistence contracts." Labelled graph-favoring (structural contract/IMPLEMENTS reasoning, not a single literal token).

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*OwnerController.*", label="Method"); D2->trace_call_path(qualified_name="...OwnerRepository.findById", direction="both"); D3->get_code_snippet(qualified_name="...owner.Owner.addVisit"); D4->get_architecture(); D5->search_code/search_graph(semantic_query="persistence repository contract", relationship="IMPLEMENTS").

**Deep-dive (LSP):**
- **Cross-repo:** Pair = OpenTelemetry Astronomy Shop **frontend** → **ad service (Java)**, via gRPC. (The Java ad service is a gRPC *server* — `AdServiceGrpc.AdServiceImplBase`, method `getAds` — and accesses feature flags via flagd/OpenFeature, not a gRPC feature-flag stub; so the graphable cross-service edge is the inbound call into the ad service, not an outbound one.) **X1 (recall):** enumerate every cross-service call caller->callee route landing on the ad service's gRPC entrypoint (`oteldemo.AdService/getAds`, served by `AdServiceImpl.getAds` [verify]) and compare against the OTel demo service-topology docs; report recall = found/actual and precision = correct/found. Ground truth = the published OTel topology edges incident to the ad service ∩ the ad service's exported gRPC method set. **X2:** check whether `get_architecture.cross_repo_links` summarizes the frontend->ad-service link (edge count, direction caller ⇒ Java-ad); graded P (count+direction correct) / partial (link present, count or direction wrong) / F (absent).
- **Semantic/similarity:** Own repo spring-petclinic, FULL mode. **S1 (vocabulary bridging):** `semantic_query=["save owner", "persist customer", "store pet owner record"]` should surface `OwnerController.processCreationForm` + `OwnerRepository.save` though the literal token is "Owner"/"create", not "save customer"; metric hit@5 against a hand-built synonym ground-truth set (owner↔customer, save↔persist↔store, find↔lookup). **S2 (near-duplicate recall):** ground truth built reproducibly from the indexer's simhash output — collect ≥20 Type-1/2 clone pairs (e.g. the near-identical `findById`/`findAll` boilerplate and the parallel `*Controller` form-validation blocks), token-diff confirmed; metric = recall of those seeded pairs + false-positive rate (flagged-but-not-clone / total flagged).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/java-graph.md`, `java-explorer.md`, `java-judged.json`, `java-deepdive.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the java tier, and the Deep-Dive section.

---

### 8. cpp — B (Systems & Low-level)

**Repo:** nlohmann/json (`/tmp/bench/cpp`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred C++ libraries on GitHub (~40k+ stars), a single-header JSON library that is idiomatic, heavily template-metaprogrammed, and substantial — matching the plan's criterion of popular, real-world, structurally rich Systems/low-level C++.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the public API surface of the library's core type: find the definition of the `basic_json` class template and its primary type alias `json`, plus the member function `parse`. All three are grep-findable identifiers in `single_include/nlohmann/json.hpp`."
2. **[D2 Relationship]** "Map the relationships around the entry point `basic_json::parse` [verify: exact QN is namespace/version-sensitive — `basic_json` is a class template behind the `json` alias and lives in an inline ABI/version namespace, so the resolvable method path may be e.g. `nlohmann::basic_json::parse` or a versioned variant]: trace (direction=both) which functions it calls into (e.g. the `parser`/`sax` machinery) and which API surfaces or operators call it, to confirm the call graph links the public `parse` API to the internal parser implementation."
3. **[D3 Retrieval]** "Retrieve the full source of the SAX-style parser class `nlohmann::detail::parser` (or the streaming `json_sax` interface). Return the exact definition with its line boundaries — a single named symbol that also exists verbatim in the amalgamated header."
4. **[D4 Architecture]** "Describe the architecture/structure of the repo: the split between the developer-facing modular sources under `include/nlohmann/` (e.g. `detail/`, `thirdparty/`) and the generated `single_include/nlohmann/json.hpp` amalgamation, plus the `tests/`, `docs/`, and `tools/` (amalgamation) layout. Which directories hold the canonical source vs. the build artifact?"
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING) Semantically locate the error/exception-handling cross-cut: find the family of exception types and where they are thrown — e.g. `parse_error`, `type_error`, `out_of_range`, `invalid_iterator` — and the `JSON_THROW`/`JSON_TRY` macro usage that wires them in. This favors semantic/similarity search because the concept ('how does the library report errors') spans many files and macro-obscured throw sites rather than one grep token."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*basic_json.*|.*::parse$"); D2->trace_call_path(qualified_name="nlohmann::basic_json::parse" [verify], direction="both"); D3->get_code_snippet(qualified_name="nlohmann::detail::parser"); D4->get_architecture(...); D5->search_code/semantic_query("exception types thrown on parse/type errors; JSON_THROW macro").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/cpp-graph.md`, `cpp-explorer.md`, `cpp-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the cpp tier.

---

### 9. csharp — A (Class-based OOP & Contracts) **[LSP]**

**Repo:** ardalis/CleanArchitecture (`/tmp/bench/csharp`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** High-star canonical .NET reference template (clean layering, DDD aggregates, CQRS via MediatR, FastEndpoints) — idiomatic and substantial C#, satisfying the plan's "popular + idiomatic + non-trivial size" repo-selection criteria for Group A.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the in-repo classes/interfaces that make up the sample aggregate's API surface — e.g. the `Contributor` entity class, the `EfRepository` implementation class, and the locally-declared contracts in `Core/Interfaces` (`IAggregateRoot`, `IDeleteContributorService`, `IEmailSender`) — and report each one's declaring file and kind (class vs interface). All cited names are plain-text greppable in `src/` (`grep -rn 'class Contributor' src/`, `grep -rn 'class EfRepository' src/`, `grep -rn 'interface IDeleteContributorService' src/`). NOTE: `IRepository<T>`/`IReadRepository<T>` are implemented by `EfRepository` but are *external* (Ardalis.Specification NuGet) and are deliberately excluded from the in-repo grep list."
2. **[D2 Relationship]** "Trace inbound and outbound calls of `EfRepository` (the generic `IRepository<T>`/`IReadRepository<T>` implementation): which use-case handlers / services depend on it (inbound, e.g. `DeleteContributorService` and the MediatR handlers under `UseCases/Contributors`), and which framework/specification members it reaches (outbound, e.g. the inherited `RepositoryBase<T>` / Ardalis.Specification `ApplySpecification`). Direction = both."
3. **[D3 Retrieval]** "Return the exact source of the `Contributor` aggregate root (`Contributor` class in `Clean.Architecture.Core.ContributorAggregate`), including its (primary) constructor, the `UpdateName` method, and the domain-event registration inside `UpdateName`." (Single named symbol; greppable via `grep -rn 'public class Contributor' src/`, which matches the primary-constructor declaration `public class Contributor(ContributorName name) : ...`.)
4. **[D4 Architecture]** "Describe the layer structure of the solution: the `Clean.Architecture.Core` / `.UseCases` / `.Infrastructure` / `.Web` projects, the dependency direction between them (Web -> UseCases -> Core; Infrastructure -> Core), and where DI wiring lives (`Program.cs` plus the `Add*Configs` service-configuration extension methods [verify]; SharedKernel building blocks come from the external Ardalis.SharedKernel package, not an in-repo project [verify]). Confirm the dependency rule (inner layers know nothing of outer)."
5. **[D5 Cross-cutting/Semantic]** "GRAPH-FAVORING: find all handlers/services that *delete a contributor* even when the literal token differs (e.g. `DeleteContributorService` in Core/Services, the FastEndpoints `Delete` endpoint under `Web/Contributors`, and the `DeleteContributorCommand`/`DeleteContributorHandler` in UseCases), via semantic_query=[\"remove\",\"contributor\",\"command\"] rather than exact text — measure recall vs a grep-only baseline for the same intent."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Contributor|EfRepository|IDeleteContributorService|IAggregateRoot", label="Class|Interface"); D2->trace_call_path(name="EfRepository", direction="both"); D3->get_code_snippet(qualified_name="...ContributorAggregate.Contributor"); D4->get_architecture(); D5->search_code / search_graph semantic_query=["remove","contributor","command"].

**Deep-dive (LSP):**
- **Cross-repo:** Pair = OpenTelemetry demo `checkout` (Go) -> `cart` (.NET), via gRPC (the verified demo edge is `checkout -->|gRPC| cart`; the .NET `cart` service is the gRPC **callee/server**, not the caller). **X1 (recall):** enumerate every cross-service call caller->callee route that resolves into the .NET `cart` service — i.e. Go `checkout`'s generated client invoking `cart`'s `CartService` methods (`GetCart`/`AddItem`/`EmptyCart`) — using ground truth taken from the generated gRPC stubs (`*.cs` server stubs + Go client from `demo.proto`) cross-checked against the OTel demo service-topology docs; report recall = found / actual and precision = correct / found. NOTE: the .NET `accounting` service is an async **Kafka consumer** (`queue -->|TCP| accounting`), not a gRPC caller of checkout, so it is excluded from the gRPC X1 set and only appears (if at all) as an ASYNC_CALLS edge. **X2:** does `get_architecture.cross_repo_links` summarize the Go-checkout -> .NET-cart link (edge count and direction)? Graded P / partial / F (P = correct count + direction Go-checkout => .NET-cart; partial = link present but miscounted or undirected; F = absent).
- **Semantic/similarity:** Indexed in FULL mode on ardalis/CleanArchitecture. **S1 (vocabulary bridging):** semantic_query=["remove","contributor"] (and ["fetch","by","id"]) must surface `DeleteContributorService` / the `Delete` endpoint (and the `GetById` endpoint / `ContributorByIdSpec` [verify]) though the source token is "Delete"/"Spec"; metric = hit@5 against a synonym ground-truth set (remove≈delete, fetch≈get, list≈enumerate). **S2 (near-duplicate recall):** ground truth built reproducibly from the indexer's simhash output (>=20 Type-1/2 clone pairs, each token-diff confirmed) — expected clusters are the near-identical FastEndpoints CRUD endpoints repeated across `Web/Contributors/*` (Create/Update/Delete/GetById request-response DTO + validator triples) and the parallel specification classes; metric = recall (seeded pairs recovered) + false-positive rate (flagged pairs that fail token-diff confirmation)."

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/csharp-graph.md`, `csharp-explorer.md`, `csharp-judged.json`, `csharp-deepdive.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the csharp tier, and the Deep-Dive section.

---

### 10. php — A (Class-based OOP & Contracts) **[LSP]**

**Repo:** laravel/framework (`/tmp/bench/php`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** The canonical, most-starred PHP framework — heavily interface-driven (`Illuminate\Contracts\*`), trait-laden, and DI-container-centric, exercising class/interface/trait/method extraction at real scale, matching the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the `Illuminate\Container\Container` class and enumerate its public resolution API — specifically the `make`, `bind`, `singleton`, and `resolve` methods. Are the method signatures and declaring class reported correctly?"
2. **[D2 Relationship]** "For `Illuminate\Container\Container::make`, trace the call graph in both directions: what does `make` call internally (e.g. `resolve` / `build` [verify]) and which callers invoke `make` (e.g. the `App` facade / `Application` bootstrap path)?"
3. **[D3 Retrieval]** "Retrieve the full source of the single method `Illuminate\Database\Eloquent\Model::save` with exact line boundaries."
4. **[D4 Architecture]** "Describe the top-level package/namespace structure under `src/Illuminate` (Container, Database, Routing, Support, Contracts, ...) and how the `Contracts` namespace relates to the concrete implementations — i.e. the interface-vs-implementation layering."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find all classes that implement the `Illuminate\Contracts\Support\Arrayable` contract via its `toArray` method, and surface near-duplicate `toArray`/`jsonSerialize` implementations across `Collection`, `Model`, and resource classes. Semantic/IMPLEMENTS-edge query — not answerable by a single grep."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*Container.*", label="Class"/"Method"); D2->trace_call_path(qualified_name="Illuminate\\Container\\Container::make", direction="both"); D3->get_code_snippet(qualified_name="Illuminate\\Database\\Eloquent\\Model::save"); D4->get_architecture(); D5->search_code/semantic_query + search_graph(relationship="IMPLEMENTS").

**Deep-dive (LSP):**
- **Cross-repo:** Pair = OpenTelemetry **shipping(Rust)** -> **quote(PHP)** over HTTP (OTel demo microservices topology). NOTE on direction: per the OTel demo docs the **shipping service (Rust) is the caller** and the **quote service (PHP) is the callee** — shipping issues an HTTP request to quote to compute shipping cost, hitting quote's `getquote` route. PHP is therefore the cross-repo *callee*; the edge to evaluate is the **inbound** cross-language link into this PHP repo.
  - **X1 (recall):** Enumerate every cross-service call edge caller(shipping/Rust)->callee(quote/PHP) with its route; report recall = found/actual and precision = correct/found. Ground truth = the shipping service's outbound HTTP client call to quote's `/getquote` route [verify route path] cross-checked against the OTel demo service-topology docs. Expected: small actual set (1–2 edges); recall/precision computed against that hand-built set. (This is a legitimate graph-vs-grep test: a grep inside the PHP repo alone cannot recover the Rust-side caller; only the linked cross-repo graph surfaces the inbound edge.)
  - **X2:** Does `get_architecture.cross_repo_links` summarize the shipping->quote link (edge count + direction)? Graded **P** (count and direction both correct, i.e. caller=shipping/Rust, callee=quote/PHP) / **partial** (link present but count or direction wrong — e.g. direction reported reversed) / **F** (link absent).
- **Semantic/similarity:** Own repo laravel/framework, indexed in FULL mode.
  - **S1 (vocabulary bridging):** `semantic_query=["dependency injection resolve", "service container lookup", "instantiate bound abstract"]` should surface `Container::make` / `Container::resolve` / `Container::build` though the literal tokens differ. Metric = hit@5 against a synonym ground-truth set {`make`, `resolve`, `build`, `singleton`}.
  - **S2 (near-duplicate recall):** Ground truth built reproducibly from the indexer's simhash output (>=20 Type-1/2 pairs, token-diff confirmed) — e.g. the many parallel `Str`/`Arr` helper variants and the repetitive Eloquent relation `getResults`/`addConstraints` pairs. Metric = recall (simhash-flagged pairs the tool surfaces) + false-positive rate (flagged pairs that are not true Type-1/2 clones on manual token-diff).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/php-graph.md`, `php-explorer.md`, `php-judged.json`, `php-deepdive.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the php tier, and the Deep-Dive section.

---

### 11. lua — C (Dynamic & Scripting)

**Repo:** awesomeWM/awesome (`/tmp/bench/lua`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** AwesomeWM is a top-tier (~6k-star) tiling window manager whose `lib/` is a large, idiomatic Lua standard library (awful/wibox/gears/naughty/menubar) — substantial real-world Lua with deep module nesting and call chains, matching the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the `gears.timer` constructor and the `awful.spawn` function — both are grep-findable top-level API symbols. Does the tool surface their qualified names and source files?"
2. **[D2 Relationship]** "Starting from `awful.spawn` (in `lib/awful/spawn.lua`), trace the call graph in both directions: which awful modules call into spawn, and which lower-level functions (e.g. the C-exposed `awesome.spawn` global [verify — C-side binding, may not be a Lua graph node]) does it call?"
3. **[D3 Retrieval]** "Retrieve the full source of `gears.timer` (the timer object factory in `lib/gears/timer.lua`) as a single snippet, with correct start/end boundaries."
4. **[D4 Architecture]** "Describe the module/directory organization of the Lua library: how are the `awful`, `wibox`, `gears`, `naughty`, and `menubar` namespaces laid out under `lib/`, and how do submodules roll up into each namespace's `init.lua`?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find code semantically related to 'connecting and handling client/signal callbacks' across the library — e.g. `connect_signal` / `emit_signal` usage patterns — and surface duplication or shared signal-wiring conventions across `awful.client`, `awful.tag`, and `naughty`."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*spawn.*|.*timer.*"); D2->trace_call_path(name="spawn", direction="both"); D3->get_code_snippet(qualified_name="gears.timer"); D4->get_architecture(); D5->search_code/semantic_query("signal connect/emit handlers").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/lua-graph.md`, `lua-explorer.md`, `lua-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the lua tier.

---

### 12. scala — A (Class-based OOP & Contracts)

**Repo:** playframework/play-samples (`/tmp/bench/scala`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Play's official sample monorepo is the most-referenced, idiomatic Scala teaching corpus (high GitHub visibility, maintained by the framework authors), and its REST-API example is a substantial trait/class/object-rich slice that exercises Group A's class-based OOP + contract (trait) concerns — matching the plan's "popular, idiomatic, substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the `PostController` class and the `PostRepository` trait in `play-scala-rest-api-example` (package `v1.post`), and report each one's declared public methods (e.g. `index`, `process`, `show` on the controller; `create`, `list`, `get` on the trait)." (grep-findable: identifiers `PostController`, `PostRepository` appear verbatim in source.)
2. **[D2 Relationship]** "Show the inbound and outbound call relationships for `PostResourceHandler.create` — which controller action ultimately invokes it (note the call is made from `PostController.process` via the private helper `processJsonPost`, not directly in the action body), and which `PostRepository` method it calls in turn — so we can see the controller → handler → repository contract chain in both directions."
3. **[D3 Retrieval]** "Retrieve the full source of the `PostRepositoryImpl` class (the in-memory implementation of the `PostRepository` trait in `play-scala-rest-api-example`)." (grep-findable: identifier `PostRepositoryImpl` appears verbatim in source; D3 is a fair, symmetric retrieval target.)
4. **[D4 Architecture]** "Describe the package/module structure of the `play-scala-rest-api-example` app: how the `v1.post` package is organized (controller, router, handler, repository, action-builder) and how the `Module`/router wiring binds the trait `PostRepository` to its implementation."
5. **[D5 Cross-cutting/Semantic]** "Graph-favoring: across the Scala samples, find the components that play the same architectural role as `PostResourceHandler` — i.e. service/handler classes that mediate between an HTTP controller and a repository/DAO — even when they are named differently (e.g. *...Handler*, *...Service*, *...ResourceHandler*). Use semantic/similarity search rather than exact-name grep, then cross-link each to the controller and repository it sits between."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="PostController|PostRepository", label="Class|Trait"); D2->trace_call_path(qualified_name="v1.post.PostResourceHandler.create", direction="both"); D3->get_code_snippet(qualified_name="v1.post.PostRepositoryImpl"); D4->get_architecture(scope="play-scala-rest-api-example"); D5->search_code/semantic_query("service mediating controller and repository").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/scala-graph.md`, `scala-explorer.md`, `scala-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the scala tier.

---

### 13. kotlin — A (Class-based OOP & Contracts) **[LSP]**

**Repo:** JetBrains/Exposed (`/tmp/bench/kotlin`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** JetBrains' own Kotlin SQL framework/ORM is a top-tier, high-star, idiomatic-Kotlin codebase that is heavily class/interface/contract-driven (Table DSL, Entity DAO, IColumnType hierarchy), matching Group A's OOP-and-contracts focus and the plan's "popular + substantial + idiomatic" repo-selection rule.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the abstract base class `Table` and the `Column` class in the `org.jetbrains.exposed.sql` package, and list the public columnar factory methods declared on `Table` (e.g. `integer`, `varchar`, `text`, `bool`, `reference`). Both class names and these method names are plain grep-findable identifiers."
2. **[D2 Relationship]** "Show the bidirectional call graph (callers and callees) of the top-level `transaction(...)` function in `org.jetbrains.exposed.sql.transactions.ThreadLocalTransactionManager` [verify] — who invokes `transaction { }` and what it calls (e.g. `TransactionManager.currentOrNew`, `Transaction.commit`, `Transaction.rollback`)."
3. **[D3 Retrieval]** "Retrieve the exact source of the `SchemaUtils.create(vararg tables: Table)` method — one specific, grep-findable named symbol — with precise line boundaries."
4. **[D4 Architecture]** "Describe the module/package architecture of Exposed: how the core SQL DSL layer (`exposed-core`, `org.jetbrains.exposed.sql`) relates to the DAO/Entity layer (`exposed-dao`, `org.jetbrains.exposed.dao`) and the dialect/JDBC layers, and which directories hold each."
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING) Using semantic search, surface the code paths that perform 'persist a row to the database' even when the literal term is absent — i.e. the `insert`/`batchInsert`/`InsertStatement` machinery — and find structurally near-duplicate statement-builder classes (`InsertStatement` vs `UpdateStatement` vs `ReplaceStatement`)."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*Table.*|.*Column.*", label="Class"); D2->trace_call_path(qualified_name="...transaction", direction="both"); D3->get_code_snippet(qualified_name="org.jetbrains.exposed.sql.SchemaUtils.create"); D4->get_architecture(); D5->search_code(semantic_query="persist row to database") / search_graph(semantic_query=...).

**Deep-dive (LSP):**
- **Cross-repo:** Pair = OpenTelemetry demo `fraud-detection`(Kotlin, the caller) -> `checkout`(Go, the callee), linked via Kafka topic consumption / gRPC. **X1 (recall):** enumerate every cross-service call route caller->callee — fraud-detection consumes the `orders` Kafka topic produced by checkout (and any gRPC stubs it calls); report recall = found/actual and precision against ground truth derived from fraud-detection's Kafka consumer config + generated client stubs and the OTel demo service-topology docs. Expected actual set is small (1–2 routes: Kafka `orders` consume; possible gRPC), so a single miss swings recall hard — record exact numerator/denominator. **X2:** does `get_architecture.cross_repo_links` summarize the fraud-detection -> checkout link (edge count, direction)? Graded P/partial/F: P = correct count and direction, partial = link present but direction or count wrong, F = absent.
- **Semantic/similarity:** (own repo JetBrains/Exposed, FULL index) **S1 (vocabulary bridging):** `semantic_query=["delete rows","remove records","purge entries"]` should surface `DeleteStatement` / `Table.deleteWhere` / `deleteAll` though the literal token differs; metric hit@5 against a synonym ground-truth set {`DeleteStatement`, `deleteWhere`, `deleteAll`, `deleteIgnoreWhere` [verify]}. **S2 (near-duplicate recall):** ground truth built reproducibly from the indexer's simhash output (>=20 Type-1/2 pairs, each token-diff confirmed) — expected clusters include the `*Statement` builders (Insert/Update/Replace/Delete), the per-dialect classes (`MysqlDialect`/`PostgreSQLDialect`/`SQLiteDialect` [verify]), and the `*ColumnType` family; metric = recall over the simhash-seeded pair set + false-positive rate (reported pairs whose token-diff exceeds the Type-2 threshold).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/kotlin-graph.md`, `kotlin-explorer.md`, `kotlin-judged.json`, `kotlin-deepdive.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the kotlin tier, and the Deep-Dive section.

---

### 14. ruby — A (Class-based OOP & Contracts)

**Repo:** sinatra/sinatra (`/tmp/bench/ruby`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Top-tier (~12k-star) idiomatic Ruby web framework whose `Sinatra::Base` DSL, mixins, and class-method routing make it a substantial, class-based OOP exemplar — matching the plan's "popular + idiomatic + substantial" repo-selection criteria for Group A.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the `Sinatra::Base` class and its routing class methods `get` and `post` (public DSL) plus the underlying `route` class method (private) — return their declaring file and signatures."
2. **[D2 Relationship]** "Map the inbound and outbound call graph of `Sinatra::Base#dispatch!`: which methods invoke it on a request and which methods it calls (e.g. `process_route`, `route!`, `invoke`)."
3. **[D3 Retrieval]** "Retrieve the full body of `Sinatra::Base#process_route` exactly as written."
4. **[D4 Architecture]** "Describe how the `sinatra` gem is organized under `lib/sinatra/` — the core `base.rb` vs. the auto-running `main.rb`, the `Delegator`/`Helpers` mixins, and middleware files like `show_exceptions.rb`."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring, semantic) Find code semantically related to 'rendering a template / view layer' across the gem — e.g. `render`, the per-engine helpers (`erb`, `haml`, `markdown`), and `Templates` — even where the identifier names differ from the query terms."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Sinatra::Base|^get$|^route$", label="Class|Method"); D2->trace_call_path(qualified_name="Sinatra::Base#dispatch!", direction="both"); D3->get_code_snippet(qualified_name="Sinatra::Base#process_route"); D4->get_architecture(scope="lib/sinatra"); D5->search_code(query="render template view engine")/semantic_query.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/ruby-graph.md`, `ruby-explorer.md`, `ruby-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the ruby tier.

---

### 15. c — B (Systems & Low-level) **[LSP]**

**Repo:** redis/redis (`/tmp/bench/c`)   **Symlink:** no
**Indexed in:** full   **Why this repo:** Among the most-starred C systems projects on GitHub (~65k stars), redis is large, idiomatic single-process C (event loop + command table + handcrafted data structures), exercising the plan's "popular, substantial, idiomatic" repo-selection criteria for the C tier.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List every definition whose name matches the hash-table API surface in `dict.c` — `dictAdd`, `dictFind`, `dictCreate`, `dictExpand`, `dictDelete` — and report the file and signature of each. (grep-findable: all are literal `dict*` identifiers.)"
2. **[D2 Relationship]** "Show the full caller/callee context of `processCommand` (server.c): which functions reach it (e.g. command-processing path from the networking layer) and what it dispatches to (e.g. `call`, `lookupCommand`, `addReplyError`)? Use direction=both."
3. **[D3 Retrieval]** "Retrieve the exact source of the single function `createStringObject` (object.c) with precise line boundaries — its full body, not the surrounding file."
4. **[D4 Architecture]** "Summarize the top-level module/file organization of redis under `src/`: how do the event loop (`ae.c`), command dispatch (`server.c`), object system (`object.c`), the data-type implementations (`t_string.c`, `t_hash.c`, `t_list.c`, …), and the string library (`sds.c`) group together? Report directory/file clustering, not symbol bodies."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the functions that *reply to a client over the wire* even when the literal token 'reply' is absent — e.g. functions that append protocol output to the client buffer. Seed a semantic_query=[\"respond\",\"client\",\"output buffer\"] and expect to surface `addReply*` family and `_addReplyToBuffer` [verify] though the query word 'respond' never appears in their names."

**Expected graph tools (hint, not a script):** D1->search_graph(project="...redis", name_pattern="dict(Add|Find|Create|Expand|Delete)", label="Function"); D2->trace_call_path(name="processCommand", direction="both"); D3->get_code_snippet(qualified_name="...createStringObject"); D4->get_architecture(project="...redis"); D5->search_code / search_graph(semantic_query=["respond","client","output buffer"]).

**Deep-dive (LSP):**
- **Cross-repo:** pair = redis/redis <-> redis/hiredis. The link is the RESP wire protocol, **not** HTTP routes, so the linker's URL/route matching cannot form CROSS edges here — report as a **documented capability gap**, not a failure.
  - **X1 (recall):** Enumerate every cross-service call caller->callee route from hiredis's client stubs into a redis server command path (e.g. `redisCommand`/`redisAppendCommand` -> server-side `processCommand`/`*Command` handler). Ground truth = hiredis client stub set (`redisConnect`, `redisCommand`, `redisGetReply`, `redisReader` [verify]) cross-referenced with redis command-table handlers, plus any OTel service-topology doc for the pair. Because RESP is not HTTP, **expected found = 0 of N actual**; report recall = 0/N and precision = undefined/(n=0) and label the gap. Metric here measures honest gap-reporting, not edge formation.
  - **X2:** Does `get_architecture.cross_repo_links` summarize a redis<->hiredis caller->callee link (count, direction)? **Expected: F** (no RESP-aware cross edge) — graded P/partial/F; a "partial" is awarded only if the field at least lists both repos as indexed without inventing a spurious link.
- **Semantic/similarity:** (redis/redis indexed in FULL mode)
  - **S1 (vocabulary bridging):** semantic_query=["fetch","key","read"] should surface `lookupKeyRead` / `lookupKeyReadWithFlags` [verify] and `getGenericCommand` [verify] though the literal token "fetch" is absent. Metric = hit@5 against a synonym ground-truth set {lookupKeyRead, lookupKeyReadWithFlags, lookupKeyWrite, getCommand, getGenericCommand}.
  - **S2 (near-duplicate recall):** Ground truth built reproducibly from the indexer's simhash output: take the top simhash-clustered pairs (>=20 Type-1/Type-2 clones) — strong candidates are the parallel `t_*` type handlers and the `addReply*` reply variants whose bodies differ only by token (e.g. `addReplyBulk` vs `addReplyBulkCBuffer` [verify], `lpushCommand` vs `rpushCommand` family) — and confirm each pair by token-diff before admitting it. Metric = recall (simhash-found / token-confirmed actual) + false-positive rate (admitted pairs that token-diff rejects).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/c-graph.md`, `c-explorer.md`, `c-judged.json`, `c-deepdive.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the c tier, and the Deep-Dive section.

---

### 16. bash — C (Dynamic & Scripting)

**Repo:** bash-it/bash-it (`/tmp/bench/bash`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** A widely-starred (>14k), idiomatic Bash framework whose functions, aliases, completions and themes span hundreds of sourced `.bash` files — substantial real-world shell with deep function-to-function sourcing, satisfying the plan's "popular + idiomatic + substantial" repo-selection criteria for Group C.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the top-level dispatcher function `bash-it` and the framework loader entrypoint `bash_it.sh`; list the public `_bash-it-*` helper functions (`_bash-it-aliases`, `_bash-it-completions`, `_bash-it-plugins`) that constitute the framework's enable/disable API." (grep-findable: these names appear literally as `function bash-it()` / `function _bash-it-aliases()` in `lib/helpers.bash`.)
2. **[D2 Relationship]** "Starting from the `bash-it` dispatcher, trace the call graph (both directions) through `_bash-it-main-help-*` [verify] / `_enable-thing` / `_disable-thing` to show how an `enable plugin <name>` invocation reaches the symlink-creation logic in `_enable-thing` that links a file from `plugins/available` into `plugins/enabled`." (structural framing allowed for D2.)
3. **[D3 Retrieval]** "Retrieve the full source of the single function `_bash-it-find-in-ancestor` (the ancestor-directory search helper in `lib/helpers.bash`)." (one real, grep-findable named symbol.)
4. **[D4 Architecture]** "Describe the directory/file organization of bash-it: the role of `bash_it.sh`, `lib/`, `plugins/available` vs `plugins/enabled`, `aliases/`, `completion/`, and `themes/`, and how the enabled-vs-available symlink convention structures the framework."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Surface naming-pattern and config↔code links: find clusters of similarly-named enable/disable helpers (`_enable-thing` / `_disable-thing` / `_enable-plugin` / `_disable-plugin` …) and the `_bash-it-reload` / `_bash-it-restart` reload functions, and identify duplicated theme-prompt scaffolding shared across `themes/*/` — a similarity/duplication query a plain grep cannot rank." (explicitly graph-favoring; included in Group C D5 only.)

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="^(bash-it|_bash-it-.*)$", label=Function); D2->trace_call_path(name="bash-it", direction="both"); D3->get_code_snippet(qualified_name="_bash-it-find-in-ancestor"); D4->get_architecture(); D5->search_code/semantic_query("theme prompt scaffolding duplication; enable/disable helper family").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/bash-graph.md`, `bash-explorer.md`, `bash-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the bash tier.

---

### 17. zig — B (Systems & Low-level)

**Repo:** tigerbeetle/tigerbeetle (`/tmp/bench/zig`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** TigerBeetle is the most-starred, most-idiomatic large Zig systems codebase (financial OLTP database) — heavy comptime generics, LSM storage, VSR consensus — making it a substantial, real-world stress test that matches the plan's "popular + idiomatic + non-trivial size" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the public API surface of the state machine: find the `StateMachineType` generic and its public methods `init`, `deinit`, `reset`, `open`, and `input_valid` in `src/state_machine.zig`. Do all five resolve with correct file/line, and is the comptime-generic `StateMachineType` recognized as a definition rather than missed?" (All targets are grep-findable plain identifiers.)
2. **[D2 Relationship]** "For `parse_addresses` (in `src/vsr.zig`), show the full call relationship in both directions: who calls it (e.g. config/CLI parsing paths) and what it calls. Does the graph reconstruct the inbound/outbound CALLS edges correctly?"
3. **[D3 Retrieval]** "Retrieve the exact source of the `exponential_backoff_with_jitter` function declared in `src/vsr.zig` — return only that function body with precise boundaries, no surrounding noise." (Single, well-known, grep-findable symbol.)
4. **[D4 Architecture]** "Describe the top-level module/folder architecture of `src/`: the major subsystems `vsr/`, `lsm/`, `io/`, `state_machine/`, `clients/`, `tigerbeetle/`, `stdx/`, and key entry file `src/tigerbeetle/main.zig`. Does the structural view capture the subsystem grouping and the main binary entry point?"
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Find the cross-cutting checksum/integrity machinery: surface declarations semantically related to data integrity — e.g. `checksum` and `ChecksumStream` in `src/vsr.zig`, the `ewah.zig` run-length codec, and superblock/manifest verification — that a plain grep for one keyword would not cohere into one set. Does semantic/similarity retrieval cluster the integrity-related symbols across files?"

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="StateMachineType|init|deinit|reset|open|input_valid", path~"src/state_machine.zig"); D2->trace_call_path(name="parse_addresses", direction="both"); D3->get_code_snippet(qualified_name="...exponential_backoff_with_jitter"); D4->get_architecture(scope="src"); D5->search_code/semantic_query("checksum integrity verification superblock manifest").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/zig-graph.md`, `zig-explorer.md`, `zig-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the zig tier.

---

### 18. elixir — D (Functional & Formal)

**Repo:** phoenixframework/phoenix (`/tmp/bench/elixir`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Phoenix is the de-facto standard Elixir web framework (~21k+ GitHub stars), a large, idiomatic, macro-heavy codebase that exercises the plan's "popular + substantial + idiomatic" repo-selection criteria for the functional/formal group.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definitions of the public macros `plug` and `pipe_through/1` exposed by `Phoenix.Router`, plus the `Phoenix.Controller` module — where are they defined and what is their arity/signature?" (all are well-known, grep-findable identifiers: `defmacro plug(plug, opts \\ [])` (arity 2, callable as `plug/1`) and `defmacro pipe_through(pipes)` in `lib/phoenix/router.ex`, and `defmodule Phoenix.Controller` in `lib/phoenix/controller.ex`)
2. **[D2 Relationship]** "For `Phoenix.Controller.render/3`, show both directions: which functions does it call internally, and which call sites/macros invoke it across the controller and view-rendering pipeline?"
3. **[D3 Retrieval]** "Retrieve the full source of `Phoenix.Endpoint.__using__/1`, the macro that injects endpoint behaviour when a module does `use Phoenix.Endpoint`." (grep-findable: `defmacro __using__(opts)` in `lib/phoenix/endpoint.ex`)
4. **[D4 Architecture]** "Describe the top-level structure of the `lib/phoenix/` tree — the major subsystems (router, controller, channel, socket, endpoint, test) and how the directories/modules are organized." (Note: Phoenix.PubSub lives in the separate `phoenix_pubsub` package, NOT in this repo's `lib/phoenix/` — do not expect a pubsub subsystem here.)
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring / semantic) Across the codebase, find the functions/macros responsible for compiling and matching routes (e.g. route building, path matching, dispatch) even when their names don't share an obvious substring — i.e. semantically related routing-dispatch logic spread across `Phoenix.Router` and helper modules."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="plug|pipe_through|Phoenix\\.Controller"); D2->trace_call_path(qualified_name="Phoenix.Controller.render", direction="both"); D3->get_code_snippet(qualified_name="Phoenix.Endpoint.__using__"); D4->get_architecture(path="lib/phoenix"); D5->search_code/semantic_query("route compilation and path matching dispatch").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/elixir-graph.md`, `elixir-explorer.md`, `elixir-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the elixir tier.

---

### 19. haskell — D (Functional & Formal)

**Repo:** jgm/pandoc (`/tmp/bench/haskell`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ~38k-star universal document converter — the canonical large, idiomatic Haskell codebase (readers/writers, typeclasses, monad transformers), exercising the Haskell extractor on real-world module/type/function structure rather than toy code.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Where is the top-level conversion entry point `convertWithOpts` defined, and where is the core `writeMarkdown` writer function declared? Both are exported, grep-findable names — does search_graph surface their defining modules (Text.Pandoc.App and Text.Pandoc.Writers.Markdown)?"
2. **[D2 Relationship]** "Trace the call relationships around `readMarkdown` [verify]: what does the Markdown reader call into (e.g. parser combinators / `Text.Pandoc.Parsing` helpers), and what callers invoke it? Use direction=both so both the dispatch in the reader registry and downstream callees appear."
3. **[D3 Retrieval]** "Retrieve the exact source of the `writeHtml5` [verify] function from Text.Pandoc.Writers.HTML — return just that function body, not the whole module."
4. **[D4 Architecture]** "Describe pandoc's module organization: how are `Text.Pandoc.Readers.*` and `Text.Pandoc.Writers.*` grouped relative to the shared core (`Text.Pandoc.Definition`, `Text.Pandoc.Options`, `Text.Pandoc.Parsing`)? Does the structure view recover the reader/writer/core layering?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across all writer modules, find the functions that serialize a `Block` / `Inline` AST node to output — i.e. the family of `blockToX` / `inlineToX` helpers (`blockToMarkdown`, `inlineToHtml`, …) that share a naming pattern and structural role across Markdown/HTML/LaTeX writers. This duplication-by-convention pattern is what semantic/pattern search should cluster and plain grep on one name cannot."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="convertWithOpts|writeMarkdown"); D2->trace_call_path(name="readMarkdown", direction="both"); D3->get_code_snippet(qualified_name="Text.Pandoc.Writers.HTML.writeHtml5"); D4->get_architecture(scope="Text.Pandoc"); D5->search_code/semantic_query(query="serialize Block or Inline AST node to output", pattern=".*(blockTo|inlineTo).*").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/haskell-graph.md`, `haskell-explorer.md`, `haskell-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the haskell tier.

---

### 20. ocaml — D (Functional & Formal)

**Repo:** ocaml/dune (`/tmp/bench/ocaml`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** dune is the de-facto OCaml build system (~4k+ stars, used by virtually every modern OCaml project); it is large, idiomatic, multi-package OCaml (functors, modules, .ml/.mli pairs), matching the plan's criterion of a substantial, popular, idiomatic in-language codebase.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the `Dune_project` module and its constructor/loader function `Dune_project.load` [verify]; list its public API as declared in the corresponding `.mli` interface." (grep-findable: the identifiers `Dune_project` and `load` appear verbatim in `src/dune_rules/dune_project.ml`/`.mli`.)
2. **[D2 Relationship]** "Starting from `Build_system.build_file` [verify] (or the nearest real entry in `src/dune_engine/build_system.ml`), trace both inbound callers and outbound callees to show how a requested target flows from the engine into rule evaluation."
3. **[D3 Retrieval]** "Retrieve the full source of the `Action_builder.t` type and the `Action_builder.bind`/`map` combinators [verify] in `src/dune_engine/action_builder.ml` — return only that symbol, not the whole file." (grep-findable identifier: `Action_builder`.)
4. **[D4 Architecture]** "Produce the high-level architecture of dune: the top-level package/library split (`src/dune_engine`, `src/dune_rules`, `src/dune_lang`, `bin/`, `vendor/`) and how the CLI entry (`bin/main.ml`) depends on the engine and rules layers."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find code semantically related to 'memoized incremental build computation' — e.g. the `Memo` module and its `Memo.create`/`Memo.exec` usage [verify] — and surface other call sites that participate in the same memoization/incrementality concern across `src/dune_engine`."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*Dune_project.*", label="Module/Function"); D2->trace_call_path(qualified_name="Build_system.build_file", direction="both"); D3->get_code_snippet(qualified_name="Action_builder.t"); D4->get_architecture(project="ocaml-dune"); D5->search_code("memoized incremental build") / search_graph(semantic_query="memoization incremental computation").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/ocaml-graph.md`, `ocaml-explorer.md`, `ocaml-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the ocaml tier.

---

### 21. objc — A (Class-based OOP & Contracts)

**Repo:** AFNetworking/AFNetworking (`/tmp/bench/objc`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred Objective-C projects on GitHub (~33k stars) and the canonical idiomatic iOS/macOS networking library — substantial class hierarchies, formal `@protocol` contracts, and category usage make it a faithful exemplar of Group A's class-based OOP + contracts theme.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the `AFHTTPSessionManager` class and list its public initializers/factory methods (e.g. `manager`, `initWithBaseURL:`, `initWithBaseURL:sessionConfiguration:`). Identifier is plain-text grep-findable as `@interface AFHTTPSessionManager`."
2. **[D2 Relationship]** "Show the inbound and outbound relationships of `AFHTTPSessionManager` — confirm it inherits from `AFURLSessionManager`, and trace what its `GET:parameters:headers:progress:success:failure:` / `dataTaskWithHTTPMethod:...` methods call internally (e.g. into the request serializer and the superclass task-creation path). [verify] exact convenience-method signature varies by AFNetworking version (3.x adds `progress:`)."
3. **[D3 Retrieval]** "Retrieve the full source of the `AFSecurityPolicy` instance method `-evaluateServerTrust:forDomain:` (grep-findable token `evaluateServerTrust:forDomain:`)."
4. **[D4 Architecture]** "Describe the module/file organization of the `AFNetworking/` source directory and how the serialization layer (`AFURLRequestSerialization`/`AFURLResponseSerialization`) sits relative to the session-management layer (`AFURLSessionManager`/`AFHTTPSessionManager`) and the security/reachability helpers."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Find all classes that conform to the `AFURLRequestSerialization` protocol via its required method `requestBySerializingRequest:withParameters:error:` (e.g. `AFHTTPRequestSerializer`, `AFJSONRequestSerializer`, `AFPropertyListRequestSerializer`) and surface other request/response serializer types that follow the same contract/naming pattern. Labelled graph-favoring: this is a protocol-conformance + naming-pattern query that plain grep can only approximate."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="AFHTTPSessionManager", label="Class"); D2->trace_call_path(qualified_name="AFHTTPSessionManager", direction="both"); D3->get_code_snippet(qualified_name="AFSecurityPolicy.evaluateServerTrust:forDomain:"); D4->get_architecture(scope="AFNetworking/"); D5->search_code/semantic_query("classes conforming to AFURLRequestSerialization / requestBySerializingRequest:withParameters:error:").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/objc-graph.md`, `objc-explorer.md`, `objc-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the objc tier.

---

### 22. swift — A (Class-based OOP & Contracts)

**Repo:** Alamofire/Alamofire (`/tmp/bench/swift`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ~41k-star, the de-facto idiomatic Swift HTTP networking library — substantial protocol-oriented + class-based design (Session/Request hierarchy, interceptor/serializer protocols), matching the plan's "popular, idiomatic, substantial" repo-selection criteria for Group A.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the public type `RequestInterceptor` and the two protocols it composes — `RequestAdapter` and `RequestRetrier`. Report each one's kind (protocol/typealias) and declaring file."
2. **[D2 Relationship]** "For `DataRequest`, show the call relationship both ways: which methods/initializers construct or return a `DataRequest` (inbound, e.g. via `Session.request`) and which serializer/response APIs it invokes (outbound, e.g. `response(...)` / `responseDecodable(...)`)."
3. **[D3 Retrieval]** "Retrieve the full source of the `AFError` enum definition (its cases and nested error types)."
4. **[D4 Architecture]** "Describe the module's structure: how the `Source/` tree is organized (Core, Features, Extensions, etc.) and where the `Session`, `Request` family, and `*Serializer` types live relative to one another."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the response-serialization concepts across the codebase — symbols semantically related to turning raw responses into typed values (e.g. `ResponseSerializer`, `DataResponseSerializerProtocol`, `DecodableResponseSerializer`, `responseDecodable`) — and group the conforming serializer implementations even when names differ. Label: semantic/similarity query, expected to favor the graph over plain grep."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="RequestInterceptor|RequestAdapter|RequestRetrier", label="Protocol"); D2->trace_call_path(qualified_name="...DataRequest", direction="both"); D3->get_code_snippet(qualified_name="Alamofire.AFError"); D4->get_architecture(project="swift"); D5->search_code/semantic_query("response serialization to typed value").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/swift-graph.md`, `swift-explorer.md`, `swift-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the swift tier.

---

### 23. dart — A (Class-based OOP & Contracts)

**Repo:** felangel/bloc (`/tmp/bench/dart`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ~12k-star, canonical Dart state-management monorepo whose idiomatic class/mixin/abstract-interface hierarchy (Bloc, Cubit, BlocBase, observers) makes it a substantial, popular Group-A exemplar per the plan's popularity-plus-idiomaticity selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the abstract class `Bloc` and its companion base `BlocBase` in `packages/bloc/lib/src/`, and list their public API surface (e.g. `add`, `on`, `emit`, `close`, `state`, `stream`, `isClosed`)."
2. **[D2 Relationship]** "Map the relationships of `Cubit`: what does it extend (`BlocBase`), which members it inherits vs. defines (it inherits `emit`/`onChange` from `BlocBase` and adds no overrides beyond its constructor — note that it is `Bloc`, not `Cubit`, that overrides `emit`), and who calls `BlocBase.emit` / `BlocBase.onChange` — show callers and callees in both directions."
3. **[D3 Retrieval]** "Retrieve the full source of the `BlocObserver` class (the lifecycle hooks `onCreate`, `onEvent`, `onChange`, `onTransition`, `onError`, `onClose`)."
4. **[D4 Architecture]** "Describe the architecture of the `bloc` monorepo: the `packages/` layout (bloc, flutter_bloc, hydrated_bloc, replay_bloc, bloc_test, bloc_concurrency) and, within `packages/bloc/lib/src/`, the class/mixin/abstract-interface hierarchy rooted at `BlocBase` (`Streamable`, `StateStreamable`, `Closable`, `Emittable`, `ErrorSink`)."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Locate the event-transformer concept across the codebase: the `EventTransformer`/`EventMapper` typedefs in `packages/bloc` and their concurrency implementations (`concurrent`, `sequential`, `droppable`, `restartable`) in `packages/bloc_concurrency` — surface the semantic cluster and the typedef<->implementation links, not just exact-name text matches."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Bloc|BlocBase", label="Class"); D2->trace_call_path(qualified_name="...BlocBase.emit", direction="both"); D3->get_code_snippet(qualified_name="...BlocObserver"); D4->get_architecture(scope="packages/bloc"); D5->search_code/semantic_query("event transformer concurrency droppable restartable").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/dart-graph.md`, `dart-explorer.md`, `dart-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the dart tier.

---

### 24. perl — C (Dynamic & Scripting)

**Repo:** mojolicious/mojo (`/tmp/bench/perl`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Mojolicious is the most-starred idiomatic Perl web framework (~2.7k stars), large and heavily OO via `Mojo::Base`/`has` accessors — a substantial, real-world Perl codebase that matches the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the `Mojo::URL` package and its accessor/method `path` — where is the `package Mojo::URL;` declared and where is `sub path` (or its `has` accessor) defined?"
2. **[D2 Relationship]** "Map the relationships around `Mojolicious::Routes::Route::to`: what calls it (inbound) and what does it call (outbound), tracing the route-dispatch chain in both directions?"
3. **[D3 Retrieval]** "Retrieve the full source of the `dispatch` method in `Mojolicious::Routes` (i.e. `sub dispatch`) exactly as written, with its precise line boundaries."
4. **[D4 Architecture]** "Show the architecture of the distribution: how is `lib/Mojo/` (core toolkit) organized versus `lib/Mojolicious/` (the framework), and which directories hold the server, transaction, and DOM subsystems?"
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Semantically locate all code that builds or parses HTTP transactions/requests (e.g. `Mojo::Transaction`, `Mojo::Message::Request`, `Mojo::UserAgent` build paths) — surface conceptually-related request/response construction logic even when identifiers differ. (Graph-favoring: relies on semantic similarity, not a single grep token.)"

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Mojo::URL|path"); D2->trace_call_path(qualified_name="Mojolicious::Routes::Route::to", direction="both"); D3->get_code_snippet(qualified_name="Mojolicious::Routes::dispatch"); D4->get_architecture(scope="lib/"); D5->search_code/semantic_query("build or parse HTTP request/response transaction").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/perl-graph.md`, `perl-explorer.md`, `perl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the perl tier.

---

### 25. groovy — A (Class-based OOP & Contracts)

**Repo:** spockframework/spock (`/tmp/bench/groovy`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Spock is the canonical, widely-adopted (~3.6k stars) Groovy BDD testing framework — idiomatic, substantial Groovy with a deep class hierarchy, traits, and AST transforms, satisfying the plan's "popular + idiomatic + non-trivial size" repo-selection criteria for Group A (Class-based OOP & Contracts).

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the public API surface a user extends to write specifications: locate the `Specification` base class (in `spock.lang`) and the `MockingApi` class it derives from in `spock-core`, and report the factory method declarations `Mock`, `Stub`, and `Spy` exposed for test authors." (All grep-findable identifiers: `class Specification`, `class MockingApi`, `public <T> T Mock(`.)
2. **[D2 Relationship]** "Map the relationships around `Specification`: what is its inheritance chain (`Specification` extends `MockingApi`, which in turn derives from the internal `SpecInternals` base) and which callers/subclasses reference the `setupSpec`/`cleanupSpec` lifecycle methods? Note that `SpecificationContext` is a runtime collaborator referenced by the spec, not a superclass. Use direction=both to capture both ancestors and the fixture-method call sites."
3. **[D3 Retrieval]** "Retrieve the full source of the `IMockController` interface in `org.spockframework.mock` so a reviewer can read the exact mock-interaction contract it declares (`addInteraction`, `enterScope`, `leaveScope`, etc.)." (Grep-findable as `interface IMockController`.)
4. **[D4 Architecture]** "Describe the module/package architecture: how is the repo split across `spock-core`, `spock-spring`, `spock-junit4`, and `spock-bom`, and within `spock-core` how do the `org.spockframework.runtime`, `org.spockframework.mock`, and `spock.lang` packages divide responsibility (runtime engine vs mocking vs user-facing DSL)?"
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Semantically locate the AST-transformation machinery that rewrites spec source at compile time — find the `SpockTransform` global transform entry point and the spread of rewriting classes (`SpecRewriter`, `WhereBlockRewriter`, `DeepBlockRewriter`, `InteractionRewriter`, `SpecAnnotator`) in `org.spockframework.compiler` — using semantic_query/search_code, since this machinery is spread across the compiler package by behavior, not by a single shared name token."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*Specification.*|.*MockingApi.*", label="Class"); D2->trace_call_path(qualified_name="spock.lang.Specification", direction="both"); D3->get_code_snippet(qualified_name="org.spockframework.mock.IMockController"); D4->get_architecture(project="groovy"); D5->search_code/semantic_query("compile-time AST transformation rewriting specification blocks").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/groovy-graph.md`, `groovy-explorer.md`, `groovy-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the groovy tier.

---

### 26. erlang — D (Functional & Formal)

**Repo:** ninenines/cowboy (`/tmp/bench/erlang`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Cowboy is the de-facto small, fast, modern HTTP server for Erlang/OTP (~7k GitHub stars, used across the BEAM ecosystem); its idiomatic OTP module/behaviour layout makes it a substantial, representative Erlang target per the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the public entry-point functions that start an HTTP/HTTPS listener — locate `cowboy:start_clear/3` and `cowboy:start_tls/3` in `src/cowboy.erl`. Are both arity-3 functions surfaced as definitions with their module qualifier?"
2. **[D2 Relationship]** "Starting from `cowboy_router:execute/2`, show the relationship neighborhood in both directions: what it calls (e.g. `cowboy_router:match/3` [verify]) and which callers reach it (the stream/handler pipeline). Does the call graph connect the router to `cowboy_handler:execute/2` [verify]?"
3. **[D3 Retrieval]** "Retrieve the full source of the single function `cowboy_router:compile/1` from `src/cowboy_router.erl` — the routes-compilation function — with exact clause boundaries."
4. **[D4 Architecture]** "Describe the top-level architecture: the `src/` module layout and how the protocol modules (`cowboy_http`, `cowboy_http2`, `cowboy_websocket`) relate to the transport/stream layer (`cowboy_clear`, `cowboy_tls`, `cowboy_stream`). Is the module-per-concern OTP structure visible?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the functions implementing HTTP response writing across protocol versions — semantically locate the `reply`/`stream_reply` paths in `cowboy_req` and how each protocol module fulfils the `cowboy_stream` behaviour callbacks. This favors semantic/behaviour-callback linking over a single literal grep term."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="cowboy:start_(clear|tls)"); D2->trace_path(function_name="cowboy_router:execute/2", direction="both"); D3->get_code_snippet(qualified_name="cowboy_router:compile/1"); D4->get_architecture (inspect src/ module clusters); D5->search_graph(semantic_query=["reply","stream_reply","cowboy_stream","behaviour"]) or search_code (HTTP reply / stream_reply across protocol versions; cowboy_stream behaviour callbacks).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/erlang-graph.md`, `erlang-explorer.md`, `erlang-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the erlang tier.

---

### 27. r — C (Dynamic & Scripting)

**Repo:** tidyverse/dplyr (`/tmp/bench/r`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** dplyr is the canonical, high-star tidyverse data-manipulation package and the most idiomatic large-scale R codebase (verb-based S3/generic dispatch under `R/`), matching the plan's "popular + substantial + idiomatic" repo-selection criteria for the r tier.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the definitions of the core dplyr verbs `mutate`, `filter`, `summarise`, `arrange`, and `select` in `R/` — are they captured as top-level function definitions (generics), and where is each defined?"
2. **[D2 Relationship]** "Show the call relationships for `summarise` (direction=both): which internal helpers it calls (e.g. `summarise_cols` [verify], `dplyr_col_modify`) and which exported functions reach it, distinguishing the generic from its `summarise.data.frame` method."
3. **[D3 Retrieval]** "Retrieve the full source of the `mutate.data.frame` method (the data-frame S3 method for `mutate`) exactly as defined in `R/mutate.R`."
4. **[D4 Architecture]** "Describe the structure of the `R/` source tree: how are the verb files (`mutate.R`, `filter.R`, `summarise.R`, `join.R`, `group-by.R`) organized, and how does the R layer relate to the `src/` C++ backend it wraps?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the family of functions implementing the tidy-select / column-selection semantics (e.g. `select`, `relocate`, `rename`, `pull` and shared `tidyselect`-style helpers) by semantic similarity rather than literal name match, and surface near-duplicate verb scaffolding across the `*.R` verb files."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="mutate|filter|summarise|arrange|select", label="Function"); D2->trace_call_path(qualified_name="summarise", direction="both"); D3->get_code_snippet(qualified_name="mutate.data.frame"); D4->get_architecture(path="R/"); D5->search_code(semantic_query="column selection / tidy-select verb helpers") / search_graph(semantic_query=...).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/r-graph.md`, `r-explorer.md`, `r-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the r tier.

---

### 28. html — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** expressjs/express (symlink javascript) (`/tmp/bench/html`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** Express is the most-depended-on Node web framework (60k+ stars, tens of millions of weekly installs). Its `examples/ejs/` tree ships real `.html` template files (`header.html`, `footer.html`, `users.html`) that are wired through `app.set('view engine', 'html')` + `app.engine('.html', ejs.__express)`, giving a genuine, idiomatic HTML/markup corpus — matching the plan's "popular + idiomatic + substantial" repo-selection criteria. (Note: express is HTML-light overall; the markup lives almost entirely under `examples/*/views/`, which the questions below stay scoped to.)

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level HTML document scaffolds declared in this repo — i.e. the files that open a full document with `<!DOCTYPE html>` / `<html lang="en">` / `<head><title>` such as `examples/ejs/views/header.html`, plus the inline HTML the framework emits itself for `res.redirect` in `lib/response.js` (the `<!DOCTYPE html>…<body><p>…</p></body>` redirect body). Enumerate each with its file. (Grep-findable: these are literal `<html`/`<!DOCTYPE` tags in the source.)"
2. **[D2 Relationship]** "Trace the include/render chain for the EJS-as-HTML example views: which partials does `examples/ejs/views/users.html` pull in via the EJS `include(...)` directive (it includes `header.html` and `footer.html`), and which route handler renders it — i.e. the `res.render('users', …)` call in `examples/ejs/index.js`?"
3. **[D3 Retrieval]** "Retrieve the single largest complete HTML document definition in the repo — the full markup of `examples/ejs/views/header.html`, the only template that declares the whole `<!DOCTYPE html><html><head><title><%= title %></title>…<body>` scaffold (the rest, e.g. `users.html` / `footer.html`, are fragments). Return it verbatim with its boundaries. (Grep-findable: locate it by grepping for `<!DOCTYPE html>`.)"
4. **[D4 Architecture]** "Describe how HTML/template assets are organized across the repo: the per-example `views/` directory convention under `examples/` (e.g. `examples/ejs/views/`, `examples/auth/views/`), the separation of templates from route code in each example's `index.js`, and where framework-emitted markup lives (the inline redirect-body HTML in `lib/response.js`) versus user-facing example markup under `examples/*/views/`."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find duplicated / near-duplicate markup and the config<->markup linkage: which example view files share the same boilerplate document scaffold or fragment pattern across `examples/*/views/` (e.g. the `header`/`footer` split repeated in the `ejs` and `auth` examples [verify]), and which template files are bound to a configured view engine via the `app.set('view engine', …)` / `app.engine('.html', …)` calls in the corresponding example `index.js`? Label: graph-favoring — duplication clustering plus config-call-to-template-file linkage are not reachable by a single grep."

**Expected graph tools (hint, not a script):** D1->search_code/grep for `<!DOCTYPE html>`/`<html` literals across `examples/*/views/` + `lib/response.js`; D2->trace_call_path(direction=both, on the `res.render` handler in `examples/ejs/index.js`) plus the EJS `include` edges between view files; D3->get_code_snippet(qualified_name="examples/ejs/views/header.html"); D4->get_architecture(scope="examples/", views/ layout + lib/ framework markup); D5->search_code/semantic_query("shared <html><head> document scaffold") + config-view-engine-to-template linkage.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/html-graph.md`, `html-explorer.md`, `html-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the html tier.

---

### 29. css — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** shadcn-ui/ui (symlink tsx) (`/tmp/bench/css`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** One of the most-starred React component ecosystems on GitHub; its design-token CSS (`globals.css` custom-property themes + `@layer` / `@tailwind` directives) is idiomatic, substantial, real-world CSS, satisfying the plan's "popular + idiomatic + non-trivial size" repo-selection criteria.

> **Indexer note (fairness):** the CBM indexer's CSS spec extracts **no definition nodes** — no selectors, no custom-property declarations — only `@import` edges and the stylesheet module. There is **no `var()` define→use edge** and **no qualified name** for a selector block. Questions below are authored honestly around this: the structural dimensions are expected to be **grep-favoring or N/A**, not graph wins.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List every top-level selector block in `apps/www/styles/globals.css` [verify] — specifically the `:root` and `.dark` blocks that declare design tokens such as `--background`, `--foreground`, `--primary`, `--radius`. (Symmetric: these selectors are equally findable by `grep -nE ':root|\.dark'`. CSS yields no graph Definition nodes, so the graph is expected to do no better than — and likely worse than — plain grep here.)"
2. **[D2 Relationship]** "**N/A.** CSS has no call/reference graph in the indexer: `var(--border)` / `var(--ring)` usages are not modeled as define→use edges, and selector blocks are not nodes. There is no faithful relationship/trace question to ask; forcing one would test a capability the language extraction does not have. A `var()` cross-reference question would be a grep/text task, not a graph-relationship task."
3. **[D3 Retrieval]** "Retrieve the full body of the `:root` token declaration block in `apps/www/styles/globals.css` [verify], exactly as written, including every `--*` custom-property line. (Symmetric: the same block is retrievable by `grep -nA40 ':root' globals.css` or a plain file read. Note CSS exposes no qualified-name snippet target, so `get_code_snippet` has no symbol to resolve — both engines fall back to file/line retrieval.)"
4. **[D4 Architecture]** "Describe the stylesheet file/directory organization: where the global theme CSS lives (`apps/www/styles/` [verify]) versus per-registry / per-component theme CSS, and how the `@tailwind base/components/utilities` + `@layer base` structure partitions the single global stylesheet."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Surface the duplication and naming convention across the theme: the `:root` (light) and `.dark` blocks define a near-mirrored set of `--*` properties — identify the parallel/duplicated token names, and any config↔code link between these CSS custom properties and the Tailwind `theme.extend.colors` mapping that references `hsl(var(--...))`. Openly graph/semantic-favoring (mirrored-block similarity + token↔config linkage via semantic retrieval, not exact-string grep)."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=":root|\\.dark") and search_code (expected: little/no graph structure for CSS; grep parity is the honest baseline); D2->N/A (no relationship edges for CSS); D3->search_code / file retrieval for the `:root` block (no qualified-name snippet target); D4->get_architecture(scope="styles/"); D5->search_code(semantic_query="mirrored light/dark token blocks; hsl(var(--..)) Tailwind mapping").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/css-graph.md`, `css-explorer.md`, `css-judged.json`.
**Aggregates into:** D1/D3/D4 cross-group rollups (D2 excluded as N/A), D5 within Group E only, Group E, the css tier.

---

### 30. scss — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** twbs/bootstrap (`/tmp/bench/scss`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ~170k-star, the canonical large SCSS codebase — deep mixin/function/variable layering and a fully `@import`/`@use`-driven module graph make it the most idiomatic, substantial SCSS target for the plan's "popular + representative" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List representative SCSS mixins and functions defined in Bootstrap's `scss/` tree — e.g. the `button-variant` mixin (in `scss/mixins/_buttons.scss`), the `make-col` mixin (in `scss/mixins/_grid.scss`), and the `color-contrast` function (in `scss/_functions.scss`) — and report the file each is declared in." (all grep-findable via the `@mixin`/`@function` keywords)
2. **[D2 Relationship]** "Trace the cross-file `@import`/`@use` graph rooted at `scss/bootstrap.scss`: which partials does it pull in (e.g. `_variables`, `_mixins`, `_buttons`), and which of those partials in turn reference the `_functions` and `_variables` partials? Show inbound + outbound include edges."
3. **[D3 Retrieval]** "Retrieve the full definition of the `button-variant` mixin from `scss/mixins/_buttons.scss`." (one real, grep-findable symbol)
4. **[D4 Architecture]** "Describe the `scss/` tree organization: the top-level partial layer (`_root`, `_reboot`, `_variables`, `_maps`, `_utilities`) vs. the `scss/mixins/`, `scss/helpers/`, `scss/forms/`, `scss/vendor/`, and `scss/utilities/` subdirectories, and how `bootstrap.scss` vs. `bootstrap-utilities.scss` / `bootstrap-grid.scss` entry points compose them."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Find duplication and naming-pattern families across the partials: e.g. all `*-variant` mixins (`button-variant`, `button-outline-variant`, `alert-variant` [verify], `list-group-item-variant` [verify]) and the `$enable-*` boolean feature-flag variables in `_variables.scss`; then link each `$enable-*` flag to the partials whose output it gates. (Config<->code linkage + similarity — favors the graph/semantic index over plain grep.)"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern=".*(button-variant|make-col|color-contrast).*"); D2->trace_call_path(qualified_name=".../bootstrap.scss", direction="both", edge="IMPORTS"); D3->get_code_snippet(qualified_name="scss/mixins/_buttons.scss::button-variant"); D4->get_architecture(path="scss/"); D5->search_code/semantic_query("*-variant mixins and $enable-* feature flags").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/scss-graph.md`, `scss-explorer.md`, `scss-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the scss tier.

---

### 31. yaml — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** kubernetes/examples (`/tmp/bench/yaml`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical Kubernetes examples repo is high-popularity and is overwhelmingly idiomatic, substantial YAML (Deployments, Services, PVs, ConfigMaps across dozens of self-contained demos), matching the plan's criterion of a widely-used, language-representative corpus.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find all top-level Kubernetes resource definitions of `kind: Service` across the repo and list the manifest files that declare them (e.g. the `frontend` Service in the guestbook example). [verify]"
2. **[D2 Relationship]** "N/A — YAML is a config/data language: the knowledge graph models manifests as documents/keys, not as symbols with call or reference edges. The relationship a reviewer would want here (a Service's `spec.selector` linking to a workload's `template.labels`) is Kubernetes-semantic, not a generic code-graph edge, so there is no `CALLS`/`HANDLES`/`IMPLEMENTS` relationship for the graph to traverse. Forcing a `trace_call_path`-style question would be unnatural and would not exercise a real graph capability."
3. **[D3 Retrieval]** "Retrieve the full manifest definition for the `cassandra` StatefulSet (the largest workload definition in the cassandra example), including its `volumeClaimTemplates`. [verify]"
4. **[D4 Architecture]** "Describe the directory/file organization of the repo: how each example (guestbook, cassandra, mysql-wordpress-pv, storage/*) groups its YAML manifests, and which top-level folders contain the most manifest files."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Across all examples, identify duplicated or near-duplicate manifest patterns and naming conventions — e.g. the recurring `app:`/`tier:` label scheme, repeated `PersistentVolumeClaim` blocks, and Services that share selector labels with multiple workloads — to surface config duplication and label<->workload links a plain text scan would miss. (Openly graph/semantic-favoring.)"

**Expected graph tools (hint, not a script):** D1->search_graph(label/kind="Service", name_pattern=".*"); D2->N/A (no relationship edges between YAML manifest keys in a generic code graph; see D2 note); D3->get_code_snippet(qualified_name="...cassandra (StatefulSet)"); D4->get_architecture(project="yaml"); D5->search_code / search_graph(semantic_query="duplicated label selector / PVC patterns").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/yaml-graph.md`, `yaml-explorer.md`, `yaml-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 counted as N/A for yaml), D5 within Group E only, Group E, the yaml tier.

---

### 32. toml — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** meilisearch/meilisearch (symlink rust) (`/tmp/bench/toml`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** A ~50k-star, production Rust search engine whose Cargo workspace carries a large, idiomatic TOML surface (root workspace manifest, ~10 per-crate `Cargo.toml`, `rust-toolchain.toml`, build profiles) — matching the plan's "popular + substantial + idiomatic for the language" repo-selection criterion; the symlink reuses the already-cloned rust repo.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level TOML tables and key arrays declared in the workspace root `Cargo.toml` — specifically the `[workspace]` table, its `members` array, and the `[profile.release]` table — and report the file each is defined in." (All grep-findable: `^\[workspace\]`, `members =`, `^\[profile.release\]`.)
2. **[D2 Relationship]** "Trace the cross-file reference structure: which directories named in the root `Cargo.toml` `[workspace] members` array contain their own `Cargo.toml` manifest (e.g. `meilisearch`, `meilisearch-types`, `milli` [verify], `index-scheduler` [verify]), and which per-crate manifests reference the shared `[workspace.package]` via `workspace = true` inheritance?"
3. **[D3 Retrieval]** "Retrieve the full `[dependencies]` table from the `meilisearch` crate's `Cargo.toml` (the binary crate) — the single largest dependency block in the workspace — verbatim with all version and feature specifiers."
4. **[D4 Architecture]** "Describe the TOML file/directory organization of the Cargo workspace: the root workspace manifest at `Cargo.toml`, the per-crate manifests under each member directory, the toolchain pin in `rust-toolchain.toml` [verify], and how `[profile.*]` build tuning is centralized at the root."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Find duplication and config↔code links: which dependencies are declared in multiple crate `Cargo.toml` files (candidates for `[workspace.dependencies]` hoisting), and which crate directories named in the root `members` array map to a real Rust source tree (`src/lib.rs`/`src/main.rs`) versus a dangling/unbuilt entry?"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern=".*workspace.*|.*profile.*", project="toml"); D2->trace_call_path(direction="both") over manifest→member references (fallback: query_graph on CONTAINS_FILE / reference edges since TOML has no call graph); D3->get_code_snippet(qualified_name="meilisearch/Cargo.toml::dependencies" [verify]); D4->get_architecture(project="toml"); D5->search_code/semantic_query for repeated dependency keys + config-to-source mapping.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/toml-graph.md`, `toml-explorer.md`, `toml-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the toml tier.

---

### 33. hcl — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** terraform-aws-modules/terraform-aws-eks (`/tmp/bench/hcl`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The most-used community Terraform EKS module (thousands of GitHub stars, millions of registry pulls); large, idiomatic, multi-submodule HCL that exercises Group E's config/IaC discovery criteria far better than a toy `.tf` fixture.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level Terraform resource and module blocks declared in the root module — e.g. the `aws_eks_cluster.this` resource and the `aws_iam_role.this` / `aws_security_group.cluster` definitions — and identify which file each is declared in." (grep-findable: every block header is plain text, e.g. `resource "aws_eks_cluster" "this"`.)
2. **[D2 Relationship]** "Show the cross-file reference graph for the cluster-name input variable (`var.cluster_name` [verify] — renamed to `var.name` on current `master`; check the pinned ref's `variables.tf`): which resources, locals, outputs, and submodule `source = "./modules/..."` invocations consume it, both inbound and outbound?"
3. **[D3 Retrieval]** "Retrieve the full definition of the `resource \"aws_eks_cluster\" \"this\"` block in the root `main.tf` — the single largest core resource in the module." (real, grep-findable symbol.)
4. **[D4 Architecture]** "Describe the file/directory organization of this module: the root `*.tf` files (`main.tf`, `variables.tf`, `outputs.tf`, `versions.tf`) versus the submodule tree under `modules/` (e.g. `eks-managed-node-group`, `self-managed-node-group`, `fargate-profile`, `karpenter`), and how the root composes the submodules."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Find duplicated / near-duplicate IAM and security-group definitions across the root module and the node-group submodules (e.g. repeated `aws_iam_role` + `aws_iam_role_policy_attachment` patterns), and surface the config<->config naming convention (`this`, `_` resource names) that ties them together — the kind of similarity/duplication link grep cannot rank."

**Expected graph tools (hint, not a script):** D1->search_graph(label="resource"|"module", name_pattern=".*aws_eks_cluster.*|.*aws_iam_role.*"); D2->trace_path(symbol="cluster_name" [verify: or "name" on master], direction=both); D3->get_code_snippet(qualified_name="aws_eks_cluster.this"); D4->get_architecture(scope="modules/"); D5->search_code/semantic_query("duplicate iam role security group node group").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/hcl-graph.md`, `hcl-explorer.md`, `hcl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the hcl tier.

---

### 34. sql — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** dbt-labs/jaffle_shop (`/tmp/bench/sql`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical dbt demo project — the most widely cloned, idiomatic example of analytics-engineering SQL (staging→marts models wired by Jinja `ref()`), small enough to index fast yet structurally representative of how real SQL/dbt repos express cross-file dependencies.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level model definitions in this dbt project — confirm that the mart models `customers` (`models/customers.sql`) and `orders` (`models/orders.sql`) and the staging models `stg_customers`, `stg_orders`, `stg_payments` (`models/staging/`) are each surfaced as defined SQL units." (grep-findable: each is a `.sql` file whose basename is the model name.)
2. **[D2 Relationship]** "Show the cross-file reference graph for the `orders` model: which staging models does it pull from via `{{ ref(...) }}` (expect `stg_orders` and `stg_payments`), and — listing them or stating 'none' — which other models reference those same staging models?"
3. **[D3 Retrieval]** "Retrieve the full body of the single largest model definition, `orders` (`models/orders.sql`), including its payment-method pivot (`sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount`, plus `coupon`/`bank_transfer`/`gift_card` branches) and the final `order_payments`→`final` CTE join." (grep-findable: `payment_method`, `credit_card_amount`.)
4. **[D4 Architecture]** "Describe the file/directory organization of the project: the `models/` root holding the two mart models, the `models/staging/` subtree holding the `stg_*` models, the seed CSVs under `data/` (e.g. `raw_customers.csv`), and the `dbt_project.yml` config at the root — and how the staging→marts layering is reflected in that layout."
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING) Surface the config↔code and duplication links a text search misses: connect `dbt_project.yml`'s `models:` materialization config and the `{{ ref('stg_*') }}` calls to the actual `stg_*.sql` files, and flag the repeated payment-method `sum(case when ...)` pivot pattern shared across the codebase. Label: semantic / config-to-code linkage — graph-favoring." [verify: exact `dbt_project.yml` model-config keys]

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern=".*(customers|orders|stg_).*"); D2->trace_path(function_name="orders", mode="calls", direction="both"); D3->get_code_snippet(qualified_name="orders"); D4->get_architecture(project="sql"); D5->search_code(pattern="payment_method case when") / search_graph(semantic_query=["payment","method","pivot","ref","config"]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/sql-graph.md`, `sql-explorer.md`, `sql-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the sql tier.

---

### 35. dockerfile — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** docker-library/official-images (`/tmp/bench/dockerfile`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, highly-starred registry of Docker official images — substantial and idiomatic Dockerfile/build content (test-fixture Dockerfiles plus a build-orchestration toolchain), matching the plan's "popular + idiomatic + substantial" repo-selection criterion for Group E build languages.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level build instructions (the `FROM`, `RUN`, `ENV`, `ENTRYPOINT`/`CMD` directives) declared in the test fixture Dockerfile `test/tests/utc/Dockerfile`, and name the base image referenced by its `FROM` stanza." (Grep-findable: `FROM`, `RUN`, `ENV`, `CMD` are literal keyword tokens; the fixture path is a stable, greppable identifier. [verify] exact fixture name)
2. **[D2 Relationship]** "Show the cross-file reference chain from the `test/config.sh` test-registry definition to the individual test directories it includes — i.e. which test entries (`testAlias`/`globalTests` entries such as `utc`, `no-hard-coded-passwords`) are wired in, and which `Dockerfile` and `run.sh` each pulls in." (Structural include/reference framing across `config.sh` → `test/tests/<name>/`.)
3. **[D3 Retrieval]** "Retrieve the full contents of the largest single build-test definition: the orchestration script `test/run.sh` (the harness that builds each image and runs its per-test `run.sh`)." (One real, grep-findable named file.)
4. **[D4 Architecture]** "Describe the directory/file organization of the repository: the relationship between the manifest `library/` entries, the `bashbrew/` tooling, and the `test/` harness tree (`test/config.sh`, `test/tests/`, `test/tests/<name>/{Dockerfile,run.sh,expected-std-out.txt}`)." (Structural / get_architecture framing.)
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Across all fixture Dockerfiles under `test/tests/`, find duplicated/near-duplicate build patterns (e.g. repeated `FROM debian`/`FROM alpine` base selections and the recurring `run.sh`+`expected-std-out.txt` pairing convention), and surface the config<->code link between a `library/<image>` manifest's `Directory:` field and the actual build context it names. Explicitly graph-favoring: relies on similarity/duplication detection and manifest-to-build-context linkage that plain text search cannot rank."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern="FROM|RUN|ENV|CMD", project="dockerfile"); D2->trace_path(direction=both, from="test/config.sh"); D3->get_code_snippet(qualified_name="test/run.sh"); D4->get_architecture(project="dockerfile"); D5->search_code(semantic_query="duplicate base-image FROM patterns across test fixtures") / search_graph(semantic_query="manifest Directory field to build context").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/dockerfile-graph.md`, `dockerfile-explorer.md`, `dockerfile-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the dockerfile tier.

---

### 36. clojure — D (Functional & Formal)

**Repo:** clojure/clojure (`/tmp/bench/clojure`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical Clojure language implementation — the most-starred, most idiomatic and substantial Clojure codebase in existence (a self-hosting Lisp whose `core.clj` defines the language in itself), satisfying the plan's "popular + idiomatic + substantial" repo-selection criteria for Group D.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the public definitions in `clojure.set` (`src/clj/clojure/set.clj`) — confirm the tool surfaces `union`, `intersection`, `difference`, `select`, `project`, `rename`, and `join`. Are `defn`/`defn-` forms recognized as defined symbols, and are private (`defn-`) defs distinguished from public ones?"
2. **[D2 Relationship]** "For `clojure.core/merge-with` (`src/clj/clojure/core.clj`), show callers and callees in both directions — e.g. that it sits over `reduce`/`assoc`/`get` and is itself used by higher-level seq/map helpers. Does the call graph resolve cross-form references inside a single namespace?"
3. **[D3 Retrieval]** "Retrieve the full source of the `clojure.core/lazy-seq` macro from `src/clj/clojure/core.clj`, with exact start/end lines including its docstring and the surrounding `defmacro` form."
4. **[D4 Architecture]** "Describe the top-level architecture: the split between the Clojure layer (`src/clj/clojure/*.clj`: `core.clj`, `set.clj`, `string.clj`, `walk.clj`, `zip.clj`, `pprint/`) and the Java runtime layer (`src/jvm/clojure/lang/*.java`: `RT.java`, `Compiler.java`, `PersistentVector.java`, `Var.java`, `Namespace.java`). How does the tool present the two-language bootstrap structure?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the persistent immutable collection implementations regardless of name — locate the family of `Persistent*`/`Transient*` classes in `src/jvm/clojure/lang/` (e.g. `PersistentVector`, `PersistentHashMap`, `PersistentTreeMap`, `TransientVector`) and the interfaces they share (`IPersistentCollection`, `IEditableCollection`). This rewards semantic/structural grouping over a single literal grep."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern in clojure.set, label=Definition); D2->trace_call_path(qualified_name="clojure.core/merge-with", direction="both"); D3->get_code_snippet(qualified_name="clojure.core/lazy-seq"); D4->get_architecture(scope=repo); D5->search_code/semantic_query("persistent immutable collection implementation") + search_graph(name_pattern=".*Persistent.*").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/clojure-graph.md`, `clojure-explorer.md`, `clojure-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the clojure tier.

---

### 37. fsharp — D (Functional & Formal)

**Repo:** giraffe-fsharp/Giraffe (`/tmp/bench/fsharp`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Giraffe is the most-starred idiomatic F# web framework (functional, combinator-based `HttpHandler` pipeline), substantial and pure-F#, matching the plan's "popular + idiomatic + non-trivial size" repo-selection criteria for Group D.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the core `HttpHandler` type and the `HttpFunc` type alias, plus the public handler combinators `choose`, `route`, and `routef` — where are they declared and what are their signatures?" (all plain identifiers: `HttpHandler`/`HttpFunc`/`choose` in `Core.fs`, `route`/`routef` in `Routing.fs` — grep-findable too)
2. **[D2 Relationship]** "Trace the call relationships (both directions) around the `>=>` compose operator and `compose` in `Giraffe.Core`: which handlers/combinators invoke it, and what does it call internally to chain two `HttpHandler`s?"
3. **[D3 Retrieval]** "Retrieve the full source of the `routef` function in `Routing.fs` (the typed-format route handler)." (`routef` is a plain function name — grep-findable too)
4. **[D4 Architecture]** "Describe the module/file organization of the `src/Giraffe` project — how are concerns split across the source files, e.g. `Core` (handlers + response writers), `Routing`/`EndpointRouting`, `Negotiation`, `Streaming`, and the model-binding files (`ModelParser`, `ModelValidation`, `FormatExpressions`)?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find all response-writing combinators semantically similar to `text`/`json` — i.e. handlers that set a body and content-type and return an updated `HttpContext` — and surface the shared `HttpHandler` shape they all follow. Note these live alongside `text`/`json` in `Core.fs` plus the content-negotiating handlers in `Negotiation.fs`; a semantic query groups them by behaviour where plain text search (scanning one file for one keyword) would miss the cross-file set."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*HttpHandler.*|choose|routef?", label="Definition"); D2->trace_call_path(qualified_name="Giraffe.Core.compose", direction="both"); D3->get_code_snippet(qualified_name="Giraffe.Routing.routef"); D4->get_architecture(path="src/Giraffe"); D5->search_code/semantic_query("response writer handler sets body and content-type returning HttpContext").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/fsharp-graph.md`, `fsharp-explorer.md`, `fsharp-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the fsharp tier.

---

### 38. julia — D (Functional & Formal)

**Repo:** SciML/DifferentialEquations.jl (`/tmp/bench/julia`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Flagship SciML solver suite (~3.1k stars, the canonical Julia ODE/SDE/DAE meta-package). NOTE: this repo is a *thin meta-package* — its entire `src/` is a single ~117-byte file (`src/DifferentialEquations.jl`) that does nothing but `@reexport` its dependencies. The actual solver code (`solve`, `__init`, algorithm structs) lives in *external* packages (SciMLBase, OrdinaryDiffEq, …), NOT in this checkout. Questions below are anchored on what is genuinely present in *this* repo (the module declaration + its reexport/dependency surface); symbols that live only in the reexported packages are explicitly labeled as such and carry `[verify]`.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Where is the public `DifferentialEquations` module declared, and what does the module body actually define? Expect the single declaration `module DifferentialEquations` in `src/DifferentialEquations.jl`, whose body is `using Reexport` plus a list of `@reexport using <Pkg>` statements and nothing else." (Both the `module DifferentialEquations` token and the `@reexport using` lines are literal, grep-findable text in the one source file — plain `grep -n '@reexport' src/DifferentialEquations.jl` recovers the full API surface, so this is symmetric with text search.)
2. **[D2 Relationship]** "This package defines no intra-repo functions, so there are no `CALLS` edges to trace. Instead, enumerate the *import/reexport dependency relationship*: which packages does the `DifferentialEquations` module pull in via `@reexport using` / `using` (expect `Reexport`, `SciMLBase`, `OrdinaryDiffEq` on current `master`; older tags additionally list `DiffEqBase`, `StochasticDiffEq`, `DiffEqNoiseProcess`, `DelayDiffEq`, etc. [verify] — depends on the checked-out ref)? This is an IMPORTS-edge / module-dependency question, not a call-graph question." (Honest framing: D2's usual call-relationship target is genuinely absent here; the meaningful relationship in a pure reexport shell is the IMPORTS edge set, which both grep and the graph can recover.)
3. **[D3 Retrieval]** "Retrieve the exact source of the top-level module block in `src/DifferentialEquations.jl` — the entire `module DifferentialEquations ... end` with its `using Reexport` and `@reexport using` statements (the whole file is only ~8–15 lines depending on ref)." (The block is a real, grep-findable region in the single source file.)
4. **[D4 Architecture]** "Describe the structural organization: that `src/` holds exactly one tiny file, that `Project.toml` declares the real dependency set (`OrdinaryDiffEq`, `Reexport`, `SciMLBase` on current `master` [verify] — older tags list many more), and explain how DifferentialEquations.jl acts as a meta-package that aggregates and re-exposes external solver packages rather than implementing solvers itself. Contrast the near-empty `src/` against the breadth of the reexported namespace."
5. **[D5 Cross-cutting/Semantic]** "*(graph-favoring)* The user-facing API of this package (e.g. `solve`, `solve!`, `init`/`__init`, and algorithm-constructor structs such as `Tsit5`, `Rodas5` [verify]) is NOT defined in this repo — it is surfaced transitively through `@reexport`. Using semantic/cross-package reasoning rather than literal substring search over this checkout (where those tokens never appear), link each `@reexport using <Pkg>` declaration to the kind of public symbols that package contributes to the merged `DifferentialEquations` namespace. This favors the graph's import/reexport modeling over grep, since grep on this repo finds none of the surfaced symbols." (Graph-favoring is legitimate here precisely because the symbols are absent from the local text — but note this also stresses whether the indexer crosses package boundaries at all; if it indexes only this checkout, BOTH tools will find nothing, which is itself a fair, informative result.)

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="DifferentialEquations", label="Module|Function"); D2->trace_call_path(qualified_name="...DifferentialEquations", direction="both") falling back to search_graph(relationship="IMPORTS") / get_architecture for the dependency edges; D3->get_code_snippet(qualified_name="DifferentialEquations"); D4->get_architecture(project="julia"); D5->search_code/semantic_query("reexported solver algorithm constructors / @reexport dependency surface"). NOTE: hints referencing `solve`/`__init` are intentionally absent because those symbols are not in this repo.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/julia-graph.md`, `julia-explorer.md`, `julia-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the julia tier.

---

### 39. vimscript — C (Dynamic & Scripting)

**Repo:** SpaceVim/SpaceVim (`/tmp/bench/vimscript`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Top-starred (~20k) vimscript project; a large, idiomatic autoload-based codebase with deep `SpaceVim#...#fn` namespacing, exercising real-world vimscript structure per the plan's "popular + substantial + idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the autoload entry-point functions `SpaceVim#begin` and `SpaceVim#end` defined in `autoload/SpaceVim.vim`. Does the graph surface them as distinct function definitions with their file and line, matching what `grep -n 'function! SpaceVim#'` would return?"
2. **[D2 Relationship]** "Starting from `SpaceVim#layers#load` (in `autoload/SpaceVim/layers.vim`), trace the call relationships in both directions: which functions invoke it (e.g. its own recursive call when loading a list of layers, plus any external callers from the config/bootstrap path [verify — callers live in `config/`-style files not confirmed here]) and which helpers it actually calls (e.g. `s:list_layers`, the dynamic `SpaceVim#layers#{layer}#set_variable` dispatch, and `SpaceVim#logger#info`/`#warn`)?"
3. **[D3 Retrieval]** "Retrieve the full source of the single function `SpaceVim#layers#isLoaded` from `autoload/SpaceVim/layers.vim` — exact body, signature, and line range, nothing else."
4. **[D4 Architecture]** "Describe the top-level architecture of the `autoload/SpaceVim/` tree: the role of the sibling modules (`layers.vim`, `plugins.vim`, `mapping.vim`, `logger.vim`, `api.vim`) and the `api/`, `layers/`, `plugins/`, `mapping/` subdirectories. How is functionality partitioned across autoload namespaces?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across the autoload tree, find the functions responsible for *layer loading / enabling / reporting* (semantically related to `SpaceVim#layers#load`, `#disable`, `#isLoaded`, `#report`) even when their names don't share a single grep token. Surface naming-pattern duplication across the `s:list_layers` / `s:find_layers` private helpers and any config<->code links between `g:spacevim_*` options and the code that reads them."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="SpaceVim#(begin|end)"); D2->trace_call_path(name="SpaceVim#layers#load", direction="both"); D3->get_code_snippet(qualified_name="SpaceVim#layers#isLoaded"); D4->get_architecture(scope="autoload/SpaceVim"); D5->search_code/semantic_query("layer load enable report").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/vimscript-graph.md`, `vimscript-explorer.md`, `vimscript-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the vimscript tier.

---

### 40. nix — C (Dynamic & Scripting)

**Repo:** nix-community/home-manager (`/tmp/bench/nix`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** home-manager is the de-facto standard for declarative per-user Nix configuration (one of the most-starred Nix-language repos), with thousands of idiomatic `.nix` modules — substantial, real-world Nix that matches the plan's "popular + idiomatic + large" repo-selection criteria.

**Language note (config/data language):** Nix is a declarative configuration language. "Functions" are attribute-set / lambda definitions; there is no conventional runtime call graph the indexer can resolve the way it does for an imperative language. Dimensions that depend on call relationships (D2) or semantic clustering (D5) are therefore handled honestly below — marked N/A with a reason where they do not naturally apply, rather than forced into unnatural questions.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the string-helper `storeFileName` in `modules/lib/strings.nix`. Confirm the graph resolves the same symbol that a plain `grep -rn 'storeFileName'` finds (grep-findable, plain-symbol)."
2. **[D2 Relationship]** "N/A for this config/data language. Nix module helpers are referenced by attribute-set composition and lazy interpolation, not a resolvable call graph; the indexer does not produce reliable CALLS edges between `.nix` lambdas, so a relationship/neighborhood question would not exercise a real graph capability. (If any CALLS edges are present for `dag.nix` helpers, they should be treated as best-effort, not scored.)"
3. **[D3 Retrieval]** "Retrieve the full definition of the DAG helper `entryAfter` from `modules/lib/dag.nix` — a single named symbol that also appears verbatim under `grep -rn 'entryAfter' modules/lib/dag.nix` (grep-findable, plain-symbol)."
4. **[D4 Architecture]** "Describe the structural organization of the `modules/` tree — how `modules/lib/`, `modules/programs/`, and `modules/services/` are arranged and where `modules/home-environment.nix` sits relative to them."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Surface the family of DAG entry helpers in `modules/lib/dag.nix` (`entryAfter`, `entryBefore`, `entryBetween`, `entryAnywhere` [verify], `entriesBetween` [verify]) as a related cluster, and identify whether any other `modules/lib/` file exposes a similarly-shaped helper family. This is a similarity/naming-pattern query where a structural/semantic index can group related symbols better than a single grep pattern. (If the index offers no semantic grouping over `.nix`, mark N/A with that reason — do not force it.)"

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="storeFileName"); D2->N/A (no reliable CALLS edges for Nix lambdas); D3->get_code_snippet(qualified_name="...dag.entryAfter"); D4->get_architecture(scope="modules/"); D5->search_code/semantic_query("DAG entry helper family") or N/A if no semantic grouping over .nix.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/nix-graph.md`, `nix-explorer.md`, `nix-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 recorded as N/A), D5 within Group C only, Group C, the nix tier.

---

### 41. commonlisp — D (Functional & Formal)

**Repo:** lem-project/lem (`/tmp/bench/commonlisp`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Lem is one of the most-starred, actively-maintained Common Lisp projects (a full editor/IDE written in idiomatic CL with macros, CLOS, and a multi-package system), giving substantial real-world structure for every dimension — matching the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the command macro `define-command` and the buffer constructor `make-buffer` — both grep-findable top-level forms — and report their defining files and forms (`defmacro` vs `defun`)."
2. **[D2 Relationship]** "Starting from `current-buffer`, show the call relationships in both directions: what does it call, and which commands/functions (e.g. buffer-editing functions) call it? [verify the exact callers against the pinned commit]"
3. **[D3 Retrieval]** "Retrieve the full source of the `define-major-mode` macro [verify] — a single named symbol, grep-findable — so a reviewer can read its entire body without opening the file."
4. **[D4 Architecture]** "Describe Lem's top-level module organization: the `src/` core (buffer/point/window), `frontends/` (e.g. sdl2, ncurses), `extensions/`, and `modes/`, and how the `.asd` system definitions (e.g. `lem.asd`) tie packages together."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the cluster of point/cursor-movement helpers (e.g. `move-point`, `character-offset`, `line-offset` [verify]) and surface duplicated movement-helper patterns across `src/` and `modes/` by their shared call relationships — plain grep finds the literal names but cannot group them by who-calls-what or rank them by structural importance the way the graph does."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="define-command|make-buffer"); D2->trace_call_path(name="current-buffer", direction="both"); D3->get_code_snippet(qualified_name="define-major-mode"); D4->get_architecture(); D5->search_code("point/cursor movement by line or character") then trace_call_path on the matched movement helpers to cluster by shared callers.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/commonlisp-graph.md`, `commonlisp-explorer.md`, `commonlisp-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the commonlisp tier.

---

### 42. elm — D (Functional & Formal)

**Repo:** elm/core (`/tmp/bench/elm`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** `elm/core` is the foundational standard-library package for the Elm language — the `.elm` modules (`Basics`, `List`, `Maybe`, `Result`, `String`, `Dict`, `Task`, …) that every Elm program implicitly depends on. It is ~85% Elm source (popular OSS, ~2.8k stars) and is the canonical place where the language's idiomatic `.elm` code actually lives, satisfying the plan's "popular + representative" criterion for the **elm** extractor.

> **Repo-selection note (recorded for fairness):** The obvious candidate, `elm/compiler`, is ~95% **Haskell** (the compiler/builder/terminal sources in `compiler/src/`, `builder/src/`, `terminal/src/`); its `.elm` content is only test fixtures and `reactor/` UI assets and does NOT contain the standard library. Indexing `elm/compiler` under LANGUAGE=elm would measure almost nothing real, so this tier targets `elm/core`, where the Elm `.elm` sources genuinely live. All symbols cited below are public, in-repo `.elm` identifiers (grep-findable in `src/*.elm`) unless tagged `[verify]`.

**The 5 questions** (bespoke; dimension in brackets):

1. **[D1 Definition/API]** "In the `Maybe` module (`src/Maybe.elm`), locate the public API — specifically `withDefault`, `map`, `andThen`, and the `Maybe` type itself (with its `Just`/`Nothing` constructors). Are all four discoverable by name?" *(All four are plain, grep-findable identifiers in `src/Maybe.elm`: the module header exposes `Maybe(..)`, `andThen`, `map`, `withDefault`. Grep finds these as easily as the graph; this question is intentionally symmetric.)*

2. **[D2 Relationship]** "Starting from `Maybe.andThen` in `src/Maybe.elm`, show the call relationship in both directions: which functions in `Maybe` reference `andThen`, and what does `andThen` itself reference (it pattern-matches on `Just`/`Nothing`). Then compare against `Result.andThen` in `src/Result.elm` to confirm the two are distinct symbols and not merged into one."

3. **[D3 Retrieval]** "Retrieve the complete source of `List.foldr` from `src/List.elm` — the named symbol, exact body, including the tail-recursive helper it delegates to (`foldrHelper`)." *(`foldr` and `foldrHelper` are canonical, grep-findable identifiers in `src/List.elm`; a grep for `foldr` returns the definition directly. Symmetric with Q1 — not a graph-only target.)*

4. **[D4 Architecture]** "Describe the module/directory organization of `src/`: how the core modules (`Basics`, `List`, `Maybe`, `Result`, `String`, `Dict`, `Set`, `Array`, `Task`, `Char`, `Bitwise`, `Tuple`) sit at the top of `src/`, and what lives under the `src/Elm/` and `src/Platform/` subdirectories (e.g. JSON, the platform/runtime glue)." *(Structural; graph framing acceptable, but a directory listing also answers it.)*

5. **[D5 Cross-cutting/Semantic]** "**(graph-favoring — semantic/duplication.)** Across the core modules, surface the family of functions that are semantic near-duplicates of `Maybe.map` — the `map`/`map2`…`map5` family replicated across `Maybe`, `Result`, `List`, and `Task`, plus the recurring `andThen` monadic-bind pattern — as one cross-module cluster. Plain text search can only approximate this via brittle per-name greps across files; the value is grouping the replicated *shape*, not matching one literal name."

**Expected graph tools (hint, not a script):** D1->`search_graph(name_pattern="withDefault|andThen|^map$|^Maybe$", project="elm")`; D2->`trace_call_path(qualified_name="Maybe.andThen", direction="both")`; D3->`get_code_snippet(qualified_name="List.foldr")`; D4->`get_architecture(project="elm")`; D5->`search_code(semantic_query="map family / andThen monadic bind across Maybe Result List Task")` then `search_graph(name_pattern="^map[2-5]?$|^andThen$")`.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/elm-graph.md`, `elm-explorer.md`, `elm-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the elm tier.

---

### 43. fortran — B (Systems & Low-level)

**Repo:** fortran-lang/stdlib (`/tmp/bench/fortran`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The community-standard Fortran Standard Library — the most-starred, most-active idiomatic modern Fortran codebase (modules, derived types, generic interfaces, `.fypp` templates), satisfying the plan's "popular + substantial + idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the public sorting API in `stdlib_sorting`: where are the `sort`, `ord_sort`, and `sort_index` interfaces/subroutines defined, and what are their qualified names?"
2. **[D2 Relationship]** "Take `ord_sort` in `stdlib_sorting` and show its call relationships in both directions — which internal helper procedures (e.g. the merge/insertion-sort workers it dispatches to) it calls, and which public entry points reach it."
3. **[D3 Retrieval]** "Retrieve the full source of the `string_type` derived type defined in `stdlib_string_type`."
4. **[D4 Architecture]** "Describe the top-level architecture of stdlib: how `src/` is organized into topical subdirectories (`sorting/`, `strings/`, `stats/`, `linalg/`, …), each holding its `stdlib_*` module sources as `.fypp` templates (preprocessed to `.f90`), and how the build (`CMakeLists.txt` per subdir) and `test/` directories relate to those module sources."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Starting from `mean` in `stdlib_stats`, map the family of statistical reduction/aggregation procedures it is conceptually related to (`var`, `moment`, `corr`, `cov`, `median`) — surface how they are grouped/connected structurally (shared module, common `.fypp` generation, cross-references) rather than by any single shared name token. (Honest note: each target is itself a plainly-named public symbol that grep can find individually; the graph-favoring claim is about recovering the *relationship/grouping* across them in one pass, not about hidden symbols.)"

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*(sort|ord_sort|sort_index).*", label="Function|Subroutine|Interface"); D2->trace_call_path(qualified_name="...ord_sort", direction="both"); D3->get_code_snippet(qualified_name="...string_type"); D4->get_architecture(project="fortran"); D5->search_graph(name_pattern=".*(mean|var|moment|corr|cov|median).*", project="fortran") + trace_call_path to expose the shared-module/cross-reference grouping.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/fortran-graph.md`, `fortran-explorer.md`, `fortran-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the fortran tier.

---

### 44. cuda — B (Systems & Low-level)

**Repo:** NVIDIA/cuda-samples (`/tmp/bench/cuda`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** NVIDIA's official, widely-starred reference corpus of idiomatic CUDA C/C++ kernels and host code — substantial, multi-category, and the canonical exemplar of host/device split that a code graph must handle for the language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the error-checking helper `checkCudaErrors` and the device-selection helper `findCudaDevice` in `Common/helper_cuda.h`; what are their signatures and where are they declared?" (both are grep-findable macro/template names used across nearly every sample)
2. **[D2 Relationship]** "Starting from the `vectorAdd` kernel in `Samples/0_Introduction/vectorAdd/vectorAdd.cu`, trace the call graph in both directions: which host function (`main`) launches it, and which device/runtime symbols does the launch path touch (e.g. `cudaMalloc`, `cudaMemcpy`, `checkCudaErrors`)?"
3. **[D3 Retrieval]** "Retrieve the full body of the `MatrixMulCUDA` templated kernel from `Samples/0_Introduction/matrixMul/matrixMul.cu`." (single named symbol, grep-findable)
4. **[D4 Architecture]** "Describe the top-level structure of the repo: the numbered sample-category directories (`0_Introduction`, `1_Utilities`, `2_Concepts_and_Techniques`, `3_CUDA_Features`, `4_CUDA_Libraries`, `5_Domain_Specific`, `6_Performance` [verify]) and the shared `Common/` helper headers, and how individual samples depend on `Common/`."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Across the corpus, find the samples that semantically implement a parallel reduction pattern — e.g. `reduce`, `reduction`, `threadFenceReduction` [verify] — and surface the shared idiom (shared-memory accumulation + `__syncthreads()`), plus the config<->code link between each sample's `Makefile`/`*_vs*.vcxproj` and its `.cu` source. Labeled graph-favoring: grep alone cannot cluster by reduction semantics."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="checkCudaErrors|findCudaDevice"); D2->trace_call_path(name="vectorAdd", direction="both"); D3->get_code_snippet(qualified_name="MatrixMulCUDA"); D4->get_architecture(); D5->search_code/semantic_query("parallel reduction shared memory __syncthreads").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/cuda-graph.md`, `cuda-explorer.md`, `cuda-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the cuda tier.

---

### 45. cobol — B (Systems & Low-level)

**Repo:** OCamlPro/gnucobol (`/tmp/bench/cobol`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** GnuCOBOL is the canonical open-source COBOL toolchain; its shipped test corpus is large, idiomatic COBOL with real PROGRAM-IDs, COPY/CALL usage and division structure — substantial and grep-findable, matching the plan's "popular + idiomatic + substantial" repo-selection criteria for the Systems group. (Note: the bulk of committed, grep-findable COBOL lives inline in the GNU Autotest `.at` files under `tests/testsuite.src/`; the NIST COBOL85 `.cob` sources under `tests/cobol85/` are downloaded at test time rather than committed.)

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the COBOL program units (the `PROGRAM-ID` definitions) embedded in the GnuCOBOL test corpus — primarily the inline programs inside `tests/testsuite.src/*.at` — and report where the program `prog` [verify] and any sample with a `PROGRAM-ID. main` [verify] are declared. Both must be reachable by a plain grep for `PROGRAM-ID`."
2. **[D2 Relationship]** "For a test program that issues a static `CALL \"subprog\"` to a called subprogram (e.g. a caller/callee pair in the `run_*` autotest cases such as `run_misc.at` / `run_file.at` [verify]), show the call relationship in both directions: which program(s) CALL the subprogram, and what that subprogram itself CALLs."
3. **[D3 Retrieval]** "Retrieve the full source of the COBOL paragraph/section named `MAIN-LOGIC` [verify] (or, if absent, the program's `PROCEDURE DIVISION` body for the program-id you confirm in Q1) exactly as written, including its `DISPLAY`/`MOVE` statements. The target name is a literal identifier, so it must also be locatable by a plain grep."
4. **[D4 Architecture]** "Describe the structural organization of the COBOL test corpus: the `tests/` directory layout — the GNU Autotest harness files in `tests/testsuite.src/*.at` (e.g. `run_misc.at`, `run_file.at`, `syn_*.at` [verify]) versus the NIST COBOL85 conformance suite under `tests/cobol85/` (downloaded `newcob.val` rather than committed `.cob` files) [verify] — and how copybooks (`COPY` members) relate to the programs that include them."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Find COBOL programs that are semantically near-duplicates — e.g. the family of arithmetic/`COMPUTE` conformance tests or the `DISPLAY`-only smoke programs — that share near-identical PROCEDURE DIVISION shape but differ only in literals/data items. This is openly graph-favoring (similarity / naming-pattern clustering) and is not expected to be reproducible by a single grep."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern="(?i)PROGRAM-ID|MAIN|prog"); D2->trace_call_path(name="subprog", direction="both"); D3->get_code_snippet(qualified_name="...MAIN-LOGIC"); D4->get_architecture(scope="tests/"); D5->search_code(semantic_query="COBOL program with COMPUTE/DISPLAY conformance body")/search_graph(semantic).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/cobol-graph.md`, `cobol-explorer.md`, `cobol-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the cobol tier.

---

### 46. verilog — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** YosysHQ/picorv32 (`/tmp/bench/verilog`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Canonical, widely-cited (~3k stars) size-optimized RISC-V CPU core written in idiomatic, substantial single-file Verilog with multiple module variants and PCPI co-processors — a representative HDL target per the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level `module` definitions declared in this repo (e.g. `picorv32`, `picorv32_axi`, `picorv32_wb`, `picorv32_pcpi_mul`, `picorv32_pcpi_div`) and report the file and line where each is declared." (All are grep-findable via `module <name>` in `picorv32.v`.)
2. **[D2 Relationship]** "Map the cross-module structure around the core `picorv32`: which wrapper modules embed it (`picorv32_axi`, `picorv32_wb`) and which sub-modules it pulls in (the PCPI units `picorv32_pcpi_mul` / `picorv32_pcpi_fast_mul` / `picorv32_pcpi_div`, plus the `picorv32_regs` register file). Note: picorv32 is plain Verilog-2005 with no package `import`s, and the indexer's Verilog call-extraction does NOT model `module_instantiation` as a graph edge [verify], so the instantiation links must be recovered from the grep-findable instantiation sites (`picorv32 <inst> (...)`, `picorv32_pcpi_* <inst> (...)`) rather than from a graph reference edge — report which side (graph vs grep) actually surfaces each link."
3. **[D3 Retrieval]** "Retrieve the full source of the largest module definition in this repo — the core `picorv32` module — with its exact start/end boundaries (`module picorv32` … `endmodule`)." (grep-findable boundary tokens.)
4. **[D4 Architecture]** "Describe the file/module organization of the repo: how the synthesizable RTL (`picorv32.v`), the bus-wrapper variants (AXI/Wishbone), the co-processor (PCPI) modules, and the testbench/firmware directories are arranged relative to one another."
5. **[D5 Cross-cutting/Semantic]** "[GRAPH-FAVORING] Identify naming-pattern / duplication structure across the bus adapters: which modules share the `picorv32_axi*` vs `picorv32_wb*` prefix families, and which interface-signal groups (e.g. the `mem_axi_*` AXI handshake ports) recur across `picorv32_axi` and `picorv32_axi_adapter`? Surface near-duplicate port/parameter blocks that plain grep would not cluster semantically. Note: in fast (non-LSP) mode `SIMILAR_TO`/`SEMANTICALLY_RELATED` edges are typically absent [verify], so the clustering leans on Module-node naming families plus `search_code`, not on a dedicated similarity edge."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Module"|definition, name_pattern="picorv32.*"); D2->search_graph(name_pattern="picorv32.*") for the Module-node set + trace_call_path(qualified_name="picorv32", direction="both") best-effort (Verilog instantiation is NOT a modeled edge [verify], so cross-check against grep instantiation sites); D3->get_code_snippet(qualified_name="picorv32"); D4->get_architecture(); D5->search_code/semantic_query(prefix-family + recurring AXI port clustering; semantic edges sparse in fast mode [verify]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/verilog-graph.md`, `verilog-explorer.md`, `verilog-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the verilog tier.

---

### 47. emacslisp — C (Dynamic & Scripting)

**Repo:** magit/magit (`/tmp/bench/emacslisp`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Magit is the most-starred and de-facto reference Emacs Lisp project (a full Git porcelain), large and idiomatic enough to exercise the indexer's `defun`/`defmacro`/`defcustom` handling — matching the plan's "popular, substantial, idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level interactive command definitions whose names match `magit-stage.*` (e.g. `magit-stage`, `magit-stage-file`, `magit-stage-modified`) and report which file each is defined in." — all grep-findable as `(defun magit-stage` etc. in `lisp/magit-apply.el`.
2. **[D2 Relationship]** "For `magit-refresh`, show both directions of its call graph: which commands invoke it (inbound, e.g. `magit-status`/post-command refresh hooks) and which helpers it calls (outbound, e.g. `magit-refresh-buffer`, `magit-run-hook-with-benchmark`)."
3. **[D3 Retrieval]** "Retrieve the full source of the macro `magit-insert-section` exactly as defined in `lisp/magit-section.el`." — single named symbol, grep-findable as `(defmacro magit-insert-section`.
4. **[D4 Architecture]** "Describe the module/file organization of the `lisp/` directory: how `magit.el` ties together the feature files (`magit-status.el`, `magit-diff.el`, `magit-log.el`, `magit-commit.el`, `magit-process.el`, `magit-git.el`) and where the section-rendering layer (`magit-section.el`) sits relative to them."
5. **[D5 Cross-cutting/Semantic]** "[graph-favoring] Find the cluster of functions that shell out to Git — i.e. semantically 'run a git subprocess' — such as `magit-call-git`, `magit-run-git`, `magit-run-git-async`, `magit-start-git`, `magit-git-string`, `magit-git-insert`; report naming-pattern duplication and which are thin wrappers over which."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="magit-stage.*", label="Function"); D2->trace_call_path(name="magit-refresh", direction="both"); D3->get_code_snippet(qualified_name="magit-insert-section"); D4->get_architecture(scope="lisp/"); D5->search_code/semantic_query("functions that run a git subprocess").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/emacslisp-graph.md`, `emacslisp-explorer.md`, `emacslisp-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the emacslisp tier.

---

### 48. json — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** SchemaStore/schemastore (`/tmp/bench/json`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, high-traffic registry of JSON Schemas (thousands of `.json` schema files under `src/schemas/json/` plus the `src/api/json/catalog.json` index); it is the most idiomatic and substantial pure-JSON/JSON-Schema corpus on GitHub, matching the plan's "popular + idiomatic + substantial" repo-selection criteria for Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the two top-level registry-defining documents: the `schemas` array in `src/api/json/catalog.json` (the index) and the catalog's own meta-schema `src/schemas/json/schema-catalog.json`. Enumerate the top-level keys these files declare (e.g. `schemas`, `version`, `url`, `fileMatch`, `name`, `description`) and report which file declares each." (Grep-findable: `"fileMatch"`, `"schemas"`, and the filename `catalog.json` appear verbatim; this dimension must be answerable by plain grep, not graph-only. Caveat: whether the graph emits `Definition` nodes for JSON object keys is itself dubious for a pure-data language [verify].)
2. **[D2 Relationship]** "N/A — JSON is a config/data language. The relationship a reviewer would actually want here is JSON-Schema-semantic: a `$ref`/`$schema` string pointer linking one schema document to another, or a `catalog.json` entry's `url`/`fileMatch` mapping to a schema file. These are string-valued pointers inside data, not generic code-graph symbols — the knowledge graph models these `.json` files as documents/keys, not as nodes joined by `CALLS`/`HANDLES`/`IMPLEMENTS`/reference edges, so there is no relationship edge for `trace_path` to traverse. Forcing a `trace_call_path`-style question would be unnatural and would not exercise a real graph capability; this linkage is instead surfaced as the explicitly graph/semantic-favoring D5 below."
3. **[D3 Retrieval]** "Retrieve a single large definition block verbatim: the full `src/schemas/json/github-workflow.json` schema (one of the largest individual schema documents in the repo), or the largest single entry-set block of the `schemas` array inside `src/api/json/catalog.json`." (Grep-findable filename; this dimension must be answerable by plain grep on the filename, not graph-only.)
4. **[D4 Architecture]** "Describe the directory/file organization: how `src/schemas/json/` (the schema documents), `src/test/<schema-name>/` (per-schema fixtures), and `src/api/json/catalog.json` (the index) are organized and how they relate."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Detect duplication and naming-pattern links — find schema files in `src/schemas/json/` that share near-identical structure (e.g. the many `tsconfig*.json` / `*.config.json` variants), and link each `catalog.json` entry's `url`/`fileMatch` to the actual schema file it points at (index<->document linkage). Plain text search cannot surface structural similarity or resolve the index-to-file mapping. (Openly graph/semantic-favoring.)"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern=".*catalog.*|.*schemas.*", project="json") [verify the graph emits Definition nodes for JSON keys; fall back to search_code on the literal keys]; D2->N/A (JSON has no call/reference graph between data keys in a generic code graph; see D2 note); D3->get_code_snippet(qualified_name="src/schemas/json/github-workflow.json" [verify JSON files are snippet-addressable]); D4->get_architecture(project="json"); D5->search_code / search_graph(semantic_query="near-duplicate schema structure + catalog url<->file links").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/json-graph.md`, `json-explorer.md`, `json-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 counted as N/A for json), D5 within Group E only, Group E, the json tier.

---

### 49. xml — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** spring-projects/spring-petclinic (symlink java) (`/tmp/bench/xml`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** Spring PetClinic is the canonical, high-popularity reference Spring Boot app; its Maven `pom.xml`, cache and CI XML are idiomatic, hand-maintained build/config markup — substantial enough to exercise structural XML queries, matching the plan's "popular + idiomatic in-language" repo criterion.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "In the root `pom.xml`, list the top-level `<plugin>` definitions under `<build>` — e.g. the `spring-boot-maven-plugin` and the `spring-javaformat-maven-plugin` [verify] entries. (Symmetric: grep-findable too — artifactIds appear as literal `<artifactId>` text under `<plugin>`.)"
2. **[D2 Relationship]** "N/A for build/config XML. A root `pom.xml` has no call/reference graph in the codebase-memory sense — `<parent>` inheritance and `${...}` property placeholders are Maven's own resolution mechanics, not CALLS/IMPORTS edges the graph models. (Best-effort, non-scoring sub-probe: which `<parent>` POM does this inherit from — `spring-boot-starter-parent` — answerable by either tool from literal text; documented here only so the dimension is acknowledged, not forced into a fake relationship query.)"
3. **[D3 Retrieval]** "Retrieve the full `<dependencies>` block (the largest single definition) from the root `pom.xml`. (Symmetric: grep-findable too — the `<dependencies>` open/close tags are literal anchors; verbatim retrieval is the test, not structure.)"
4. **[D4 Architecture]** "Describe the XML/config file organization of the repo: where build (`pom.xml`), CI workflow (`.github/workflows/*.yml` [verify]), and resource config (`src/main/resources/**`) markup live relative to the Java source tree."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Within the XML/config surface, which Maven property keys defined in `pom.xml` (e.g. `<java.version>`, `<webjars-bootstrap.version>` [verify]) are reused via `${...}` in the same or sibling config files, and are any version literals duplicated across `pom.xml` and CI workflow files [verify]? (Note: config↔Java-runtime semantic links are out of scope for XML — there is no graph edge from a Maven `<artifactId>` to Java usage — so this stays inside the markup/config tree where a cross-cutting query is meaningful.)"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern=".*plugin.*"); D2->N/A (no call/reference graph for build XML — acknowledge, do not invoke trace_call_path); D3->get_code_snippet(qualified_name="pom.xml:<dependencies>"); D4->get_architecture(scope="config/markup tree"); D5->search_code("\\$\\{.*version\\}")/query_graph for duplicated version literals across config files.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/xml-graph.md`, `xml-explorer.md`, `xml-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the xml tier.

---

### 50. markdown — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** github/docs (`/tmp/bench/markdown`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of GitHub's most-starred docs repos (~16k stars), with thousands of substantial, idiomatic Markdown files carrying rich YAML frontmatter and Liquid includes — exactly the "popular + idiomatic + substantial" profile the plan's repo-selection criteria demand for a markup/docs tier.

> **Graph model note (markdown):** The extractor models a Markdown file as a single `document` module node and maps each heading (`atx_heading`/`setext_heading`) to a "class"-labeled Definition node. It does **not** extract YAML frontmatter keys, `children:` lists, or `{% data reusables.* %}` Liquid includes, and markdown has **no CALLS / IMPORTS edges**. Questions and expected-tool hints below are written to reflect that real model — frontmatter/include facts are grep-findable text, not graph nodes.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the section headings (the `#`/`##` ATX headings the indexer captures as Definition nodes) in `content/get-started/start-your-journey/hello-world.md` [verify], e.g. the top-level `# Hello World` title and its sub-section headings." — symmetric: grep-findable via the literal heading lines `# `/`## ` in the file, and graph-findable as heading Definition nodes.
2. **[D2 Relationship]** "N/A for markdown — the code graph extracts no CALLS/IMPORTS edges and does not model `children:` frontmatter or `{% data reusables.* %}` Liquid includes, so there is no symbol-to-symbol relationship to traverse. (Reason: markdown lang-spec defines only a `document` module type and heading class types; no call/import/field types.) The cross-file include graph is answerable only by grep/text traversal, not by the graph, so it is intentionally not posed as a graph question."
3. **[D3 Retrieval]** "Retrieve the contents of `content/get-started/index.md` — the document node for that landing page — and report its leading section (title heading + intro paragraph)." — symmetric: grep-findable by opening the named file, and graph-findable via the `document`/heading node for that path.
4. **[D4 Architecture]** "Describe the directory organization of the `content/` tree: enumerate the top-level product folders (`actions`, `authentication`, `code-security`, `get-started`, `pull-requests`, `repositories`, `rest`, …) and explain the `index.md`-per-folder convention plus the parallel `data/reusables/` tree."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Identify duplicated or near-duplicate prose across `data/reusables/**.md` and `content/**.md` — e.g. reusable snippets whose text is also inlined verbatim in pages instead of referenced via `{% data reusables.* %}`. This is openly graph/semantic-favoring (near-duplicate detection over heading/document nodes + text), is best-effort for markdown (the graph stores headings + document text, not prose-level dedup), and is scored within Group E only."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", project="markdown", name_pattern=".*Hello World.*") [verify exact heading text]; D2->N/A (no CALLS/IMPORTS edges for markdown; use grep for frontmatter `children:`/Liquid includes); D3->get_code_snippet(qualified_name="<document QN for content/get-started/index.md>") [verify QN format]; D4->get_architecture(scope="content/"); D5->search_code(query="reusables inlined duplicate prose") / search_graph(semantic_query="near-duplicate documentation snippets").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/markdown-graph.md`, `markdown-explorer.md`, `markdown-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 records N/A), D5 within Group E only, Group E, the markdown tier.

---

### 51. makefile — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** redis/redis (symlink c) (`/tmp/bench/makefile`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** Redis is a top-tier (60k+ star) C project whose hand-written, multi-level GNU Make build (`Makefile` → `src/Makefile` → `deps/Makefile`) is large and idiomatic, exercising real-world makefile structure rather than a generated stub — matching the plan's "popular + substantial + idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level variable and target definitions declared in `src/Makefile` — e.g. the `STD`, `OPT`, `FINAL_CFLAGS`, `FINAL_LDFLAGS` variable assignments [verify] and the `$(REDIS_SERVER_NAME)` / `redis-server` build targets [verify]. Are these grep-findable identifiers surfaced as graph definition nodes?"
2. **[D2 Relationship]** "How does the root `Makefile` delegate to the sub-build? Identify the cross-file reference where the top-level targets (e.g. `all:`, `install:`) hand off into `src/Makefile` — in redis this is a `cd src && $(MAKE) $@` shell recursion [verify] rather than a `-C` flag, so confirm the exact mechanism. Report whether the graph models this as a relationship edge (e.g. INCLUDES/IMPORTS or a delegation edge) at all, and if not, note that recursive-make handoff may be N/A for a code graph (it is shell recursion, not a code call)."
3. **[D3 Retrieval]** "Retrieve the full recipe block for the `install:` target as defined in `src/Makefile` [verify] (its body and prerequisites), returning only that one definition rather than the whole file."
4. **[D4 Architecture]** "Describe the file/directory organization of the build system: the layering of root `Makefile`, `src/Makefile`, `deps/Makefile`, and the generated `src/Makefile.dep` [verify] — how the three Make units compose and where each lives in the tree."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring.) Find naming-pattern duplication and config↔code links across the makefiles: where the same dependency-toolchain pattern (`$(MAKE) ... -C ../deps` / `-C deps`) recurs [verify], and where build variables like `$(REDIS_SERVER_NAME)` [verify] map to the produced binary names — clusters that text search cannot group by similarity."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern=".*(STD|OPT|FINAL_CFLAGS|REDIS_SERVER_NAME).*"); D2->search_graph(label="Definition", name_pattern=".*(all|install)$") + query_graph for any cross-file/INCLUDES edge into src/Makefile (expect possible N/A — recursive make is shell recursion, not a graph call edge); D3->get_code_snippet(qualified_name="src/Makefile::install"); D4->get_architecture(scope="build"); D5->search_code/query_graph("recursive $(MAKE) -C dependency build pattern").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/makefile-graph.md`, `makefile-explorer.md`, `makefile-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the makefile tier.

---

### 52. cmake — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** kitware/CMake (`/tmp/bench/cmake`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, most-starred CMake-language corpus on GitHub — its `Modules/*.cmake` tree is large, idiomatic, and authored by the language's own maintainers, making it the reference body of `function()`/`macro()`/`include()` build code the plan's repo-selection criteria call for.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level CMake-language definitions provided by `Modules/FindPkgConfig.cmake` — specifically the `function(pkg_check_modules ...)` and `macro(pkg_search_module ...)` public entry points — and report their defining file and line. (grep-findable: the literal tokens `pkg_check_modules` and `pkg_search_module`.)"
2. **[D2 Relationship]** "Show the cross-file include/reference graph for `function(FetchContent_MakeAvailable ...)` in `Modules/FetchContent.cmake`: what helper functions it invokes (e.g. `FetchContent_GetProperties`, `FetchContent_Populate`) and which modules `include()` or call it, traversing both directions."
3. **[D3 Retrieval]** "Retrieve the full body of the `function(ExternalProject_Add ...)` definition in `Modules/ExternalProject.cmake` — its largest single public definition — exactly as written, with no surrounding module boilerplate. (grep-findable: the literal token `ExternalProject_Add` locates the definition site; the value tested is exact-boundary body retrieval, not symbol discovery.)"
4. **[D4 Architecture]** "Describe the organization of the CMake-language module tree: how `Modules/` is partitioned (top-level helpers vs. `Modules/FindXXX.cmake` find-modules vs. `Modules/Platform/`), and where `CMakeLists.txt` build entry points sit relative to `Source/`."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across all `Modules/*.cmake`, find duplicated/near-duplicate argument-parsing blocks that re-implement `cmake_parse_arguments(...)` handling, and flag find-modules that follow the same `find_path` + `find_library` + `find_package_handle_standard_args` naming pattern — surfacing config<->convention links plain grep cannot cluster."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*pkg_(check|search)_module.*", label="Function"); D2->trace_call_path(name="FetchContent_MakeAvailable", direction="both"); D3->get_code_snippet(qualified_name="ExternalProject_Add"); D4->get_architecture(scope="Modules"); D5->search_code/semantic_query("cmake_parse_arguments duplicate argument parsing find_package_handle_standard_args pattern").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/cmake-graph.md`, `cmake-explorer.md`, `cmake-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the cmake tier.

---

### 53. protobuf — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** googleapis/googleapis (`/tmp/bench/protobuf`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Canonical, high-star (~8.6k) Google monorepo of idiomatic, substantial `.proto` API definitions — the de-facto reference corpus for protobuf, matching the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level `message` and `service` definitions declared in `google/longrunning/operations.proto` — at minimum the `Operations` service and the `Operation` message — and report the `.proto` file each is declared in." (grep-symmetric: `message Operation`/`service Operations` are plain-text findable.)
2. **[D2 Relationship]** "Starting from `google/longrunning/operations.proto`, map its cross-file `import` references (e.g. `google/protobuf/any.proto`, `google/rpc/status.proto`, `google/api/annotations.proto`) and show which imported types (`google.protobuf.Any`, `google.rpc.Status`) are actually referenced by fields inside the `Operation` message."
3. **[D3 Retrieval]** "Retrieve the full definition of the `Operation` message from `google/longrunning/operations.proto`, including all its fields (`name`, `metadata`, `done`, and the `result` oneof)." (grep-symmetric: `message Operation {` is plain-text findable.)
4. **[D4 Architecture]** "Describe the directory/package organization under `google/` — how API surfaces are grouped into namespaces such as `google/rpc/`, `google/type/`, `google/api/`, and `google/longrunning/` — and how `package` declarations mirror that directory layout."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find `.proto` definitions semantically similar to a generic error/status carrier — surface duplication and convergent naming across `google.rpc.Status` (`google/rpc/status.proto`), the error-detail messages in `google/rpc/error_details.proto`, and the canonical codes in `google/rpc/code.proto` — and flag the cross-file naming-pattern / type-reference links a plain grep over a single file would miss."

**Expected graph tools (hint, not a script):** D1->search_graph(label="message|service", name_pattern=".*Operation.*"); D2->query_graph IMPORTS edges from `google/longrunning/operations.proto` plus type-reference resolution into the `Operation` message fields (protobuf has IMPORTS + DEFINES edges, not CALLS); D3->get_code_snippet(qualified_name="google.longrunning.Operation"); D4->get_architecture(scope="google/"); D5->search_code/semantic_query("error status detail carrier message").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/protobuf-graph.md`, `protobuf-explorer.md`, `protobuf-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the protobuf tier.

---

### 54. graphql — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** graphql/graphql-js (`/tmp/bench/graphql`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** graphql-js is the reference JavaScript implementation of the GraphQL spec (20k+ stars, the canonical SDL/schema toolkit), giving a large, idiomatic body of schema-definition and type-system code — squarely matching the plan's "popular + substantial + idiomatic" repo-selection criteria for Group E (schema).

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level type-system definition classes exported by the schema layer — e.g. `GraphQLObjectType`, `GraphQLInterfaceType`, `GraphQLScalarType`, `GraphQLEnumType`, and `GraphQLSchema` — and confirm where each is declared in `src/type/definition.ts` / `src/type/schema.ts`."
2. **[D2 Relationship]** "Starting from the `buildSchema` entry point, show the cross-file reference chain into `buildASTSchema` and the `extendSchema`/`extendSchemaImpl` machinery — what depends on it and what it pulls in (direction=both)."
3. **[D3 Retrieval]** "Retrieve the full definition of the `coerceInputValue` function in `src/utilities/coerceInputValue.ts`." [verify]
4. **[D4 Architecture]** "Describe the directory/module organization of the `src/` tree — how the `type/`, `language/`, `execution/`, `validation/`, and `utilities/` subdirectories partition the schema, AST/parser, and execution concerns."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across the validation layer, surface the recurring naming/structure pattern of the `src/validation/rules/*.ts` rule modules (each exporting a `*Rule` validator) and link these rule definitions to where they are aggregated in `specifiedRules` — a duplication/naming-pattern + definition<->aggregation query that plain grep cannot rank structurally."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="GraphQL.*Type|GraphQLSchema", label="Class"); D2->trace_call_path(name="buildSchema", direction="both"); D3->get_code_snippet(qualified_name="coerceInputValue"); D4->get_architecture(); D5->search_code/semantic_query("validation rule *Rule modules aggregated in specifiedRules").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/graphql-graph.md`, `graphql-explorer.md`, `graphql-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the graphql tier.

---

### 55. vue — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** vuejs/core (`/tmp/bench/vue`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** vuejs/core is the canonical, highest-popularity Vue codebase (the framework's own monorepo), giving the broadest authentic sample of `.vue` SFC markup plus the template-AST definitions that drive it — exactly the substantial, idiomatic Group-E artifact the plan's "popular + representative" repo-selection criterion calls for.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level template-AST node-type definitions in the compiler — e.g. the `NodeTypes` enum and node interfaces such as `ElementNode`, `InterpolationNode`, and `DirectiveNode` declared in `packages/compiler-core/src/ast.ts`. Are these top-level definitions enumerated?" (grep-findable: `enum NodeTypes`, `interface ElementNode`.)
2. **[D2 Relationship]** "Starting from the SFC descriptor produced by `parse` in `packages/compiler-sfc/src/parse.ts`, which template/script/style blocks reference (include) which other definitions across files — e.g. how the `<template>` block's compiled output ties back to `compiler-core`? Map the cross-file reference/include edges in both directions."
3. **[D3 Retrieval]** "Retrieve the full definition of the `compileTemplate` function (the public SFC `<template>`-compilation entry point) in `packages/compiler-sfc/src/compileTemplate.ts`. Return its exact body." (grep-findable: `export function compileTemplate`.)
4. **[D4 Architecture]** "Describe the file/directory organization of the template/markup pipeline: how `packages/compiler-core`, `packages/compiler-sfc`, and `packages/compiler-dom` are arranged and how `.vue` SFC blocks flow through them."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find duplication and naming-pattern links between the template-AST `create*` factory helpers (e.g. `createSimpleExpression`, `createCompoundExpression`, `createCallExpression`, `createVNodeCall` [verify]) and the `NodeTypes` enum members they construct — i.e. config<->code consistency between the markup node taxonomy and the code that builds it. This is openly semantic/similarity-driven; plain grep cannot cluster the `create*`/`NodeTypes` correspondence."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="NodeTypes|ElementNode|DirectiveNode", label="Definition"); D2->trace_path(direction=both, from="parse") plus cross-file reference/IMPORTS edges from `compiler-sfc` into `compiler-core`; D3->get_code_snippet(qualified_name="compileTemplate"); D4->get_architecture(scope="packages/compiler-*"); D5->search_code/semantic_query("template node factory helpers vs NodeTypes enum members").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/vue-graph.md`, `vue-explorer.md`, `vue-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the vue tier.

---

### 56. svelte — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** sveltejs/svelte (`/tmp/bench/svelte`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ~80k-star reference UI compiler; the canonical, substantial `.svelte` markup/template + compiler corpus, matching the plan's "popular + idiomatic + large" repo-selection criteria for Group E markup.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level exported definitions of the public compiler API in `packages/svelte/src/compiler/index.js` — specifically `compile`, `compileModule`, `parse`, and `preprocess`. Are all four surfaced as graph nodes with their qualified names?" (Symmetric: each is an `export function ...` line, plainly grep-findable as well as graph-findable.)
2. **[D2 Relationship]** "Starting from the `compile` entry point, trace the relationship (direction=both) through the compiler phases — does it reach the phase functions in `packages/svelte/src/compiler/phases/` (e.g. `parse`/`analyze`/`transform`), and what calls `compile` in return? (Inbound callers of a public entry point may be sparse — tests/internal only — which is itself a valid signal.)"
3. **[D3 Retrieval]** "Retrieve the full definition of the markup parser entry `parse` in `packages/svelte/src/compiler/phases/1-parse/index.js` [verify] — return the exact function/class body, not the whole file." (Symmetric: the symbol name and file are grep-findable; the graph advantage is exact boundary retrieval.)
4. **[D4 Architecture]** "Describe the file/directory organization of `packages/svelte/src/compiler/` — how are the numbered phase directories (`1-parse`, `2-analyze`, `3-transform`) and the shared `state.js`/`errors.js` arranged relative to `src/internal/`?"
5. **[D5 Cross-cutting/Semantic]** "[GRAPH-FAVORING] Across the template/runtime split, surface naming-pattern and config↔code links: which client vs. server runtime modules under `src/internal/client/` and `src/internal/server/` share parallel names (e.g. `render`, `hydrate`), and how does `package.json`'s `exports` map (`svelte`, `svelte/compiler`, `svelte/internal/client`) tie to those directories? Semantic/similarity query — not answerable by a single grep."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="compile|compileModule|parse|preprocess", path~"src/compiler/index"); D2->trace_call_path(name="compile", direction="both"); D3->get_code_snippet(qualified_name="...phases/1-parse...parse"); D4->get_architecture(path="packages/svelte/src/compiler"); D5->search_code + query_graph ("client/server runtime parity + exports map").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/svelte-graph.md`, `svelte-explorer.md`, `svelte-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the svelte tier.

---

### 57. meson — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** mesonbuild/meson (`/tmp/bench/meson`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The reference implementation of the Meson build system — the single most idiomatic and substantial corpus of Meson DSL on GitHub (its own `meson.build` plus thousands of `.build` files under `test cases/`), satisfying the plan's "popular + idiomatic + substantial" repo-selection criterion for the build-language slot of Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level build definitions invoked in the repository-root `meson.build` — specifically the `project()` declaration and the build targets/installers it sets up (e.g. `executable(...)` [verify], `install_data(...)` [verify], `configure_file(...)` [verify]). These are literal grep-findable function calls in the DSL, so a plain grep for the call names must surface the same set."
2. **[D2 Relationship]** "Starting from the root `meson.build`, trace the cross-file include graph formed by `subdir(...)` calls (e.g. into `man/`, `data/` [verify], `test cases/`) — which child `meson.build` files are pulled in, and which of those recurse further via their own `subdir(...)`?"
3. **[D3 Retrieval]** "Retrieve the full `project(...)` invocation at the top of the repository-root `meson.build`, including its positional name argument (`'meson'`) and the language positional/keyword arguments and keywords such as `version`, `license` [verify], `meson_version`, and `default_options` [verify]. The `project(...)` call name is grep-findable, so a plain grep on `meson.build` must locate the same block."
4. **[D4 Architecture]** "Describe how Meson build configuration is organized across the repo: the root `meson.build`, the options file (`meson.options`, historically `meson_options.txt`), and the per-directory `meson.build` files reached through `subdir(...)` — i.e. the directory/file layout of the build description."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across all `meson.build` files in `test cases/`, surface duplicated or near-duplicate target patterns — e.g. repeated `executable('prog', 'prog.c')` idioms and recurring `dependency('...')` names — and link option names declared in `meson.options` [verify] to the `get_option('...')` lookups that consume them (config<->code). This relies on similarity/cross-file linking grep cannot easily do."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern="project|executable|install_data|configure_file"); D2->trace_path(direction="both", from="root meson.build subdir edges"); D3->get_code_snippet(qualified_name="meson.build::project"); D4->get_architecture(scope="build-config layout"); D5->search_code/query_graph("duplicate target idioms; get_option<->meson.options links").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/meson-graph.md`, `meson-explorer.md`, `meson-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the meson tier.

---

### 58. glsl — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** repalash/Open-Shaders (`/tmp/bench/glsl`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** A widely-referenced, well-starred curated collection of idiomatic GLSL shader snippets/includes (noise, color-space, lighting, SDF, math utilities) — substantial breadth of real `.glsl`/`.frag`/`.vert` definitions and glslify-style `#include` relationships, matching the plan's criterion of "popular + idiomatic + structurally substantial" for the shader/HDL slice of Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level GLSL function definitions whose name matches the noise family (e.g. `snoise`, `cnoise`, `pnoise`, `noise`) across the collection — these are grep-findable via the `float snoise(` / `float cnoise(` signatures." (grep-symmetric: the noise-family signatures are plain-text findable.)
2. **[D2 Relationship]** "Starting from the simplex-noise entry point `snoise`, what helper functions does it reference (e.g. `mod289`, `permute`, `taylorInvSqrt`) and which other shader files reference `snoise` in turn? Show the reference graph in both directions."
3. **[D3 Retrieval]** "Retrieve the full definition of the simplex-noise function `snoise` (the `vec3`-input 3D variant) [verify] — return only that function body, not the whole file." (grep-symmetric: the `snoise(vec3` signature is plain-text findable; confirm the 3D variant exists in this repo.)
4. **[D4 Architecture]** "Describe the directory/category organization of the collection — how shader includes are grouped (e.g. Noise, Color, Lighting, SDF, Math [verify]) into folders, and how many top-level definition files sit under each."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Find duplicated / near-duplicate helper definitions across files (the classic `mod289`, `permute`, `taylorInvSqrt`, `rand`/`random` hash helpers re-declared per snippet), and cluster the snippets by semantic purpose (e.g. all hash/PRNG helpers vs. all gradient-noise generators). This is openly graph/semantic-favoring: it relies on similarity + cross-file naming-pattern detection that plain grep cannot cluster."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*noise.*|snoise|cnoise|pnoise"); D2->trace_call_path(name="snoise", direction="both"); D3->get_code_snippet(qualified_name=".*snoise.*vec3.*"); D4->get_architecture(); D5->search_code/semantic_query("hash PRNG helper mod289 permute duplicate") + search_graph(name_pattern="mod289|permute|taylorInvSqrt|rand").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/glsl-graph.md`, `glsl-explorer.md`, `glsl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the glsl tier.

---

### 59. ini — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** httpie/cli (symlink python) (`/tmp/bench/ini`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** httpie/cli is a ~35k-star, widely-used Python CLI HTTP client whose packaging/test config is idiomatic, substantial INI (a multi-section `setup.cfg` with `[metadata]`/`[options]`/`[options.extras_require]`/`[tool:pytest]`/`[flake8]`/`[options.entry_points]` plus a separate `pytest.ini`), satisfying the plan's "popular + idiomatic + non-trivial size" repo-selection criterion for the ini tier. The symlink reuses the already-cloned python repo.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level INI sections defined in `setup.cfg` — at minimum confirm `[metadata]`, `[options]`, and `[options.extras_require]` are recognized as distinct section definitions." (grep-findable: literal `[metadata]` / `[options]` / `[options.extras_require]` header lines, so plain grep can recover them too.)
2. **[D2 Relationship]** "Show the cross-section / cross-file reference structure of the packaging+test config: how `[options]` keys (`install_requires`, `python_requires`) relate to the extras under `[options.extras_require]` (`dev`, `test`), and how the test configuration is split between `setup.cfg`'s `[tool:pytest]` (testpaths/addopts) and the separate `pytest.ini`'s `[pytest]` (markers). (Structural cross-reference framing; INI has no call graph, so 'relationship' is the section→section/file→file reference web.)"
3. **[D3 Retrieval]** "Retrieve the full body of the `[options]` section in `setup.cfg` verbatim, including all of its keys (`packages`, `install_requires`, `python_requires`)." (grep-findable: the `[options]` header anchors the block; a grepper can read to the next `[` header.)
4. **[D4 Architecture]** "Describe the configuration-file organization of the repo: which INI/CFG files exist (`setup.cfg`, `pytest.ini`, and the `setup.py` shim [verify]) and how config responsibilities are split — packaging/metadata (`[metadata]`,`[options]`,`[options.entry_points]`,`[options.data_files]`), test config (`[tool:pytest]` in setup.cfg + `[pytest]` markers in pytest.ini), and lint (`[flake8]` living inside setup.cfg rather than a standalone `.flake8`)."
5. **[D5 Cross-cutting/Semantic]** "*(Graph-favoring)* Identify duplication / split-config overlap and config→code links: the pytest configuration is spread across two files (`setup.cfg` `[tool:pytest]` vs `pytest.ini` `[pytest]`) — surface that overlap — and trace where a config key names a real code artifact, e.g. `[options.entry_points] console_scripts` and `[options.packages.find] include` pointing at the actual `httpie` package directory. Plain grep can find the literal headers but cannot resolve the config↔code mapping or recognize the two pytest blocks as one logically-split concern; flagged graph-favoring. (D2/D5 do legitimately apply here via the section-reference web and config→code edges, so no forced N/A is needed.)"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Section"|name_pattern=".*\\[.*\\].*", project="ini"); D2->trace_call_path(direction="both") over section/file reference edges (fallback: query_graph on CONTAINS_FILE / reference edges since INI has no call graph); D3->get_code_snippet(qualified_name="setup.cfg::[options]" [verify]); D4->get_architecture(project="ini"); D5->search_code/semantic_query for the split pytest config + config→code (entry_points/packages.find → `httpie/`) links.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/ini-graph.md`, `ini-explorer.md`, `ini-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the ini tier.

---

### 60. matlab — B (Systems & Low-level)

**Repo:** chebfun/chebfun (`/tmp/bench/matlab`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred, most-cited MATLAB projects (numerical computing with Chebyshev technology); large, idiomatic class-based MATLAB codebase (`@chebfun/`, `@chebtech2/`, …) with many `.m` method files — matches the plan's "popular + substantial + idiomatic" repo-selection criteria for the matlab tier.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the public method `roots` and the `sum` method on the `chebfun` class (files `@chebfun/roots.m` and `@chebfun/sum.m`). What is each function's signature?" — both are well-known, grep-findable identifiers (`function ... = roots(`, `function ... = sum(`).
2. **[D2 Relationship]** "For the `chebfun` constructor (`@chebfun/chebfun.m`), show callers and callees in both directions: what does it call (e.g. the parsing/population helpers such as `parseInputs` [verify] and the underlying `populate` [verify]) and what builds chebfuns by calling it?"
3. **[D3 Retrieval]** "Retrieve the full source of the `compose` method on the `chebfun` class (`@chebfun/compose.m`) exactly as defined — one named symbol, body and signature." — single named symbol, grep-findable file (`@chebfun/compose.m`).
4. **[D4 Architecture]** "Describe the class-folder architecture: how are the technology classes organized (`@chebtech1`, `@chebtech2`, `@trigtech`, `@bndfun`, `@unbndfun`, `@classicfun`, `@fun`, `@deltafun`) and how do they relate to the top-level `@chebfun` class and shared utilities (e.g. `chebpts.m`, `chebpoly.m`)?"
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING) Across the @-class folders, find methods that implement the *same numerical operation* under different names or duplicated logic — e.g. quadrature/integration (`sum`, `cumsum`), differentiation (`diff`), and evaluation (`feval`) reimplemented per technology class. Surface near-duplicate method clusters and naming-pattern overlaps that plain text search would miss."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="roots|sum", label="Method"); D2->trace_path(function_name="chebfun", mode="calls", direction="both"); D3->get_code_snippet(qualified_name="@chebfun/compose"); D4->get_architecture(); D5->search_graph(semantic_query=["sum","cumsum","diff","feval","per-class duplicate numerical op"]) / search_code.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/matlab-graph.md`, `matlab-explorer.md`, `matlab-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the matlab tier.

---

### 61. lean — D (Functional & Formal)

**Repo:** leanprover-community/mathlib4 (`/tmp/bench/lean`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** mathlib4 is the canonical, community-scale Lean 4 library (tens of thousands of files, >1M lines of proofs/definitions) and the de-facto reference for idiomatic Lean, satisfying the plan's "popular + substantial + idiomatic" repo-selection criteria for the functional/formal tier.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the typeclass `Group` and the predicate `Continuous` (both grep-findable as `class Group` and `def Continuous`). Does the graph surface their declaring files under `Mathlib/Algebra/Group/Defs.lean` and `Mathlib/Topology/Continuous.lean` (or `Mathlib/Topology/Defs/`)? [verify]"
2. **[D2 Relationship]** "For the lemma `Continuous.comp` [verify], trace its relationships in both directions: which lemmas/defs it depends on (e.g. `Continuous`, `Function.comp`) and which downstream lemmas invoke it. Does the call/dependency graph reconstruct the proof-term usage chain?"
3. **[D3 Retrieval]** "Retrieve the full source of the single declaration `Nat.factorial` (grep-findable as `def factorial` / `Nat.factorial`; defined in mathlib4 at `Mathlib/Data/Nat/Factorial/Basic.lean`, NOT in Lean core). Return only that one recursive declaration's body with correct line boundaries. [verify]"
4. **[D4 Architecture]** "Describe the top-level module organization of `Mathlib/`: how the major namespaces (`Algebra`, `Topology`, `Analysis`, `CategoryTheory`, `Order`, `Data`) map onto the directory tree, and which subtrees are largest. Does `get_architecture` reflect the import-layered structure?"
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Semantically locate declarations implementing 'a continuous map between topological spaces preserves a limit/filter' across files — i.e. find the `Filter.Tendsto`/`Continuous` family of lemmas by concept rather than exact name. Surface near-duplicate or naming-convention-sibling lemmas (`*.comp`, `*.continuousAt`) that grep alone would miss. [verify]"

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Group|Continuous", label=Definition); D2->trace_call_path(name="Continuous.comp", direction=both); D3->get_code_snippet(qualified_name="Nat.factorial"); D4->get_architecture(root="Mathlib"); D5->search_code/semantic_query("continuous map preserves limit / Tendsto").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/lean-graph.md`, `lean-explorer.md`, `lean-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the lean tier.

---

### 62. form — D (Functional & Formal)

**Repo:** vermaseren/form (`/tmp/bench/form`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** FORM is the reference open-source symbolic-manipulation engine for high-energy-physics computation (long-lived, widely cited, ~hundreds of stars); its substantial hand-written C core in `sources/` plus idiomatic `.frm` example scripts make it the canonical, idiomatic-yet-large repo for the FORM tier under the plan's "popular + substantial + idiomatic" selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the FORM module-processing driver function `Processor` and the dollar-variable handler `CatchDollar` in `sources/proces.c`/`sources/dollar.c`; both are plain identifiers a grep over `sources/*.c` would also surface." [verify]
2. **[D2 Relationship]** "Show the full caller/callee neighborhood of `Processor` (direction=both): which compiler/preprocessor entry points reach it inbound and which term-manipulation routines (e.g. `Generator`, `TakeNormalForm` [verify]) it calls outbound."
3. **[D3 Retrieval]** "Retrieve the complete body of the expression-generator function `Generator` (in `sources/proces.c` [verify]) — one named symbol, exact line range; a plain `grep -n 'Generator'` over `sources/*.c` would also surface its definition site."
4. **[D4 Architecture]** "Describe how the FORM C core is organized: the `sources/` directory split across the compiler (`compiler.c`), preprocessor (`pre.c`), processor (`proces.c`), the central `declare.h` declarations header, and how the `.frm` example/test scripts relate to that source tree."
5. **[D5 Cross-cutting/Semantic]** "GRAPH-FAVORING: find functions semantically related to 'allocate and grow a term/expression buffer' (e.g. the `TermMalloc`/`Malloc1` [verify] family) and surface near-duplicate buffer-management helpers across `sources/` that share that naming/behavior pattern — a similarity/semantic ranking plain grep cannot produce."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Processor|CatchDollar"); D2->trace_call_path(function_name="Processor", direction="both"); D3->get_code_snippet(qualified_name="Generator"); D4->get_architecture(); D5->search_code(pattern="grow term/expression buffer allocation") for graph-ranked semantic matches.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/form-graph.md`, `form-explorer.md`, `form-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the form tier.

---

### 63. magma — D (Functional & Formal)

**Repo:** defeo/ss-isogeny-software (`/tmp/bench/magma`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Cited as an idiomatic, substantial isogeny-crypto reference by L. De Feo; HOWEVER validation (see below) shows it contains NO Magma source — the magma↔repo pairing is INVALID and must be re-sourced before this tier runs.

> **VALIDATION FAILURE — pairing is unusable for the magma tier.**
> Per the repo note ("rare; validate") and the GROUND-TRUTH RULE, the pinned repo was checked against GitHub's language API and file listing. Findings:
> - GitHub language bytes: **C 29,323 / Python 26,710** — zero Magma.
> - Top-level files: `LICENSE`, `README.md`, `gfp2.c`, `gfp2.pxd`, `paths.py`, `pqcrypto11.sage`, `pqcrypto11.spyx`. **No `.m` (Magma) files exist.**
> - The real symbols (`ss_isogeny_gen`, `ss_isogeny_exchange`, `keygen`, `keygen_c`, `scramble2`, `MontgomeryCurve`, `MontgomeryIsogeny`, `MontgomeryTwoIsogeny`, `MontgomeryFourIsogeny`, `MontgomeryCurve_from_j`) are **Sage/Cython (.spyx/.sage)**, not Magma.
> Consequently the magma parser/grammar has nothing to ingest here: indexing yields an empty magma slice. The five questions below are therefore each **N/A** and **excluded from every mean**. ACTION FOR EXECUTION: substitute a genuinely Magma-bearing repo (e.g. a `.m`-heavy isogeny/number-theory codebase) before this tier is scored. This chapter is retained as a documented negative result, not as a scored unit.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "N/A — repo contains no Magma definitions (`intrinsic`/`function`/`procedure`); the cited symbols (`ss_isogeny_gen`, `ss_isogeny_exchange`) are Sage. Excluded from the D1 mean."
2. **[D2 Relationship]** "N/A — no Magma call graph can exist with zero `.m` files; cross-symbol edges among the Sage classes (`MontgomeryIsogeny` → `MontgomeryCurve`) are out of language scope for the magma tier. Excluded from the D2 mean."
3. **[D3 Retrieval]** "N/A — there is no Magma symbol to retrieve; the largest real definition (`keygen` in `pqcrypto11.spyx`) is Cython, not Magma. Excluded from the D3 mean."
4. **[D4 Architecture]** "N/A — file/dir architecture (`gfp2.c` + `pqcrypto11.spyx` + `paths.py`) is a C/Sage layout with no Magma module organization to report. Excluded from the D4 mean."
5. **[D5 Cross-cutting/Semantic]** "N/A (graph-favoring) — no Magma corpus to support semantic/duplication queries; cross-language config↔code links would not exercise the magma path. Excluded from the D5 (Group D only) mean."

**Expected graph tools (hint, not a script):** D1→search_graph(...); D2→trace_call_path(direction=both, ...); D3→get_code_snippet(...); D4→get_architecture(...); D5→search_code/semantic_query(...). NOTE: all are inapplicable until a Magma-bearing repo is substituted; no tool invocation is expected to return magma nodes for this repo.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/magma-graph.md`, `magma-explorer.md`, `magma-judged.json` — each must record the validation failure verbatim and emit empty/`N/A` result sets so the negative result is auditable.

**Aggregates into:** D1–D4 cross-group rollups, D5 within Group D only, Group D, the magma tier — but ONLY as an explicit exclusion (all five dimensions N/A). The magma tier MUST NOT be scored on this repo; re-source a `.m`-bearing repository first, then re-run this chapter against the new pairing.

---

### 64. wolfram — D (Functional & Formal)

**Repo:** WolframResearch/WolframLanguageForJupyter (`/tmp/bench/wolfram`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The official Wolfram-maintained Jupyter kernel for the Wolfram Language — the canonical, substantial, idiomatic `.wl`/`.m` body of Wolfram code on GitHub, satisfying the plan's "popular + idiomatic + non-trivial" repo-selection criteria for a functional/formal language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Where is the kernel's main read-eval-print routine `WolframLanguageForJupyter\`Private\`loop` defined, in which resource file, and what other top-level functions/symbols does that kernel session file declare?"
2. **[D2 Relationship]** "Starting from the main read-eval-print `loop[]`, what helper functions does it call (e.g. the frame/message helpers `getFrameAssoc` [verify] / `createReplyFrame` [verify] and the output handlers) and what calls into it — show callers and callees in both directions."
3. **[D3 Retrieval]** "Show the full definition of the output-formatting helper `toOutTextHTML` (the routine in `OutputHandlingUtilities.wl` that builds the HTML textual output presentation returned to the Jupyter frontend)."
4. **[D4 Architecture]** "Give the structure of the `WolframLanguageForJupyter/Resources/` package directory — how the `.wl` resource files (e.g. `KernelForWolframLanguageForJupyter.wl`, `EvaluationUtilities.wl`, `SocketUtilities.wl`, `OutputHandlingUtilities.wl`, `Initialization.wl`) are organized and loaded relative to the install/launch scripts."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the symbols responsible for serializing kernel results into ZMQ/Jupyter wire messages — i.e. functions semantically about 'encode result as JSON/MIME message and send on socket' — across the resource files, even where naming differs (e.g. `sendFrame` [verify], `socketWriteFunction` [verify], or the `redirectMessages` [verify] error/output-redirect path)."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*loop.*", label="Function"); D2->trace_call_path(qualified_name="...loop", direction="both"); D3->get_code_snippet(qualified_name="...toOutTextHTML"); D4->get_architecture(scope="WolframLanguageForJupyter/Resources"); D5->search_code/semantic_query("encode kernel result as MIME/JSON and write to ZMQ socket").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/wolfram-graph.md`, `wolfram-explorer.md`, `wolfram-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the wolfram tier.

---

### 65. solidity — A (Class-based OOP & Contracts)

**Repo:** OpenZeppelin/openzeppelin-contracts (`/tmp/bench/solidity`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The de-facto standard library for smart contracts (40k+ stars, audited, widely forked); its inheritance-heavy, idiomatic Solidity (abstract contracts, interfaces, libraries, modifiers) is a substantial and representative class-based OOP target for Group A.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the `ERC20` contract and its public/external functions such as `transfer`, `transferFrom`, `approve`, and `allowance`. Where is `ERC20` declared, and what interface (`IERC20`) does it implement?"
2. **[D2 Relationship]** "Map the relationships around `ERC20._update` (the internal balance-mutation hook): which functions call it (e.g. `_transfer`, `_mint`, `_burn`) and what does it call in turn? Show callers and callees in both directions." [verify: `_update` is the post-v5 hook; older tags use `_beforeTokenTransfer`/`_afterTokenTransfer`]
3. **[D3 Retrieval]** "Retrieve the full source of the `SafeERC20.safeTransfer` function from `contracts/token/ERC20/utils/SafeERC20.sol`, including its body and any internal helper it delegates to."
4. **[D4 Architecture]** "Describe the directory/module organization of the library: how are `token/` (ERC20, ERC721, ERC1155), `access/` (Ownable, AccessControl), `utils/`, and `governance/` arranged, and how do the inheritance chains span these folders?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Surface the cross-cutting reentrancy-and-authorization protection pattern: locate contracts/modifiers that combine `nonReentrant` (from `ReentrancyGuard`) with access modifiers like `onlyOwner`/`onlyRole`, and find semantically similar guard usages across `token/` and `governance/`. This favors semantic/similarity search over plain grep because the relevant code shares an intent (guarded state mutation) rather than a single literal token."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="ERC20", label="Class/Contract"); D2->trace_call_path(qualified_name="ERC20._update", direction="both"); D3->get_code_snippet(qualified_name="SafeERC20.safeTransfer"); D4->get_architecture(scope="contracts/"); D5->search_code/semantic_query("nonReentrant guarded privileged state mutation").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/solidity-graph.md`, `solidity-explorer.md`, `solidity-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the solidity tier.

---

### 66. typst — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** typst/packages (`/tmp/bench/typst`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical Typst package registry — thousands of idiomatic `typst.toml` manifests plus `.typ` markup/template sources; large, popular, and the de-facto standard for the Typst ecosystem, matching the plan's "substantial, idiomatic, widely-used" repo-selection criteria for Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level `[package]` manifest definitions across the registry — specifically every `typst.toml` declaring the required `name`, `version`, and `entrypoint` keys — and surface the `[template]` and `[tool.*]` tables where present." (grep-findable literal keys: `entrypoint`, `[package]`, `[template]`.)
2. **[D2 Relationship]** "Within the `cetz` package [verify], starting from its manifest `entrypoint` (`src/lib.typ` [verify]), trace the intra-package `#import`/`#include` chain to the local modules it pulls in (e.g. `draw.typ`, `tree.typ`, plotting sources under `src/`). (graph-favoring extension, [verify] — likely unresolved in a *fast* index of isolated package dirs: in reverse, which other preview packages import `@preview/cetz`.) Note for Group E: cross-*registry* import edges generally do not exist in a fast index because each `packages/preview/<name>/<version>/` dir is indexed in isolation; the intra-package chain is the symmetric, both-tools-answerable part."
3. **[D3 Retrieval]** "Retrieve the full `typst.toml` manifest for the `cetz` package (e.g. version `0.3.4` [verify] — pick the most-populated manifest, with `categories = [\"visualization\"]`, `keywords = [\"draw\", \"canvas\", \"tree\"]`, multi-author `authors`, and an `exclude` list) — exactly as written." [verify]
4. **[D4 Architecture]** "Describe the registry's file/directory organization: the `packages/preview/<name>/<version>/` layout, where the repo-root `bundler/` and `docs/` directories sit relative to package sources, and how version directories nest under each package name."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find naming/structural duplication and config↔source links across the registry: manifests sharing the same `entrypoint` convention (`src/lib.typ` vs root `lib.typ`), packages reusing identical `categories`/`keywords` value sets, and manifest `entrypoint` values that correctly resolve to an existing `.typ` source file vs dangling ones."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern="package|entrypoint|template"); D2->trace_path(name="cetz/src/lib.typ", direction="both") for the intra-package chain, falling back to search_code for cross-registry `@preview/cetz` references; D3->get_code_snippet(qualified_name="packages/preview/cetz/0.3.4/typst.toml"); D4->get_architecture(scope="packages/preview"); D5->search_code/query_graph("entrypoint convention + shared categories/keywords; config↔source resolution").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/typst-graph.md`, `typst-explorer.md`, `typst-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the typst tier.

---

### 67. gdscript — C (Dynamic & Scripting)

**Repo:** godotengine/godot-demo-projects (`/tmp/bench/gdscript`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The Godot engine's official demo collection — high-popularity, broad, and idiomatic GDScript (lifecycle callbacks, signals, `extends` inheritance) across dozens of self-contained demos, matching the plan's "popular + substantial + idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List every GDScript definition of the physics-step callback `_physics_process` across the demos, and identify the scripts that also define the `_ready` initialization callback." (Both are well-known, grep-findable identifiers via `func _physics_process` / `func _ready`.)
2. **[D2 Relationship]** "For the `_physics_process` function in the 2D platformer player script (`2d/platformer/player/player.gd`), show its inbound and outbound relationships — what engine/lifecycle entry drives it and which helper functions/methods it calls (e.g. the `move_and_slide` move routine and the `try_jump` jump helper; note input is read inline rather than via a dedicated `get_input` helper [verify])."
3. **[D3 Retrieval]** "Retrieve the full source of the `_physics_process` function defined in the platformer player script (`2d/platformer/player/player.gd`)." (Single named, grep-findable symbol: `func _physics_process`.)
4. **[D4 Architecture]** "Describe the top-level file/directory organization of the repo — how demos are grouped into per-feature project folders (e.g. `2d/`, `3d/`, `networking/`, `gui/`) and where each demo's entry scripts and `project.godot` live."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring / semantic) Across all demos, find scripts whose movement/input handling is structurally near-duplicate — e.g. multiple player controllers that read directional input and call a slide/move routine inside `_physics_process` — to surface copy-paste patterns and naming conventions for the velocity/`speed` fields. Plain grep cannot cluster these by structural similarity."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="_physics_process|_ready"); D2->trace_call_path(qualified_name="...2d/platformer/player/player.gd::_physics_process", direction="both"); D3->get_code_snippet(qualified_name="...2d/platformer/player/player.gd::_physics_process"); D4->get_architecture(...); D5->search_code/semantic_query("input-driven movement inside physics callback").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/gdscript-graph.md`, `gdscript-explorer.md`, `gdscript-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the gdscript tier.

---

### 68. gleam — D (Functional & Formal)

**Repo:** gleam-lang/stdlib (`/tmp/bench/gleam`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The official Gleam standard library — the most-starred, canonical Gleam codebase; idiomatic, substantial, pure-functional, and exercising the language's module/type system, matching the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "In `src/gleam/list.gleam`, find the public list-folding API: the definitions of `fold`, `fold_right`, `fold_until`, and `try_fold`. Confirm each is a `pub fn` and report its arity/signature."
2. **[D2 Relationship]** "Map the relationships of `result.try` (`src/gleam/result.gleam`): who within stdlib calls `try`, and what does `try` itself call (direction=both)? Use this to gauge whether the graph recovers the monadic-chaining call sites across modules."
3. **[D3 Retrieval]** "Retrieve the exact source of the single symbol `gleam/list.flat_map` — its full body, not the surrounding file."
4. **[D4 Architecture]** "Describe the module/file organization of `src/gleam/`: the flat set of core modules (`list`, `result`, `option`, `dict`, `string`, `int`, `float`, `order`, …) and the nested `gleam/dynamic/` subpackage containing `decode.gleam`. Does the structure view surface the `dynamic/` subdirectory as a distinct unit?"
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Across stdlib, locate the recurring fallible-combinator naming/shape pattern — the `try_*` family (`list.try_map`, `list.try_fold`, `list.try_each`, `result.try`) plus the Result/Option aggregators `result.all` and `result.flatten` (and their `list.flatten` counterpart). Surface this cross-module convention via semantic/similarity search rather than a single text match."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="fold.*|try_fold", label="Function", project="gleam"); D2->trace_call_path(qualified_name="gleam/result.try", direction="both"); D3->get_code_snippet(qualified_name="gleam/list.flat_map"); D4->get_architecture(project="gleam"); D5->search_code/semantic_query("fallible combinator try_ prefix; result all/flatten aggregation").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/gleam-graph.md`, `gleam-explorer.md`, `gleam-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the gleam tier.

---

### 69. powershell — C (Dynamic & Scripting)

**Repo:** PowerShell/PowerShell (`/tmp/bench/powershell`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** PowerShell is the canonical, high-star shell+language project whose own build/test tooling (`build.psm1`) and shipped script modules are large, idiomatic .ps1/.psm1/.psd1 code — exactly the "popular + substantial in-language" criterion the plan uses for Group C.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the PowerShell function definitions exposed by the repo's build module `build.psm1` — e.g. `Start-PSBuild`, `Start-PSPester`, `New-PSOptions`, and `Find-Dotnet` — and confirm where each is declared." (All four are well-known `function`-keyword definitions in `build.psm1`, grep-findable as `function Start-PSBuild` etc.)
2. **[D2 Relationship]** "Show the call relationships around `Start-PSBuild` in `build.psm1`: which helper functions it invokes (e.g. `New-PSOptions`, `Find-Dotnet`, `Restore-PSPackage`) and which other functions invoke it."
3. **[D3 Retrieval]** "Retrieve the full source of the `Start-PSPester` function from `build.psm1`." (Single, real, grep-findable symbol via `function Start-PSPester`.)
4. **[D4 Architecture]** "Describe the repo's module organization: how `src/Modules/` is split into platform subtrees — `Shared/` (e.g. `Microsoft.PowerShell.Host/Microsoft.PowerShell.Host.psd1`), `Unix/` and `Windows/` (each shipping platform-specific manifests such as `Microsoft.PowerShell.Utility/Microsoft.PowerShell.Utility.psd1`) — relates to the root `build.psm1` tooling module and the Pester `*.Tests.ps1` suites under `test/powershell/`."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Find functions across the script modules whose behavior is semantically 'locate or validate the dotnet/SDK toolchain' (e.g. `Find-Dotnet`, `Install-Dotnet`, `Get-LatestBuiltDotnet` [verify]) even when their names differ, and link each `*.psd1` manifest to the `*.psm1`/binary it declares via `RootModule`/`NestedModules`. Labeled graph-favoring: relies on semantic similarity + manifest→module config-to-code linking that plain grep cannot rank."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Start-PS.*|New-PSOptions|Find-Dotnet", label="Function"); D2->trace_call_path(qualified_name="Start-PSBuild", direction="both"); D3->get_code_snippet(qualified_name="Start-PSPester"); D4->get_architecture(scope="src/Modules,build.psm1,test/powershell"); D5->search_code(semantic_query="locate or validate the dotnet SDK toolchain") / search_graph(semantic_query=...).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/powershell-graph.md`, `powershell-explorer.md`, `powershell-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the powershell tier.

---

### 70. pascal — B (Systems & Low-level)

**Repo:** castle-engine/castle-engine (`/tmp/bench/pascal`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Castle Game Engine is the most prominent open-source Object Pascal codebase (multi-thousand-star, actively maintained 3D/2D game engine); it is large, idiomatic FreePascal/Lazarus, with deep class hierarchies and unit cross-references — matching the plan's criterion of a substantial, popular, idiomatic repo per language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the class `TCastleScene` and list the public methods/properties it declares directly (e.g. `PrepareResources`, `GLContextClose`, `FreeResources`, `Clone`). Both grep and the graph should locate the `TCastleScene = class(TCastleSceneCore)` declaration in `src/scene/castlescene.pas` and the members declared in that class body. (Animation/loading API such as `Load` / `PlayAnimation` is inherited from `TCastleSceneCore`, not declared here — do not credit those to `TCastleScene`.)"
2. **[D2 Relationship]** "Show the relationship between `TCastleViewport` and the rendering/transform pipeline: what does `TCastleViewport.Render` [verify] call, and which types reference `TCastleViewport` (e.g. `TCastleWindow` / `TCastleControl`)? Trace callers and callees both directions."
3. **[D3 Retrieval]** "Retrieve the full source of the method `TCastleTransform.LocalRender` [verify] (a single named, `virtual` method on `TCastleTransform`; the unit is `CastleTransform`, with the class body in the include file `src/transform/castletransform_transform.inc`). A plain grep for `LocalRender` across the unit's includes should also find it."
4. **[D4 Architecture]** "Describe the high-level architecture of the `src/` tree — how the engine is split into unit groups such as `src/base` (e.g. `CastleVectors`, `CastleClassUtils`), `src/scene`, `src/transform`, and `src/window`/`src/ui` — and how these layers depend on one another."
5. **[D5 Cross-cutting/Semantic] (graph-favoring)** "Semantic/cross-cutting: locate all code paths that perform OpenGL/rendering resource setup and teardown (`GLContextOpen` / `GLContextClose` style lifecycle methods) across scene, viewport and UI units, and group conceptually-similar 'prepare GPU resources' routines even when names differ. This is openly graph-favoring (vocabulary-bridging + cross-file clustering that grep cannot do)."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*TCastleScene.*", label="Class"); D2->trace_call_path(qualified_name="...TCastleViewport.Render", direction="both"); D3->get_code_snippet(qualified_name="CastleTransform.TCastleTransform.LocalRender"); D4->get_architecture(scope="src/"); D5->search_code/semantic_query(["GLContextOpen","prepare","resources","render"]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/pascal-graph.md`, `pascal-explorer.md`, `pascal-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the pascal tier.

---

### 71. dlang — B (Systems & Low-level)

**Repo:** dlang/phobos (`/tmp/bench/dlang`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Phobos is the official D standard library — the canonical, large, idiomatic D codebase (heavy on templates, ranges, CTFE, and `version`/`static if` metaprogramming), making it the most representative real-world stress test of D extraction for the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the definitions of the conversion API in `std.conv` — specifically the `to` template, `parse`, `text`, `octal`, and the `ConvException` / `ConvOverflowException` classes. Are their declaration sites and kinds (template/function/class) correctly identified?"
2. **[D2 Relationship]** "For the `to` template in `std.conv`, show the call relationships in both directions: what internal helpers (e.g. `toImpl`, `parse`) does it dispatch to, and which call sites across Phobos invoke `to`?"
3. **[D3 Retrieval]** "Retrieve the exact source of the `walkLength` function in `std.range.primitives` (the range-length walker)."
4. **[D4 Architecture]** "Describe the structure of the `std` package: the top-level modules (`conv.d`, `range`, `algorithm`, `format`, `container`, `datetime`, …) and how `package.d` files compose the multi-file sub-packages (e.g. `std.algorithm`, `std.range`, `std.format`)."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the range-trait predicate templates that are conceptually similar to `isInputRange` — e.g. `isForwardRange`, `isBidirectionalRange`, `isRandomAccessRange`, `isOutputRange`, `hasLength` — by semantic/naming-pattern similarity rather than exact text, surfacing the family of `is*Range`/`has*` traits scattered across `std.range.primitives` and `std.traits`."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="to|parse|text|octal|ConvException", path~="std/conv.d"); D2->trace_call_path(symbol="to", direction="both"); D3->get_code_snippet(qualified_name="std.range.primitives.walkLength"); D4->get_architecture(scope="std"); D5->search_code/semantic_query("range trait predicate is*Range / has* templates").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/dlang-graph.md`, `dlang-explorer.md`, `dlang-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the dlang tier.

---

### 72. nim — B (Systems & Low-level)

**Repo:** nim-lang/Nim (`/tmp/bench/nim`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The reference Nim implementation (compiler + stdlib, ~16k GitHub stars), large and idiomatic enough to stress definition/call-graph extraction; it matches the plan's "popular, substantial, idiomatic" repo-selection criterion for the Systems & Low-level group.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "In `lib/pure/strutils.nim`, list the public string-manipulation procs whose names match `split`, `parseInt`, and `strip`, and report each one's defining file and signature."
2. **[D2 Relationship]** "Show the call relationships around `compiler/parser.nim`'s `parseTopLevelStmt` (callers and callees, direction=both): which routines invoke it and which parsing helpers (e.g. `complexOrSimpleStmt`, `parseStmt`) does it call?"
3. **[D3 Retrieval]** "Retrieve the full source of the `parseString` proc defined in `compiler/parser.nim`."
4. **[D4 Architecture]** "Describe the top-level structure of the repo and the compiler pipeline: how do the `compiler/`, `lib/system.nim`, `lib/pure/`, and `tests/` areas relate, and what is the lexer -> parser -> sem -> codegen module flow (`lexer.nim`, `parser.nim`, `sem.nim`/`semexprs.nim`, `cgen.nim`/`jsgen.nim`/`vm.nim`)?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find code semantically related to 'destructor lifting and move semantics injection' across the compiler — e.g. `injectdestructors.nim`, `liftdestructors.nim` — and surface duplicated/parallel codegen backends (`cgen.nim` vs `jsgen.nim`) that implement the same logical pass. Labeled graph-favoring: relies on semantic similarity / cross-file clustering that plain grep cannot rank."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="split|parseInt|strip", label="Function"); D2->trace_call_path(qualified_name=".*parseTopLevelStmt", direction="both"); D3->get_code_snippet(qualified_name=".*parser.*parseString"); D4->get_architecture(); D5->search_code/semantic_query("destructor lifting move semantics injection").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/nim-graph.md`, `nim-explorer.md`, `nim-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the nim tier.

---

### 73. scheme — D (Functional & Formal)

**Repo:** gambit/gambit (`/tmp/bench/scheme`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Gambit is one of the most-starred, production-grade R7RS Scheme implementations (Feeley), with a large idiomatic `.scm` runtime/compiler corpus — ideal for the plan's "popular + substantial + idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level definitions whose names match `c#` / `define-prim` runtime primitives — e.g. find every definition of `##make-vector`, `##make-string`, and `##car` in the `lib/` runtime [verify], and confirm `.scm` files (not just generated `.c`) are the source surfaced." (grep-findable: concrete primitive names appear verbatim in the `.scm` sources.)
2. **[D2 Relationship]** "For the public reader entry point `read` / `##read-all` [verify], trace the call graph in both directions (callers and callees) across `lib/_io.scm` and `lib/_eval.scm` [verify] — who invokes it and what reader/datum-parsing helpers it dispatches to."
3. **[D3 Retrieval]** "Retrieve the exact source of the single procedure `##eval` (the runtime evaluator entry) [verify] from `lib/_eval.scm` [verify] by qualified name, returning only its body and precise line range." (grep-findable: `##eval` is a literal definition name.)
4. **[D4 Architecture]** "Describe the top-level structure of the Gambit source tree — the split between `lib/` (runtime `_*.scm`), `gsc/` (compiler, `_source.scm`/`_t-*.scm` backends) [verify], and `gsi/` (interpreter) — and how `.scm` modules are organized into these directories."
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING) Find all definitions semantically related to 'hash table construction and rehashing' across the runtime (e.g. `make-table`, `##gc-hash-table-rehash!`, `table-set!`) [verify] — a similarity/naming-cluster query that plain grep cannot resolve without enumerating every candidate token."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="##(make-vector|make-string|car)", label="Definition"); D2->trace_path(function_name="##read-all", mode="calls", direction="both"); D3->get_code_snippet(qualified_name="##eval"); D4->get_architecture(); D5->search_graph(semantic_query=["hash","table","construction","rehash"]) / search_code(pattern="hash table").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/scheme-graph.md`, `scheme-explorer.md`, `scheme-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the scheme tier.

---

### 74. fennel — C (Dynamic & Scripting)

**Repo:** bakpakin/Fennel (`/tmp/bench/fennel`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Fennel is the most popular Lisp-to-Lua compiler (multi-thousand-star, widely used in the LÖVE/Lua ecosystem) and is self-hosted — its compiler, parser, and macro system are written in idiomatic, substantial Fennel — making it the canonical real-world `.fnl` corpus for the plan's "popular + idiomatic + non-trivial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Where is the public compiler entry point `compile-string` defined, and what other top-level API functions (`compile-stream`, `eval`, `dofile`) does the module export? List them with their defining file." (grep-findable: `compile-string`/`compile-stream` appear verbatim as `(fn compile-string ...)` / `(fn compile-stream ...)` in `src/fennel/compiler.fnl` and are re-exported from `src/fennel.fnl`; `eval` and `dofile` are defined/exported in `src/fennel.fnl`.)
2. **[D2 Relationship]** "Trace the call relationships of `compile-string` in both directions: which functions does it call (directly or transitively) to produce Lua — e.g. `parser`/`string-stream` then `compile-stream` -> `compile` -> `compile1` — and which public-facing entry points (e.g. `eval`, `dofile`) call into it?"
3. **[D3 Retrieval]** "Retrieve the full source of the `granulate` function (the byte-stream-to-character generator used by the parser) exactly as defined in `src/fennel/parser.fnl`." (grep-findable: `(fn granulate [getchunk]`.)
4. **[D4 Architecture]** "Describe the module/file organization of the self-hosted compiler under `src/fennel/`: how are responsibilities split across `parser.fnl`, `compiler.fnl`, `specials.fnl`, `macros.fnl`, `utils.fnl`, and how do they connect through the top-level `src/fennel.fnl` aggregator?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring / semantic) Find the functions responsible for producing human-friendly compile/parse error messages — the 'friendly error' / suggestion machinery in `src/fennel/friend.fnl`, e.g. `friendly-msg`, `suggest`, and the `assert-compile` / `parse-error` drop-in replacements — using semantic similarity to terms like 'error', 'suggestion', 'helpful message' rather than guessing exact names. This favors the graph's semantic/`search_code` path because the relevant logic is concentrated in one aux module whose entry points (`assert-compile`, `parse-error`) read as ordinary asserts; a plain grep for 'error' is noisy across the whole compiler, and a searcher must already know to look at the `friend` vocabulary to grep precisely."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="compile-string|compile-stream|eval|dofile"); D2->trace_call_path(name="compile-string", direction="both"); D3->get_code_snippet(qualified_name=".../parser.fnl::granulate"); D4->get_architecture(scope="src/fennel"); D5->search_code("friendly error suggestion helpful message") / search_graph(semantic_query=["error","suggestion","friendly","message"]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/fennel-graph.md`, `fennel-explorer.md`, `fennel-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the fennel tier.

---

### 75. fish — C (Dynamic & Scripting)

**Repo:** fish-shell/fish-shell (`/tmp/bench/fish`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred shells on GitHub (~28k stars), a substantial, idiomatic body of `.fish` script (the shipped function library under `share/functions/`) plus a large core (Rust in fish 4.x, C++ in earlier releases) — exactly the "popular + substantial + idiomatic" profile the plan's repo-selection criteria call for in the Dynamic & Scripting group. The evaluation subject is the `.fish` script library, not the compiled core.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the `.fish` functions whose name matches the completion-helper / prompt family in `share/functions/`, e.g. `fish_default_key_bindings`, `fish_prompt`, `fish_vcs_prompt`, and `__fish_complete_path` — return each definition's file and the `function` header. (Symbols are plain-grep-findable via `function NAME`, so the graph and a grep baseline are scored on the same target.)"
2. **[D2 Relationship]** "For the function `fish_prompt`, show its caller/callee neighborhood across the `.fish` library: which `.fish` library functions it invokes (e.g. `fish_vcs_prompt`, `prompt_pwd`) — note that `set_color` is a core builtin, not a `.fish` function, so it should appear as a builtin call rather than a graph CALLS edge into the library — and which entry points trigger it. Direction = both."
3. **[D3 Retrieval]** "Retrieve the full body of the single function `fish_vcs_prompt` (defined in `share/functions/fish_vcs_prompt.fish`) — exact line range, no surrounding file noise. (File and symbol are grep/open-findable, so this is symmetric with a baseline.)"
4. **[D4 Architecture]** "Describe the structure of the shipped `.fish` script tree: how `share/functions/` (one function per file), `share/completions/`, and `share/tools/` are organized, and how the per-file `function`/`complete` convention maps onto the directory layout."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring.) Find `.fish` functions semantically similar to a git-status prompt segment — i.e. functions that probe VCS state and colorize output (`fish_vcs_prompt`, `fish_git_prompt`, `fish_hg_prompt`, `fish_svn_prompt`) — and surface near-duplicate prompt-building logic. Labeled graph-favoring because it relies on the keyword-vector `semantic_query` parameter of `search_graph` plus naming-pattern clustering, which a plain grep baseline cannot reproduce."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern, label=function); D2->trace_call_path(name="fish_prompt", direction=both); D3->get_code_snippet(qualified_name="fish_vcs_prompt"); D4->get_architecture(scope="share/"); D5->search_graph(semantic_query=["vcs","status","prompt","segment","colorize"], name_pattern=".*_prompt").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/fish-graph.md`, `fish-explorer.md`, `fish-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the fish tier.

---

### 76. awk — C (Dynamic & Scripting)

**Repo:** e36freak/awk-libs (`/tmp/bench/awk`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** A widely-referenced, idiomatic collection of reusable awk/gawk function libraries (strings, math, sort, csv, options/getopt, times) — substantial enough to exercise definition discovery and intra-/cross-file function reuse, satisfying the plan's "popular + idiomatic for the language" repo-selection criterion for a niche scripting language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "In the strings library (`strings.awk`), report the whitespace-trimming function `trim` and its parameter signature, and confirm whether the library exposes any separate left-only / right-only trim variants (e.g. `ltrim`/`rtrim`) or only the single `trim(str)`." (Grep-findable: `trim` is a literal `function trim(` definition; symmetric with text search.)
2. **[D2 Relationship]** "Within the sort library (`qsort.awk`), describe the call relationship around the helper `__quicksort` [verify]: which public sort entry points (`qsort`, `iqsort`, `qsorti`, `iqsorti` [verify]) invoke it, and what does it call in turn (e.g. a comparison helper such as `__compare` [verify])."
3. **[D3 Retrieval]** "Retrieve the complete body of the `getopts` function (the long-option-aware option parser in `options.awk`), exactly as defined in its source file, with no surrounding code." (Grep-findable: `getopts` is a literal `function getopts(` definition; symmetric with text search.)
4. **[D4 Architecture]** "Describe the structure of the repository: how the per-domain library files (`strings.awk`, `math.awk`, `qsort.awk`, `msort.awk`, `csv.awk`, `options.awk`, `times.awk`, etc.) live at the repo root, and how the sample scripts under the `examples/` directory relate to those libraries."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Semantic/duplication query (graph-favoring): cluster the family of related sort entry points in `qsort.awk` that share behavior and naming convention — `qsort` / `iqsort` / `qsorti` / `iqsortv` family [verify] — and identify which delegate to the same private helper (`__quicksort` vs `__vquicksort` [verify]). This groups functions by shared callee/behavior in a way plain text grep would not cluster."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*trim.*", label="Function"); D2->trace_call_path(qualified_name="...__quicksort", direction="both"); D3->get_code_snippet(qualified_name="...getopts"); D4->get_architecture(...); D5->search_graph(name_pattern=".*sort.*", label="Function") + trace_call_path to group by shared helper.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/awk-graph.md`, `awk-explorer.md`, `awk-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the awk tier.

---

### 77. zsh — C (Dynamic & Scripting)

**Repo:** ohmyzsh/ohmyzsh (`/tmp/bench/zsh`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, massively-starred (>170k) zsh framework — its `oh-my-zsh.sh` loader, `lib/*.zsh` core, and hundreds of `plugins/*/` and `themes/*/` files form deep, idiomatic, substantial zsh with heavy function-to-function sourcing, satisfying the plan's "popular + idiomatic + substantial" repo-selection criteria for Group C.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the top-level CLI dispatcher function `omz` (in `lib/cli.zsh`) and its `_omz` completion, plus the framework's public predicate API `is_plugin` and `is_theme` (in `lib/functions.zsh`); list these grep-findable entrypoints that constitute oh-my-zsh's command surface." (grep-findable: these appear literally as `function omz` / `is_plugin()` in `lib/*.zsh`.)
2. **[D2 Relationship]** "Starting from the `omz` dispatcher, trace the call graph (both directions) through its `_omz::*` subcommand handlers (e.g. `_omz::plugin::enable`, `_omz::update`, `_omz::reload` [verify]) to show how an `omz update` invocation reaches `upgrade_oh_my_zsh` / the `tools/upgrade.sh` upgrade path." (structural framing allowed for D2.)
3. **[D3 Retrieval]** "Retrieve the full source of the single function `git_prompt_info` (the prompt git-status helper in `lib/git.zsh`)." (one real, grep-findable named symbol.)
4. **[D4 Architecture]** "Describe the directory/file organization of oh-my-zsh: the role of `oh-my-zsh.sh`, the `lib/` core (`cli.zsh`, `git.zsh`, `functions.zsh`, `theme-and-appearance.zsh`, `completion.zsh`, `history.zsh`), `plugins/<name>/<name>.plugin.zsh` convention, `themes/*.zsh-theme`, `custom/`, and `tools/` (`install.sh`, `upgrade.sh`)."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Surface naming-pattern and config↔code links: cluster the `parse_git_*` / `git_prompt_*` prompt-state helper family across `lib/git.zsh` and the themes, and identify duplicated git-status prompt scaffolding shared across `themes/*.zsh-theme` — a similarity/duplication query a plain grep cannot rank." (explicitly graph-favoring; included in Group C D5 only.)

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="^(omz|_omz|is_plugin|is_theme)$", label=Function); D2->trace_call_path(name="omz", direction="both"); D3->get_code_snippet(qualified_name="git_prompt_info"); D4->get_architecture(); D5->search_code/semantic_query("git prompt state helper family; duplicated theme git-status scaffolding").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/zsh-graph.md`, `zsh-explorer.md`, `zsh-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the zsh tier.

---

### 78. tcl — C (Dynamic & Scripting)

**Repo:** tcltk/tcllib (`/tmp/bench/tcl`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Tcllib is the canonical, widely-deployed standard Tcl library (hundreds of pure-Tcl `modules/`), making it the most idiomatic and substantial public Tcl corpus — satisfying the plan's "popular + idiomatic + substantial" repo-selection criteria for the dynamic/scripting group.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the procedures defined by the `base64` module — e.g. show where `::base64::encode` and `::base64::decode` are declared (their `proc` definitions in `modules/base64/base64.tcl`)." (Both names are plain `proc`/`namespace export` tokens, so grep can find them too.)
2. **[D2 Relationship]** "For the `::csv::join` procedure in `modules/csv/csv.tcl`, show the full call relationship in both directions: which procedures `::csv::join` calls and which procedures (e.g. higher-level CSV writers such as `::csv::joinlist` [verify]) call it."
3. **[D3 Retrieval]** "Retrieve the complete source of the `::md5::md5` procedure as defined in `modules/md5/md5x.tcl` [verify: tcllib ships both `md5.tcl` (v1) and `md5x.tcl` (v2); confirm which file holds the active `proc ::md5::md5`]." (Well-known, grep-findable `proc ::md5::md5` token.)
4. **[D4 Architecture]** "Describe the top-level structure of tcllib: how the `modules/` tree is organized into independent packages, and how `pkgIndex.tcl` files relate modules to the package-loading layout."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Starting from the `base64` module, surface the config↔code structural links that grep cannot traverse: for each related encoder module (`uuencode`, `yencode`, `ascii85`), connect its `pkgIndex.tcl` package declaration to the implementing `.tcl` file and the procedure it registers (the encode/decode entry points). The graph wins here because the package-index-to-implementation edge is a structural relationship, not a shared token." (Plain text search can locate the individual files, but cannot follow the `pkgIndex.tcl`→implementation→proc chain in one hop.)

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="::base64::(encode|decode)", label="Function"); D2->trace_call_path(qualified_name="::csv::join", direction="both"); D3->get_code_snippet(qualified_name="::md5::md5"); D4->get_architecture(scope="modules/"); D5->search_graph(name_pattern=".*(uuencode|yencode|ascii85).*") + query_graph (MATCH pkgIndex/IMPORTS/CONTAINS edges from each module's pkgIndex.tcl to its implementing .tcl and registered proc).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/tcl-graph.md`, `tcl-explorer.md`, `tcl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the tcl tier.

---

### 79. ada — B (Systems & Low-level)

**Repo:** AdaCore/Ada_Drivers_Library (`/tmp/bench/ada`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** AdaCore's flagship open-source driver/HAL library is the most-starred, most idiomatic Ada repo for embedded/systems work — large, multi-package, spec/body (`.ads`/`.adb`) split — matching the plan's "popular + substantial + idiomatic" repo-selection criteria for Group B.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the declaration of the `HAL.GPIO` abstraction — locate the `GPIO_Point` interface type and the `Set`/`Clear`/`Set_Mode` primitive operations declared on it in the `HAL.GPIO` package spec (`hal/src/hal-gpio.ads`). List the package and the operation signatures." (grep-findable: `HAL.GPIO`, `GPIO_Point`, `Set_Mode`)
2. **[D2 Relationship]** "Using the call graph in both directions for the `Bitmapped_Drawing.Draw_Char` (or `Draw_String`) procedure, show what bitmap/font primitives it calls (e.g. pixel/character rendering) and which higher-level display/example routines call into it."
3. **[D3 Retrieval]** "Retrieve the full body of the `Configure` procedure for the MMA8653 accelerometer driver (package `MMA8653`), i.e. the implementation that configures the device over I2C." (single named symbol: `MMA8653.Configure` — grep-findable; the package's init/config entry point is `Configure`, not `Initialize`)
4. **[D4 Architecture]** "Describe the layered architecture: how the hardware-independent `HAL` packages (`hal/`), the chip-family/arch layer (`arch/ARM/STM32`), the board layer (`boards/`), and the component/middleware drivers (`components/`, e.g. `MMA8653`, `LIS3DSH`) are organized into directories and how the dependency layering flows from boards down to HAL interfaces."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across all peripheral component drivers, find packages that implement the same conceptual `Configure` + register read-write pattern over I2C/SPI (e.g. `MMA8653`, `LIS3DSH`, `L3GD20`) — i.e. semantically-similar driver configuration routines and their links to the `HAL.I2C`/`HAL.SPI` interface specs they depend on. This duplication/interface-implementation clustering is not reachable by plain text search."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*GPIO_Point.*|.*Set_Mode.*", label="Type/Subprogram"); D2->trace_call_path(qualified_name="Bitmapped_Drawing.Draw_Char", direction="both"); D3->get_code_snippet(qualified_name="MMA8653.Configure"); D4->get_architecture(scope="repo"); D5->search_code/semantic_query("I2C/SPI device configure register read-write pattern").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/ada-graph.md`, `ada-explorer.md`, `ada-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the ada tier.

---

### 80. agda — D (Functional & Formal)

**Repo:** agda/agda-stdlib (`/tmp/bench/agda`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical Agda standard library (~3k stars, the de-facto stdlib every Agda project depends on); large, idiomatic, hierarchically-organized dependently-typed code, satisfying the plan's "popular + substantial + representative of the language" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definitions of the natural-number addition lemmas `+-comm` and `+-assoc` in `Data.Nat.Properties` — where are they declared and what are their type signatures?" (Both are grep-findable identifiers in `src/Data/Nat/Properties.agda`.)
2. **[D2 Relationship]** "Show the relationship neighborhood of `+-comm` [verify]: which proofs/lemmas in `Data.Nat.Properties` reference it, and which lower-level lemmas (e.g. `+-suc`, `+-identityʳ` [verify]) does its proof depend on?"
3. **[D3 Retrieval]** "Retrieve the full source of the `CommutativeMonoid` record bundle defined in `Algebra.Bundles`." (Single named symbol, grep-findable as `record CommutativeMonoid`.)
4. **[D4 Architecture]** "Describe the top-level module/directory architecture of `src/` — how are `Data`, `Relation`, `Algebra`, `Function`, and `Everything.agda` organized, and what is the layering between `*.Base` modules and their `*.Properties` siblings?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across the whole library, find proofs semantically equivalent to a commutativity statement — i.e. lemmas named `*-comm`, `∨-comm`, `++-comm` [verify] etc. that mirror `+-comm`'s shape — and cluster the recurring `Properties`-module naming/structure pattern. This leans on semantic similarity + naming-pattern detection that plain grep cannot cluster."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="\\+-comm|\\+-assoc", project="agda"); D2->trace_call_path(name="+-comm", direction="both"); D3->get_code_snippet(qualified_name="Algebra.Bundles.CommutativeMonoid"); D4->get_architecture(project="agda"); D5->search_code/semantic_query("commutativity lemma pattern across Properties modules").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/agda-graph.md`, `agda-explorer.md`, `agda-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the agda tier.

---

### 81. racket — D (Functional & Formal)

**Repo:** racket/racket (`/tmp/bench/racket`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The reference implementation of Racket (Lisp/Scheme family); large, idiomatic, macro-heavy functional codebase — a high-popularity OSS pick that stresses the indexer on `define`/`define-syntax` forms, matching the plan's "popular + substantial + idiomatic" repo-selection criteria for Group D.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the definitions in `racket/collects/racket/list.rkt` — e.g. the public list utilities `filter-map`, `flatten`, `last`, `take`, `drop`, `count`, `append-map` — and report which file/line each is defined at. (grep-findable: each appears verbatim as `(define (<name> ...)` in that file.)"
2. **[D2 Relationship]** "For `append-map` (defined in `racket/collects/racket/list.rkt`), show the full call relationship in both directions: which helper/library procedures it calls (e.g. `map`, `apply`, `append`) and which other library procedures call it. Use direction=both."
3. **[D3 Retrieval]** "Retrieve the exact source body of the single `flatten` definition in `racket/collects/racket/list.rkt` — return just that procedure body, not the whole module. (grep-findable: the literal `(define (flatten` appears in that file.)"
4. **[D4 Architecture]** "Describe the top-level architecture of the repo: the split between `racket/src/cs/` (Chez Scheme backend), `racket/src/bc/` (bytecode backend), `racket/src/ChezScheme/`, and the `racket/collects/` standard-library tree — how source backends relate to the collection hierarchy."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find procedures across `racket/collects/racket/` that are semantically 'list traversal / accumulation' even when the name does not contain `fold` — e.g. `for/fold`, `append-map`, `count`, `filter-map` — and surface near-duplicate accumulation patterns that a text grep on `fold` alone would miss. Note that the canonical `foldl`/`foldr` themselves live in `racket/collects/racket/private/list.rkt` [verify], not in the public `list.rkt`, so name-only retrieval is insufficient."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="filter-map|flatten|last|take|drop|count|append-map", path~="racket/collects/racket/list.rkt"); D2->trace_call_path(name="append-map", direction="both"); D3->get_code_snippet(qualified_name=".../racket/collects/racket/list.rkt:flatten"); D4->get_architecture(scope="repo"); D5->search_code/semantic_query("list traversal accumulation fold-like").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/racket-graph.md`, `racket-explorer.md`, `racket-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the racket tier.

---

### 82. odin — B (Systems & Low-level)

**Repo:** odin-lang/Odin (`/tmp/bench/odin`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The official Odin compiler + `core` standard library is the canonical, idiomatic, substantial Odin corpus (high-profile systems language), matching the plan's "popular, real-world, language-representative" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the procedure `strings.contains` in `core/strings/strings.odin` and list the other `contains_*` procedures (e.g. `contains_rune`, `contains_any`) declared in the same file."
2. **[D2 Relationship]** "Trace the call relationships around `fmt.println` (in `core/fmt/fmt.odin`): which formatting helpers does it invoke (e.g. `wprintln` / `fmt_value` [verify]) and which user-facing `print*` procedures share that path?"
3. **[D3 Retrieval]** "Retrieve the full source of the procedure `fmt.fmt_value` [verify] from `core/fmt/fmt.odin` (the large central value-formatting dispatcher)."
4. **[D4 Architecture]** "Describe the package/directory organization of the Odin standard library under `core/` (e.g. `core/fmt`, `core/strings`, `core/mem`, `core/os`, `core/slice`, `core/runtime` [verify]) and how packages are grouped."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find procedures across `core/` that perform allocator-aware allocation — semantically similar to `mem.alloc` / `make` — surfacing the cross-package allocation pattern (e.g. `context.allocator` usage) that plain text search for one name would miss."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*contains.*", label="Function"); D2->trace_call_path(name="fmt.println", direction="both"); D3->get_code_snippet(qualified_name="fmt.fmt_value"); D4->get_architecture(); D5->search_code/semantic_query("allocator-aware allocation in core").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/odin-graph.md`, `odin-explorer.md`, `odin-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the odin tier.

---

### 83. rescript — D (Functional & Formal)

**Repo:** rescript-lang/rescript-core (`/tmp/bench/rescript`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The official `@rescript/core` standard library — the most widely depended-on idiomatic ReScript codebase, substantial (dozens of `.res`/`.resi` module pairs) and authored by the language team, matching the plan's "popular + idiomatic + non-trivial" repo-selection criterion.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the public functions exposed by the `Array` module — confirm the tool surfaces grep-findable bindings such as `Array.map`, `Array.filter`, `Array.reduce`, and `Array.findIndex` from `src/Core__Array.res` / `.resi`."
2. **[D2 Relationship]** "For `Result.flatMap` [verify], show its full call relationships (callers and callees, direction=both) — e.g. whether `Result.map` or `Option.flatMap` share/route through the same helper construction."
3. **[D3 Retrieval]** "Retrieve the exact source of the single symbol `Array.reduce` [verify] from `src/Core__Array.res`, including its signature and body."
4. **[D4 Architecture]** "Describe the module/file organization of the library under `src/`: how the `Core__*.res` implementation modules relate to the top-level `RescriptCore.res` re-export aggregator and the paired `.resi` interface files."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find functions semantically similar to 'apply a transform to every element and flatten the result' across modules (e.g. `Array.flatMap`, `Option.flatMap`, `Result.flatMap`) — surfacing the recurring functor-like `flatMap`/`map` naming pattern that plain text search cannot cluster by intent."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Function", name_pattern="Array\\..*"); D2->trace_call_path(name="Result.flatMap", direction="both"); D3->get_code_snippet(qualified_name="Array.reduce"); D4->get_architecture(scope="src"); D5->search_code/semantic_query("map and flatten each element").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/rescript-graph.md`, `rescript-explorer.md`, `rescript-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the rescript tier.

---

### 84. purescript — D (Functional & Formal)

**Repo:** purescript-halogen/purescript-halogen (`/tmp/bench/purescript`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical PureScript UI framework (~2k stars [verify], the de-facto SPA library); idiomatic, substantial pure-PureScript source under `src/Halogen`, matching the plan's "popular + representative of the language's real-world style" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the public component-construction API in `Halogen.Component`: locate the definitions of `mkComponent`, `mkEval`, and `defaultEval`, and the `ComponentSpec` / `EvalSpec` type aliases. Report each symbol's module and kind (function vs. type alias)." (all grep-findable identifiers)
2. **[D2 Relationship]** "Map the relationships around `mkEval` in `Halogen.Component` (direction=both): which functions call it, and which `EvalSpec`/`defaultEval` pieces it consumes? Show the eval-construction call chain that connects `defaultEval` through `mkEval` to its callers."
3. **[D3 Retrieval]** "Retrieve the full source of the `subscribe` function in `Halogen.Query.HalogenM` (the emitter-subscription combinator that auto-stops on component disposal) with exact line boundaries." (single named, grep-findable symbol)
4. **[D4 Architecture]** "Describe the module/directory architecture of `src/Halogen`: the top-level modules (`Aff`, `Component`, `HTML`, `Query`) and the subpackages (`HTML/`, `Query/`, `VDom/`, `Data/`, `Aff/`, and a `Component/` submodule directory [verify]). How is the VDom layer separated from the public `HTML` and `Component` surface?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find code semantically related to 'subscribing to an event emitter and cleaning up the subscription when the component is finalized' across the codebase — e.g. `subscribe`, `subscribe'`, `unsubscribe`, and `SubscriptionId` usage in `HalogenM` and its consumers — surfacing the cross-cutting subscription-lifecycle pattern that plain symbol lookup would miss."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="mkComponent|mkEval|defaultEval|ComponentSpec|EvalSpec"); D2->trace_call_path(name="mkEval", direction="both"); D3->get_code_snippet(qualified_name="Halogen.Query.HalogenM.subscribe"); D4->get_architecture(scope="src/Halogen"); D5->search_code/semantic_query(query="subscribe to emitter and unsubscribe on component finalize").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/purescript-graph.md`, `purescript-explorer.md`, `purescript-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the purescript tier.

---

### 85. nickel — D (Functional & Formal)

**Repo:** tweag/nickel (`/tmp/bench/nickel`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Tweag's reference implementation of the Nickel config language and ships a substantial hand-written Nickel standard library — the canonical, most-starred idiomatic Nickel corpus, matching the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the standard-library function `map` in the `std.array` module (`std.array.map`) and report its file and the record field that binds it." — well-known, grep-findable identifier (`map` bound as a field, defined as `fun f array => ...`, under the `array` record in `core/stdlib/std.ncl` [verify path]).
2. **[D2 Relationship]** "Starting from `std.array.fold_left`, trace the relationship graph in both directions: which other stdlib functions does it reference (e.g. helper folds/recursion), and which stdlib functions invoke `fold_left` (e.g. `std.array.sum` [verify], `std.array.flatten` [verify])."
3. **[D3 Retrieval]** "Retrieve the full source of the single function `std.string.split` exactly as defined in the Nickel stdlib." — one real, grep-findable named symbol (`split` field under the `string` record).
4. **[D4 Architecture]** "Describe the structural/module organization of the Nickel standard library: how the top-level `std` record is partitioned into nested sub-module records (`array`, `record`, `string`, `number`, `contract`, `function`, `enum` [verify]) within the single `core/stdlib/std.ncl` file [verify path], and how the language implementation is organized across the Rust crate layout (`core/`, `lsp/`, `utils/` [verify])."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find functions across the stdlib that are semantically equivalent record/array transformers — i.e. the `map`/`filter`/`fold` family duplicated in shape between `std.array` and `std.record` — and surface naming-pattern and structural-similarity links a plain text search would miss."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*map.*", label="Definition"); D2->trace_call_path(qualified_name="std.array.fold_left", direction="both"); D3->get_code_snippet(qualified_name="std.string.split"); D4->get_architecture(project="nickel"); D5->search_code/semantic_query("map filter fold transformers across array and record modules").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/nickel-graph.md`, `nickel-explorer.md`, `nickel-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the nickel tier.

---

### 86. crystal — A (Class-based OOP & Contracts)

**Repo:** crystal-lang/crystal (`/tmp/bench/crystal`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The reference Crystal compiler + standard library — the canonical, idiomatic, large-scale Crystal corpus (classes, modules, mixins, macros), satisfying the plan's "popular + substantial + idiomatic" repo-selection criterion for the language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definition of the `Array(T)` class and enumerate its public instance methods such as `push`, `map`, and `sort` (defined in `src/array.cr`)."
2. **[D2 Relationship]** "Map the relationships of `Comparable#<=>` (`src/comparable.cr`): which comparison operators (`<`, `>`, `==`, `clamp`) call into it, and which stdlib classes mix in `Comparable` and thus implement it? Show callers and callees (direction=both)."
3. **[D3 Retrieval]** "Retrieve the exact source of `String#gsub` from `src/string.cr` (one named symbol)."
4. **[D4 Architecture]** "Describe the top-level structure of the standard library under `src/` — how the compiler tree (`src/compiler/crystal/`) is separated from the runtime stdlib (`src/array.cr`, `src/hash.cr`, `src/io.cr`, `src/json/`, `src/http/`), and how mixin modules (`Enumerable`, `Indexable`, `Comparable`) are organized relative to the concrete collection classes that include them."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Across the stdlib, find the methods that materialize a collection's elements regardless of vocabulary — e.g. `to_a`, `to_unsafe`, `each`, `map` — and identify duplicated `Enumerable`/`Indexable` iteration idioms reimplemented per-class. Uses semantic/similarity search; not reliably reachable by literal grep."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Array", label="Class", project="crystal"); D2->trace_call_path(qualified_name="Comparable#<=>", direction="both"); D3->get_code_snippet(qualified_name="String#gsub"); D4->get_architecture(project="crystal"); D5->search_code(["to_a","each","iterate","materialize"]) / query_graph (semantic/similarity).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/crystal-graph.md`, `crystal-explorer.md`, `crystal-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the crystal tier.

---

### 87. teal — C (Dynamic & Scripting)

**Repo:** teal-language/tl (`/tmp/bench/teal`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ~2.8k-star reference compiler for Teal (a typed dialect of Lua), self-hosted in Teal itself — a substantial, idiomatic `.tl` codebase (lexer/parser/type-checker/codegen) that exercises Teal records, enums, generics, and module structure, matching the plan's "popular + idiomatic + non-trivial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the exported lexer entry point `lexer.lex` and the top-level `Token` record / `TokenKind` enum defined in `teal/lexer.tl`. Does the tool surface these as distinct definitions with their kinds (function vs record vs enum)?" (all grep-findable identifiers)
2. **[D2 Relationship]** "For the type-checker entry point in `teal/check/type_checker.tl`, show the call graph in both directions — what `parser.parse` / `node_checker` functions feed into it, and which API-layer functions in `teal/api/` invoke it. Are caller and callee edges across the parser→check→api module boundary recovered?"
3. **[D3 Retrieval]** "Retrieve the full source of the `RecordType` record definition from `teal/types.tl` (one named symbol), with exact start/end boundaries and no surrounding sibling records."
4. **[D4 Architecture]** "Describe the module/directory organization of the `teal/` source tree — the relationship between `lexer.tl`, `parser.tl`, `types.tl`, `environment.tl`, `errors.tl` and the `check/`, `gen/`, `api/` subdirectories. Does the structure view reflect the lex→parse→check→gen pipeline?"
5. **[D5 Cross-cutting/Semantic]** "**(Graph-favoring — semantic.)** Across `teal/types.tl`, find the family of `*Type` records that model Teal's type system (`RecordType`, `FunctionType`, `ArrayType`, `MapType`, `TupleType`, `EnumType`, `NominalType`, …) by semantic similarity / naming pattern rather than one literal string, and link them to the interfaces they implement (`Type`, `StructuralType`, `AggregateType`). Recovering the implements/naming cluster is where the graph should beat grep."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="lex|Token|TokenKind", project="teal"); D2->trace_call_path(name="type_checker"/"check", direction="both"); D3->get_code_snippet(qualified_name="teal.types.RecordType"); D4->get_architecture(project="teal"); D5->search_code/semantic_query(".*Type$" / "Teal type-system record variants and their implemented interfaces").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/teal-graph.md`, `teal-explorer.md`, `teal-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the teal tier.

---

### 88. hare — B (Systems & Low-level)

**Repo:** harelang/hare (`/tmp/bench/hare`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical Hare standard library + bootstrap toolchain (sourcehut mirror), the largest idiomatic body of Hare in existence — exercises a systems language (tagged-union errors, modules, `.ha` units) the way the plan's repo-selection criteria demand: popular, substantial, self-hosted.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the public formatting API in the `fmt` module — locate the definitions of `fmt::printfln` and `fmt::println` (declared with `export fn` in `fmt/print.ha` [verify]) and report their signatures."
2. **[D2 Relationship]** "Map the call relationships around `hare::lex::lex` (the lexer entry point [verify]): which parse-layer functions (e.g. in `hare/parse/`) call into it, and what helpers does it itself call — give the inbound and outbound call graph."
3. **[D3 Retrieval]** "Retrieve the complete source of the `strings::dup` function (in `strings/dup.ha` [verify]) — exact body, not a summary."
4. **[D4 Architecture]** "Describe the top-level module organization of the standard library: how are directories like `crypto/`, `encoding/`, `hare/` (the self-hosted lex/parse/ast/types pipeline), `io/`, `os/`, and `fmt/` arranged, and how do `.ha` units group into modules?"
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring.) Hare encodes failure as tagged-union error types rather than exceptions. Find functions across the stdlib whose return type is or includes an error union (e.g. members of `io::error`, `fs::error`, or module-local `!`-returning fns) and identify the recurring error-handling pattern — a semantic/similarity query grep cannot resolve from identifiers alone."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*printfln.*|.*println.*", label="Function"); D2->trace_call_path(qualified_name="hare::lex::lex", direction="both"); D3->get_code_snippet(qualified_name="strings::dup"); D4->get_architecture(...); D5->search_code/semantic_query("tagged-union error return handling").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/hare-graph.md`, `hare-explorer.md`, `hare-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the hare tier.

---

### 89. pony — A (Class-based OOP & Contracts)

**Repo:** ponylang/ponyc (`/tmp/bench/pony`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ponyc is the canonical Pony compiler and ships the entire idiomatic Pony standard library under `packages/`; it is the most-starred, most-substantial body of real Pony (traits/interfaces, actors, reference capabilities) and matches the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the public types defined in `packages/collections` and confirm the API surface of the `Map` / `HashMap` family — the hashing contract (`HashFunction` / `HashEq` [verify]) plus the `apply`, `update`, and `remove` methods — so we can check whether the graph enumerates these grep-findable class/method definitions as completely as a text search for `class Map` / `fun apply`."
2. **[D2 Relationship]** "For the actor `TCPConnection` in `packages/net`, trace the call relationships (direction=both) of its `write` / `_pending_writev` [verify] path: which methods invoke it and which lower-level send/flush behaviours it calls in turn?"
3. **[D3 Retrieval]** "Retrieve the full source of the `Promise` class (its `apply`, `next`, and `join` methods) defined in `packages/promises/promise.pony`."
4. **[D4 Architecture]** "Describe the package/directory architecture of the Pony standard library under `packages/` — how `builtin`, `collections`, `net`, `files`, and `time` are organized as separate packages and which depend on `builtin`."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across the stdlib, find the types that implement the `Stringable` / `Comparable` / `Hashable` contract pattern (a `fun string(): String iso^` or `fun compare(...)` method) and surface near-duplicate `HashMap` specializations — a semantic/contract-similarity query that plain grep for one keyword cannot cluster."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Class"/"Trait", name_pattern=".*Map.*", project="pony"); D2->trace_call_path(qualified_name="...TCPConnection.write", direction="both"); D3->get_code_snippet(qualified_name="promises.Promise"); D4->get_architecture(project="pony"); D5->search_code("fun string(): String iso^") + query_graph (cluster types sharing the Stringable/Comparable contract).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/pony-graph.md`, `pony-explorer.md`, `pony-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the pony tier.

---

### 90. luau — C (Dynamic & Scripting)

**Repo:** luau-lang/luau (`/tmp/bench/luau`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** High-profile (>11k stars), idiomatic typed-Lua dialect from Roblox; a substantial real-world language implementation (Ast/Compiler/Analysis/VM/CodeGen in C/C++ plus `tests/*.luau` fixtures), matching the plan's "popular + substantial + idiomatic" repo-selection criteria for the Dynamic & Scripting group.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the definitions of the public compile entry points exposed by the Luau compiler — specifically the C++ `Luau::compile` and the C-ABI `luau_compile` — and report their declaring header/source files." (Both are well-known, grep-findable identifiers: `Luau::compile` in `Compiler/include/Luau/Compiler.h` / `Compiler/src/Compiler.cpp`, and the C-ABI `luau_compile` in `Compiler/include/luacode.h` / `Compiler/src/lcode.cpp` [verify — exact C-ABI header/source file names].)
2. **[D2 Relationship]** "For `Luau::BytecodeBuilder::finalize` [verify], show the full caller/callee neighborhood (direction=both): which compiler stages invoke it and which lower-level bytecode-emission helpers it calls."
3. **[D3 Retrieval]** "Retrieve the complete source of the `Luau::compileOrThrow` [verify — overloaded; exact QN may resolve to a source-string or AST-block overload] function (the throwing compile wrapper) exactly as written, with no surrounding noise. `compileOrThrow` is a real, grep-findable identifier declared in `Compiler/include/Luau/Compiler.h`, so plain text search can recover it too."
4. **[D4 Architecture]** "Describe the top-level architecture of the repository: the separation between `Ast/`, `Compiler/`, `Analysis/`, `VM/`, and `CodeGen/` and how the `tests/` tree (including the `tests/*.luau` conformance fixtures) is organized relative to them."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Find code semantically related to 'type inference of table/record fields' across the Analysis subsystem — e.g. constraint-solving and table-unification routines — that a plain keyword grep for 'infer' would miss because the relevant functions are named after their constraint type rather than the concept."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Luau::compile|luau_compile"); D2->trace_call_path(name="Luau::BytecodeBuilder::finalize", direction="both"); D3->get_code_snippet(qualified_name="Luau::compileOrThrow"); D4->get_architecture(...); D5->search_code/semantic_query("table field type inference / constraint solving").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/luau-graph.md`, `luau-explorer.md`, `luau-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the luau tier.

---

### 91. janet — C (Dynamic & Scripting)

**Repo:** janet-lang/spork (`/tmp/bench/janet`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** spork is the official Janet contrib/standard library — the canonical, substantial, idiomatic multi-module Janet codebase, satisfying the plan's "popular + representative of the language" repo-selection criterion.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the public definitions exported by the `spork/argparse` module — specifically locate the `argparse` function and any helper defs it relies on. (Both `defn argparse` and the module name `argparse` are plain grep-findable identifiers.)"
2. **[D2 Relationship]** "Map the call relationships around `spork/sh`'s `exec-slurp` [verify] — which functions within `spork/sh` call it and which lower-level process/exec primitives it invokes (direction=both)."
3. **[D3 Retrieval]** "Retrieve the full source of the `argparse` function defined in `spork/argparse.janet` — the single largest top-level `defn` in that module."
4. **[D4 Architecture]** "Describe spork's module organization: how the top-level `spork/` directory groups independent modules (e.g. `argparse`, `json`, `htmlgen`, `path`, `sh`, `test`, `misc`, `fmt`, `http` [verify]) and how `init.janet` / the project file wires them together."
5. **[D5 Cross-cutting/Semantic] (graph-favoring)** "Find functions across spork that perform the same cross-cutting concern of shelling out / spawning subprocesses (e.g. `spork/sh` exec helpers vs. `spork/cc` [verify] compiler invocation) — a semantic-similarity query that plain grep for one literal name cannot surface."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*argparse.*", label="Function"); D2->trace_call_path(qualified_name="...exec-slurp", direction="both"); D3->get_code_snippet(qualified_name="...argparse"); D4->get_architecture(...); D5->search_code/semantic_query("spawn subprocess / shell out helpers").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/janet-graph.md`, `janet-explorer.md`, `janet-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the janet tier.

---

### 92. sway — A (Class-based OOP & Contracts)

**Repo:** FuelLabs/sway-applications (`/tmp/bench/sway`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** FuelLabs' flagship, actively-maintained monorepo of reference Sway dapps (AMM, escrow, multisig, NFT, auction, oracle) — the most popular, idiomatic, substantial body of Sway smart-contract code, satisfying the plan's "popular + idiomatic + non-trivial" repo-selection criteria for the contract-OOP group.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the contract ABI declarations and their method signatures defined for the AMM application in `AMM/libraries/src/interface.sw` — specifically the `abi AMM` interface (declares `initialize`, `add_pool`, `pool`) and the `abi Exchange` interface (declares `deposit`, `add_liquidity`, `remove_liquidity`, `swap_exact_input`, `swap_exact_output`, `withdraw`, `balance`, `pool_info`, …). These are grep-findable via `abi ` / `fn ` and should also surface as graph definition nodes."
2. **[D2 Relationship]** "For the Exchange contract, trace the call relationships of the `add_liquidity` function in both directions: which entry-point ABI methods reach it and which internal/library helpers (e.g. balance lookups, `transfer`, `mint`) it calls."
3. **[D3 Retrieval]** "Retrieve the full source of the `swap_exact_input` function in the Exchange contract (`AMM/exchange-contract/src/main.sw`) — one named symbol, exact body."
4. **[D4 Architecture]** "Describe the structural organization of the sway-applications monorepo: how each application (AMM, escrow, multisig-wallet, NFT, english-auction, oracle) is laid out into its `*-contract/src/main.sw` entry, shared interface/`abi` modules (e.g. `AMM/libraries/src/interface.sw`), and library files, and how these directories nest."
5. **[D5 Cross-cutting/Semantic] (graph-favoring)** "Across all applications, find the recurring access-control / ownership guard pattern — code that asserts the caller is the owner before a state-mutating call (e.g. `require(msg_sender()... , ...)` ownership checks, `only_owner`-style guards). This is semantic/duplication-pattern discovery openly favoring graph+semantic search over plain grep."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*Exchange.*|.*AMM.*", label="Interface"); D2->trace_path(name="add_liquidity", direction="both"); D3->get_code_snippet(qualified_name=".*swap_exact_input"); D4->get_architecture(...); D5->search_code / search_graph(semantic_query=["owner","authorization","guard","state mutation"]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/sway-graph.md`, `sway-explorer.md`, `sway-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the sway tier.

---

### 93. nasm — B (Systems & Low-level)

**Repo:** cirosantilli/x86-bare-metal-examples (`/tmp/bench/nasm`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred standalone x86 bare-metal teaching repos. Note: the bulk of the repo is **GNU assembler (`.S`, AT&T syntax)** plus a GAS macro library (`common.h`); the genuinely NASM (Intel-syntax `.asm`) content is the small `nasm/` subdirectory (`bios_hello_world.asm`, `bios_disk_load.asm`, `bios_one_char.asm`, `protected_mode_so.asm`, `protected_mode_thiscouldbebetter.asm`). Questions below are scoped to that real NASM subset, so the corpus is modest, not "large" — chosen as the most-starred *NASM* example set available for the systems/low-level NASM tier, with the size caveat noted explicitly for fair grading.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the labels defined across the NASM examples in `nasm/` — entry/structure labels such as `start`, `stage2`, `print32`, `for_each_char`, and the GDT labels `gdt_start` / `gdt_code` / `gdt_data` — and report which `.asm` file each is defined in. (grep-findable: every one is a literal `name:` label definition.)"
2. **[D2 Relationship]** "Starting from the `print32` routine in `protected_mode_so.asm` — the single NASM routine in this repo actually reached via a `call` — show its relationship: which site issues `call print32`, and what `print32` itself touches (its `.loop`/`.done` VGA-write loop into `0xb8000`). NOTE: this NASM corpus has near-zero call-graph density — most examples print via inline `int 0x10` loops with `jmp`, not `call` — so D2 is graded against this one real `call`-to-label edge and is **not** penalized for the absence of a richer call graph."
3. **[D3 Retrieval]** "Retrieve the full body of the `print32` routine in `protected_mode_so.asm` — from the `print32:` label through its `.done` exit (the per-character loop that writes character/attribute pairs into VGA memory) — so I can read it without opening the file. (one real, grep-findable named label.)"
4. **[D4 Architecture]** "Describe how the repo is organized for the NASM subset: the `nasm/` subdirectory of Intel-syntax `.asm` bootloader examples (each its own self-contained file with `bits 16` / `org 0x7c00`, no `%include` and no `%macro`), its own `Makefile`/`run` script, and how this sits **alongside** the larger AT&T-syntax `.S` corpus + GAS `common.h` macro library at the repo root (which is a different assembler dialect)."
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING) Surface the duplication/similarity cluster of near-identical *inline* 'print a string to screen' loops copy-pasted across the NASM examples — the `lodsb` + `int 0x10` (AH=0x0E teletype) pattern in `bios_hello_world.asm`'s `.loop`, `bios_disk_load.asm`'s `for_each_char`, and `protected_mode_thiscouldbebetter.asm`'s `for_each_char`, plus the VGA `0xb8000`-write variant in `protected_mode_so.asm`'s `print32` — and rank them as a Type-1/Type-2 clone family. A plain grep on any single identifier cannot assemble this cross-file similarity cluster because the loops share *no* common label name. (explicitly graph-favoring; included in Group B D5 only.)"

**Expected graph tools (hint, not a script):** D1->search_graph(project="...nasm", name_pattern="(start|stage2|print32|for_each_char|gdt_.*)", label/kind=label-or-function); D2->trace_call_path(name="print32", direction="both"); D3->get_code_snippet(qualified_name=".*print32"); D4->get_architecture(project="...nasm"); D5->search_code / search_graph(semantic_query=["print string","lodsb","int 0x10 teletype","VGA write loop"]) for near-duplicate clustering.

**Note on D2/D3 for NASM:** there is no high-level "call graph" in the C/Go sense — structural relationships here are `call`/`jmp` to labels. In this particular repo the NASM examples are almost entirely inline (one real `call print32`; no `%include`, no `%macro` anywhere in the `.asm` files). D2 is therefore framed against the single genuine `call`-to-label edge and graded as the closest faithful analogue, **not** penalized for absent function-call semantics. (The `%include`/`%macro` machinery exists only in the repo's separate GAS/AT&T `.S` + `common.h` portion, which is out of scope for the NASM tier.)

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/nasm-graph.md`, `nasm-explorer.md`, `nasm-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the nasm tier.

---

### 94. assembly — B (Systems & Low-level)

**Repo:** pret/pokered (`/tmp/bench/assembly`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred, actively-maintained reverse-engineering disassemblies on GitHub (the canonical RGBDS Game Boy Z80 project); large, idiomatic hand-written `.asm` with thousands of labels, macros, and `INCLUDE`-stitched files — a stress test for a graph that must treat assembly labels as first-class symbols.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the definitions of the core engine routines `DisplayTextID` and `PlayMusic` — list the file and label location where each is defined as a callable Z80 label." (Both are well-known global labels, grep-findable as `DisplayTextID:` / `PlayMusic:`.)
2. **[D2 Relationship]** "Map the call relationships around `DisplayTextID`: which routines `call`/`jp` into it, and which routines it itself `call`s (e.g. into text-printing helpers). Show callers and callees in both directions."
3. **[D3 Retrieval]** "Retrieve the full body of the `JoypadLowSensitivity` routine exactly as defined (label through its terminating `ret`)." (Grep-findable as `JoypadLowSensitivity:` in `home/joypad.asm` [verify].)
4. **[D4 Architecture]** "Describe the top-level architecture: how the ROM is assembled from `main.asm`/`home.asm` via `INCLUDE` directives, and how the `home/`, `engine/`, `data/`, `constants/`, and `macros/` directories partition the codebase."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Semantic/cross-cutting: locate all routines and data tables related to 'battle damage calculation' across the disassembly, even when label names differ (e.g. `CalculateDamage`, `CriticalHitTest`, the per-side damage-variable setup `GetDamageVarsForPlayerAttack` / `GetDamageVarsForEnemyAttack`, and the stat/modifier data they read). This favors the graph's semantic_query + cross-file linking over literal grep, since the concept spans many differently-named labels and data files."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="DisplayTextID|PlayMusic", label="Definition"); D2->trace_call_path(name="DisplayTextID", direction="both"); D3->get_code_snippet(qualified_name="JoypadLowSensitivity"); D4->get_architecture(); D5->search_code/semantic_query("battle damage calculation critical hit").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/assembly-graph.md`, `assembly-explorer.md`, `assembly-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the assembly tier.

---

### 95. astro — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** withastro/astro (`/tmp/bench/astro`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Astro is a top-tier (40k+ star) web framework whose monorepo is the canonical, idiomatic body of `.astro` markup/template files, content-collection schemas, and layered `astro.config.*` files — exactly the config/markup/schema material Group E is meant to stress, at substantial scale (packages/, examples/, hundreds of test fixtures).

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the top-level public configuration/schema entry points exposed by the `astro` package — specifically the `defineConfig`, `defineCollection`, and `defineMiddleware` helpers and the `AstroUserConfig` config type. Where are they defined and what is each one's shape?" (grep-findable: each name is a literal exported identifier; `defineConfig` and `defineCollection` are the most-typed strings in any Astro project.)
2. **[D2 Relationship]** "Take the `defineConfig` helper (or, structurally, the `astro:content` virtual module) and show its cross-file reference graph: which config files (`astro.config.mjs`/`.ts` across `examples/` and `packages/*`) import it, and which internal module(s) consume the object it returns (e.g. the config validation / `validateConfig` path)?"
3. **[D3 Retrieval]** "Retrieve the full definition of the Astro config Zod schema `AstroConfigSchema` (the largest single schema definition in the config package) verbatim. [verify — exact exported name expected to be `AstroConfigSchema` in `packages/astro/src/core/config/schema.ts`]" (grep-findable: `AstroConfigSchema` is a literal exported identifier.)
4. **[D4 Architecture]** "Describe the file/directory organization of the Astro monorepo from a config-and-markup standpoint: the pnpm workspace layout (`packages/astro`, `packages/integrations/*`, `examples/*`), where `.astro` template fixtures live under `packages/astro/test/fixtures/`, and where the central config schema vs. the content-collection schema live."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Across all `astro.config.mjs`/`astro.config.ts` files in `examples/*`, surface duplication and naming-pattern structure: which configs share the same integration-registration shape (e.g. repeated `integrations: [...]` blocks, `output`/`adapter` pairings), and link each example config back to the package it configures. This is openly graph-favoring (config↔code linkage + near-duplicate detection across many small files), where plain grep returns matches but not the cross-file grouping."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*(defineConfig|defineCollection|defineMiddleware|AstroUserConfig).*"); D2->trace_path(function_name="defineConfig", direction="both") plus search_graph(relationship="IMPORTS"); D3->get_code_snippet(qualified_name=".*AstroConfigSchema"); D4->get_architecture(...); D5->search_code(pattern="integrations", path_filter="examples/") or search_graph(semantic_query=["integrations","adapter","output","config"]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/astro-graph.md`, `astro-explorer.md`, `astro-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the astro tier.

---

### 96. blade — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** monicahq/monica (`/tmp/bench/blade`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Monica is a ~20k-star Laravel personal-CRM that historically shipped a large, idiomatic Blade view tree (`resources/views/**/*.blade.php`) — substantial, real-world template usage matching the plan's "popular + idiomatic in-language" repo-selection criteria. (Note: recent Monica releases migrated significant UI to Vue/Inertia, so the live Blade surface must be confirmed against the pinned commit — see verify tags below.)

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level Blade structural definitions in `resources/views/layouts/skeleton.blade.php` [verify] — its `@section`/`@yield` names (e.g. a `content` yield [verify]) and any `@stack`/`@push` declarations — so we can see what blocks templates may override." (grep-findable: `@section`, `@yield`, `@stack` directives.)
2. **[D2 Relationship]** "Starting from the primary app layout (`resources/views/layouts/skeleton.blade.php` [verify], else the actual top-level layout for the pinned commit), what other Blade files does it pull in via `@include`/`@extends`/`@component`, and which child views `@extends` this layout? Show the include/extends graph in both directions."
3. **[D3 Retrieval]** "Retrieve the full source of a concrete, sizable Blade view in the people module — `resources/views/people/dashboard.blade.php` [verify] — so we can inspect its complete block structure verbatim." (No superlative claim; exact path resolved grep-first at execution.)
4. **[D4 Architecture]** "Describe the organization of the `resources/views/` directory: how are its sub-trees (e.g. `layouts/`, `partials/`, and the feature view folders that actually exist at the pinned commit — `people/`, `settings/`, `auth/` [verify]) arranged, and where do shared layouts vs. feature views live?"
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING — semantic/duplication) Find near-duplicate Blade partials and repeated markup blocks across the view tree (e.g. repeated form-field/`@include('partials...')` patterns), and surface config<->code links where a view name is referenced from a Laravel route or controller `view('...')` call. Cluster views by structural similarity."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition"/template-section, file="layouts/skeleton.blade.php"); D2->trace_call_path(direction="both", symbol=top-level layout includes/extends); D3->get_code_snippet(qualified_name="resources/views/people/dashboard.blade.php"); D4->get_architecture(path="resources/views"); D5->search_code/semantic_query(duplication + view<->controller `view()` references).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/blade-graph.md`, `blade-explorer.md`, `blade-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the blade tier.

**Authoring notes (fairness/ground-truth):**
- D1 & D3 target plain-text-grep-discoverable artifacts: Blade directives (`@section`, `@yield`) and a concrete `.blade.php` file path — both findable without a graph. (D1 maps a template language's closest "definition" analogue — overridable section/yield blocks — to dimension D1; it is intentionally grep-symmetric, not graph-only.)
- D2 is framed structurally on Blade's `@include`/`@extends`/`@component` composition (the closest template analogue to a call graph). If the indexer does not model template includes as edges, D2 degrades to "N/A — Blade composition not modeled as graph edges" and is excluded from this tier's mean.
- D4 uses architecture/structure framing over the real `resources/views/` tree.
- D5 is openly graph-favoring (similarity-clustering + view<->controller `view('...')` linkage) and is rolled up only within Group E.
- VERIFY DISCIPLINE: because Monica has been migrating Blade -> Vue/Inertia, every concrete path, the `content` yield example, the named feature sub-trees, and the "people module" view are `[verify]`-tagged draft placeholders. All are confirmed grep-first against the pinned commit at execution time; if a tagged artifact is absent at that commit, substitute the nearest real equivalent (or mark the affected question N/A) before the run.

---

### 97. just — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** casey/just (`/tmp/bench/just`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** just is a top-tier (~20k-star) command runner whose own repo ships a substantial, idiomatic root `justfile` plus `GRAMMAR.md` — the canonical reference corpus for the just/justfile language, making it the natural Group-E exemplar for the build/justfile slice of the repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level recipes defined in the repo's root `justfile` — e.g. `test`, `build`, `check`, `fmt`, `clippy`, `watch`, `install` [verify]. Each is a recipe header (`name:` at column 0) that is equally findable by plain `grep -nE '^[A-Za-z0-9_-]+:' justfile`."
2. **[D2 Relationship]** "Show the dependency relationships among recipes in the root `justfile`: which recipes list other recipes as prerequisites after the `:` (e.g. a `ci`/`check` recipe depending on `test`, `clippy`, `fmt` [verify]), and resolve the transitive prerequisite chain for the `ci` recipe. NOTE: justfile recipe prerequisites are a real intra-file relationship but are not modelled as CALLS edges in the graph — this is a fair, grep-confirmable question, not a graph-only one."
3. **[D3 Retrieval]** "Retrieve the full body of the `test` recipe from the root `justfile`. The recipe header is grep-findable (`grep -n '^test' justfile`); the body is the indented block beneath it. [verify]"
4. **[D4 Architecture]** "Describe how just-language artifacts are organized across the repo: the root `justfile`, the grammar/spec docs (`GRAMMAR.md`, `README.md`), and any example/fixture justfiles under `examples/` or test fixtures — i.e. the file/dir layout of justfile definitions vs. their documentation. [verify]"
5. **[D5 Cross-cutting/Semantic]** "(text/grep-favoring — see note) Find duplication across recipes: recipe bodies that invoke the same underlying `cargo` subcommand (e.g. multiple recipes wrapping `cargo test` / `cargo build`). NOTE: this is honestly a text-similarity task that plain `grep -n cargo justfile` answers well; the graph does NOT model justfile-recipe shell bodies as edges to Rust entry points such as `src/main.rs`, so no genuine config<->code graph link exists for this language. A truly graph-favoring D5 is largely N/A for a build-config language — this question is kept as the closest honest semantic angle (cross-recipe duplication) and is explicitly labelled text-favoring rather than rigged toward the graph."

**Expected graph tools (hint, not a script; many are uncertain for this language — verify):** D1->search_graph(label="Definition"/"Recipe" [verify — a dedicated Recipe label may not exist], project="just"); D2->grep/Read on the justfile for prerequisite chains (trace_call_path will NOT follow recipe prerequisites — they are not CALLS edges); D3->get_code_snippet(qualified_name [verify — exact QN scheme for justfile recipes is unconfirmed]) or Read the recipe block; D4->get_architecture(project="just"); D5->search_code/grep for shared `cargo` subcommands (text similarity, not a semantic-graph win).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/just-graph.md`, `just-explorer.md`, `just-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the just tier.

---

### 98. gotemplate — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** prometheus-community/helm-charts (`/tmp/bench/gotemplate`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred Helm chart monorepos (~5k stars, 40+ production charts); its `templates/*.tpl` files are large, idiomatic Go-template/Sprig code with heavy `define`/`include` reuse, making it a substantial and representative gotemplate corpus per the plan's popularity + idiomatic-substance selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the named template definitions (the `{{ define \"...\" }}` blocks) declared in `charts/prometheus/templates/_helpers.tpl` — e.g. confirm that `prometheus.fullname`, `prometheus.server.fullname`, `prometheus.serviceAccountName.server`, and `prometheus.namespace` are defined there. These names are plain-text grep-findable on the `define` lines (symmetric: a `grep '{{- define' _helpers.tpl` lists the same set)."
2. **[D2 Relationship]** "For the helper `kube-prometheus-stack.fullname` in `charts/kube-prometheus-stack/templates/_helpers.tpl`, show the cross-reference graph in both directions: which other named templates `include`/`template` it (callers — expect a rich set: `operator.fullname`, the `*.crname` and `*.serviceAccountName` helpers, etc.), and which named templates it itself references (callees). Treat `{{ include \"...\" }}` / `{{ template \"...\" }}` as the edge type. Note: this helper's body uses only Sprig built-ins (`trunc`/`default`/`printf`), so a correct answer reports MANY callers and ZERO callees — the test is whether the tool reports the empty outbound side honestly rather than hallucinating callees."
3. **[D3 Retrieval]** "Retrieve the full body of the single named template `kube-prometheus-stack.kubeVersionDefaultValue` (one of the largest, most conditional-heavy `define` blocks in `charts/kube-prometheus-stack/templates/_helpers.tpl`). Return exact start/end line boundaries of that `define`. The symbol itself is grep-findable on its `{{- define ... -}}` line (symmetric: grep locates the start; the test is precise body/boundary extraction)."
4. **[D4 Architecture]** "Describe the chart/file organization of the repo: how the top-level `charts/<chart-name>/` directories are structured, where each chart keeps its `templates/` (including `_helpers.tpl` partials vs rendered manifests), and how `Chart.yaml`/`values.yaml` sit relative to `templates/`. Report each chart's declared `Chart.yaml` `type:` and classify library vs application charts accordingly — in this repo every chart is `type: application` (no `type: library` charts exist), so the correct answer states that all charts are application charts rather than inventing a library/application split. [verify]"
5. **[D5 Cross-cutting/Semantic] (graph-favoring)** "Across all charts, find near-duplicate helper templates that follow the same naming-pattern convention — e.g. every `*.fullname`, `*.name`, and `*.serviceAccountName*` helper (`prometheus.fullname` vs `kube-prometheus-stack.fullname` vs `prometheus.server.fullname`, etc.). Cluster them by structural similarity to surface the copy-pasted fullname/name idiom and any drift between copies. Labeled graph-favoring: this is duplication / naming-pattern detection that plain grep can list but not cluster by similarity."

**Expected graph tools (hint, not a script):** D1->search_graph(label/define-name pattern over the `_helpers.tpl` file); D2->trace_call_path(name="kube-prometheus-stack.fullname", direction="both"); D3->get_code_snippet(qualified_name="kube-prometheus-stack.kubeVersionDefaultValue"); D4->get_architecture(scope=repo); D5->search_code/semantic_query(pattern=".*\\.(fullname|name|serviceAccountName).*" + similarity clustering).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/gotemplate-graph.md`, `gotemplate-explorer.md`, `gotemplate-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the gotemplate tier.

---

### 99. templ — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** a-h/templ (`/tmp/bench/templ`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** a-h/templ is the de-facto HTML templating language for Go (8k+ stars, widely adopted in the Go web ecosystem), and it is the canonical, idiomatic, substantial `.templ` corpus — its own `examples/`, docs, and test fixtures are written in templ — satisfying the plan's "popular + idiomatic + substantial in-language" repo-selection criterion for Group E template/markup evaluation.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level `templ` component definitions declared across the repo's `.templ` files (e.g. the `hello` component declared as `templ hello(name string)` in `examples/hello-world-static/hello.templ` and `examples/hello-world-ssr/hello.templ`, plus the example components under `examples/counter` and the docs/syntax fixtures), and confirm the grep-findable `templ.Component` interface and its `Render(ctx context.Context, w io.Writer) error` method are surfaced. [verify]"
2. **[D2 Relationship]** "For the templ runtime entry point `templ.Component.Render` (and the `templruntime`-aliased helpers in `github.com/a-h/templ/runtime` that generated code relies on), show both inbound callers and outbound callees so we can see how a rendered component fans out to child-component `Render` calls and to the runtime buffer/writer helpers. [verify]"
3. **[D3 Retrieval]** "Retrieve the full source of the parsed top-level template node in the templ parser type model — `parser.HTMLTemplate` (the struct a `templ name() { ... }` block parses into, defined in `parser/v2/types.go`, package `parser`) — by its qualified name. (Sibling whole-file node: `parser.TemplateFile`.) [verify]"
4. **[D4 Architecture]** "Describe the file/directory organization of the templ toolchain: how `parser/v2`, `generator`, `runtime`, `cmd/templ` (incl. `lspcmd`/`generatecmd`/`fmtcmd`), and the `.templ` example/fixture files are arranged and how the source-to-generated-Go pipeline is layered across those directories."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Graph-favoring: find duplication / naming-pattern and config<->code links — e.g. the recurring `*_templ.go` generated-file naming convention paired with each hand-written `.templ` source (`hello.templ` -> `hello_templ.go`), and structurally similar component-definition blocks across `examples/` — that plain text search cannot cluster by similarity. Use semantic/structural similarity, not literal grep."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*", label="Definition"/"templ component", project="templ"); D2->trace_call_path(qualified_name="...Component.Render", direction="both"); D3->get_code_snippet(qualified_name="parser.HTMLTemplate"); D4->get_architecture(project="templ"); D5->search_code(semantic) / search_graph(semantic_query="generated *_templ.go vs source .templ duplication & component naming patterns").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/templ-graph.md`, `templ-explorer.md`, `templ-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the templ tier.

---

### 100. liquid — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** Shopify/dawn (`/tmp/bench/liquid`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Shopify's flagship reference theme (the default storefront, ~5k+ stars) — the canonical, substantial, idiomatic Liquid codebase, with deep `{% render %}`/`{% section %}` include graphs across `sections/`, `snippets/`, and `templates/`, matching the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level Liquid template definitions in the theme — e.g. the snippet `snippets/card-product.liquid`, the section `sections/main-product.liquid`, and the root layout `layout/theme.liquid` [verify] — and identify which files declare a `{% schema %}` block (sections) versus pure render partials (snippets)."
2. **[D2 Relationship]** "Trace the include/reference graph for `snippets/card-product.liquid` [verify]: which sections/templates invoke it via `{% render 'card-product' %}` (inbound), and which snippets it in turn renders such as `price` and `card` (outbound)?"
3. **[D3 Retrieval]** "Retrieve the full source of the largest single template definition, the product section `sections/main-product.liquid` [verify], including its trailing `{% schema %}` settings block."
4. **[D4 Architecture]** "Describe the theme's file/directory organization: the role of `layout/`, `templates/`, `sections/`, `snippets/`, `assets/`, `config/`, and `locales/`, and how `templates/product.json` [verify] composes sections into a rendered page."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Find duplication and config<->markup links: which `settings.*` keys defined in `config/settings_schema.json` [verify] are actually referenced via `{{ settings.<key> }}` across sections/snippets, and which icon snippets (`snippets/icon-*.liquid`) form a near-duplicate naming/structure cluster?"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern=".*\.liquid"); D2->trace_call_path(name="card-product", direction="both"); D3->get_code_snippet(qualified_name="sections/main-product.liquid"); D4->get_architecture(); D5->search_code("settings.")/semantic_query("icon snippet duplication").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/liquid-graph.md`, `liquid-explorer.md`, `liquid-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the liquid tier.

---

### 101. jinja2 — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** sovereign/sovereign (`/tmp/bench/jinja2`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Sovereign is a widely-starred, idiomatic Ansible self-hosting platform whose role `templates/` are substantial `.j2` files rendering real service configs (Postfix, Dovecot, Nginx), giving a realistic Jinja2 corpus per the plan's "popular + substantial + idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level Jinja2 templates that render Postfix configuration — e.g. the `main.cf.j2` and `master.cf.j2` files under `roles/postfix/templates/` [verify] — and the `{% block %}` / `{% macro %}` definitions they declare [verify the graph emits Definition nodes for Jinja blocks/macros; fall back to search_code on `{% macro` / `{% block`]." (Grep-findable: the filenames `main.cf.j2` / `master.cf.j2` and the `{% macro`/`{% block` tags appear verbatim; this dimension must be answerable by plain grep, not graph-only.)
2. **[D2 Relationship]** "Trace the template-to-template reference graph: which `.j2` templates pull in shared fragments via `{% include %}`, `{% import %}`, or `{% extends %}` (e.g. a role template including a common header/footer fragment from `roles/common/templates/` [verify]), and what is the include/extends chain rooted at `roles/nginx/templates/nginx.conf.j2` [verify]? (These are genuine template reference edges — the graph-appropriate analogue of imports — and must resolve to other `.j2` files, not to Ansible YAML variables.)"
3. **[D3 Retrieval]** "Retrieve the full contents of a single large template verbatim — `roles/nginx/templates/nginx.conf.j2` [verify it is among the largest `.j2` files], including its `{% for %}` server-block loop." (Grep-findable filename; this dimension must be answerable by plain grep/find on the filename, not graph-only.)
4. **[D4 Architecture]** "Describe how the `.j2` templates are organized across the `roles/*/templates/` tree and how that directory layout mirrors the Ansible role structure."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Across roles, find near-duplicate template blocks — e.g. repeated TLS / `ssl_certificate` snippets [verify] copied between the nginx, dovecot and postfix templates — that a plain text scan would miss because they are paraphrased rather than identical. (Openly graph/semantic-favoring.) Note: the often-wanted `{{ placeholder }}`<->Ansible-variable linkage is cross-language — the Jinja2 extractor does not resolve a `.j2` placeholder to the YAML `vars`/`defaults` file that defines it [verify] — so treat any such linkage as best-effort/likely-unsupported, not a guaranteed graph capability."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern=".*\\.cf\\.j2", project="jinja2") [verify Definition nodes exist for Jinja macros/blocks; else search_code on `{% macro`/`{% block`]; D2->trace_path(name="nginx.conf.j2", direction="both") [verify include/import/extends edges between `.j2` files are emitted; else search_code on `{% include %}`/`{% extends %}`]; D3->get_code_snippet(qualified_name="roles/nginx/templates/nginx.conf.j2" [verify `.j2` files are snippet-addressable]); D4->get_architecture(project="jinja2"); D5->search_code / search_graph(semantic_query="duplicated ssl_certificate / TLS template blocks across roles").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/jinja2-graph.md`, `jinja2-explorer.md`, `jinja2-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the jinja2 tier.

---

### 102. prisma — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** prisma/prisma-examples (`/tmp/bench/prisma`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Prisma's own canonical example monorepo (15k+ stars) ships dozens of idiomatic `schema.prisma` files across ORM/database/deployment stacks — the most substantial, real-world corpus of Prisma schema for the language, matching the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level schema definitions declared in the example schemas — specifically the `datasource db`, `generator client`, and the `model` blocks (e.g. `User`, `Post` [verify], plus any `enum` declarations). These names are findable by plain grep (`grep -rn '^model ' --include=schema.prisma`); the question tests whether the same definitions surface as named symbols."
2. **[D2 Relationship]** "For the `Post` model [verify], resolve its cross-definition references: which models does it point to via relation fields carrying `@relation` (e.g. `author User` [verify]), and which models reference `Post` back? Treat `@relation` references as the edges between schema definitions (direction=both). NOTE: Prisma relations are the genuine structural backbone of a schema, so this dimension applies even though it is not a runtime call graph."
3. **[D3 Retrieval]** "Retrieve the full `model User` block [verify] from its `schema.prisma` — the single named definition with all its scalar fields, `@id`/`@unique` attributes, and relation fields. This block is also locatable by grep (`grep -n -A20 '^model User' schema.prisma`); the question tests precise boundary retrieval of one named definition."
4. **[D4 Architecture]** "Describe how the schema artifacts are organized across the example tree: how many `schema.prisma` files exist and how are they grouped by example category (the repo's top-level folders such as `databases/`, `orm/`, `typescript/`, `javascript/`, `deployment-platforms/` [verify] — confirm the actual taxonomy against the checked-out tree)? Show the file/dir layout of the schema layer."
5. **[D5 Cross-cutting/Semantic]** "N/A for this language. Prisma `.prisma` files are a declarative data/schema DSL with no runtime call graph, no import edges, and no semantic cross-cutting layer that the knowledge graph indexes. There is no symbol set here on which a graph would have a fair structural advantage over grep, so a forced D5 question would be either trivially greppable (recurring `model User` names → plain `grep -rc '^model User'`) or beyond what the graph computes for `.prisma` (e.g. near-duplicate-shape detection). Marking N/A rather than rigging a graph-favoring question keeps the chapter fair."

**Expected graph tools (hint, not a script):**
D1 → `search_graph(project="prisma", name_pattern="User|Post", label="Model"/"Definition" [verify label exists for .prisma])`;
D2 → `trace_path(function_name="Post", project="prisma", direction="both")` [verify that relation edges are modeled; if `.prisma` relations are not edges in the graph, this collapses to grep parity];
D3 → `get_code_snippet(qualified_name=".*User", project="prisma")`;
D4 → `get_architecture(project="prisma")` (no scope param; read the folder/cluster layout from the overview);
D5 → N/A (no graph tool — see question text).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/prisma-graph.md`, `prisma-explorer.md`, `prisma-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 N/A (excluded from Group E semantic rollup), Group E, the prisma tier.

---

### 103. hyprlang — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** end-4/dots-hyprland (`/tmp/bench/hyprlang`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred Hyprland dotfiles repos (illogical-impulse); its `.config/hypr/` tree is large, modular, and idiomatic hyprlang (variables, `source=` includes, named sections, `bind=` directives), matching the plan's "popular + substantial + idiomatic" repo-selection criteria for Group E config languages.

**Language note (config/data language):** hyprlang is a declarative configuration DSL (`key = value`, `section { }`, `$variable` definitions, `source = ...` text includes). It has no runtime call graph; the indexer models the config as sections/keys/variables, not as symbols with resolvable CALLS/IMPORTS edges. Dimensions that depend on a traversable reference graph (D2) are therefore handled honestly below — marked N/A with a reason rather than forced into an unnatural question that would overstate a graph capability that does not exist for this language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level hyprlang definitions in `.config/hypr/hyprland/general.conf` [verify] — specifically the named sections (e.g. `general { }`, `input { }`) and the assigned keys such as `gaps_in`, `gaps_out`, and `border_size` [verify]. These are plain `key = value` / `section { }` constructs that a plain `grep -rn 'general {'` / `grep -rn 'border_size'` finds the same symbols (grep-findable, plain-symbol)."
2. **[D2 Relationship]** "N/A — hyprlang is a config/data language. Its only cross-file relation is the `source = ...` text include, which is a literal path string a plain `grep -rn 'source ='` resolves just as well as any index; the generic code graph does not emit traversable IMPORTS/reference edges between hyprlang files, so there is no `CALLS`/`IMPORTS`/`HANDLES` relationship for the graph to walk. Forcing a `trace_path`-style include-graph question would not exercise a real graph capability and would not favor the graph over grep. (If any include edges happen to be present, treat them as best-effort, not scored.)"
3. **[D3 Retrieval]** "Retrieve the full body of the largest single named section in the keybind config — the keybinds block in `.config/hypr/hyprland/keybinds.conf` [verify] containing the `bind = ...` / `bindd = ...` directives. Name it and return the complete definition, not a fragment. The section header is grep-findable (`grep -rn 'keybinds.conf'` / the block delimiter) — the index must resolve the same symbol grep points at (grep-findable, plain-symbol)."
4. **[D4 Architecture]** "Describe the file/directory organization of `.config/hypr/`: the root `hyprland.conf`, the modular `hyprland/` subdirectory (general/decoration/keybinds/rules/env/execs [verify]), and the sibling top-level configs `hypridle.conf` and `hyprlock.conf` [verify]. How is concern-separation expressed across files?"
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Find duplicated / near-duplicate hyprlang variable definitions and naming-pattern clusters across the config tree — e.g. repeated `$`-variables (color/theme tokens like `$primary` [verify]) defined or overridden in more than one file, and config keys whose values reference variables defined elsewhere (config↔config linkage). Explicitly graph-favoring: this similarity + cross-file variable-reference clustering is hard for a single grep pattern. (If the index offers no semantic grouping over hyprlang, mark N/A with that reason — do not force it.)"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition"/"Section", file=".config/hypr/hyprland/general.conf"); D2->N/A (no traversable include/reference edges between hyprlang files; `source=` is a literal path grep resolves equally — see D2 note); D3->get_code_snippet(qualified_name="keybinds.conf:<keybind-section>" [verify]); D4->get_architecture(scope=".config/hypr"); D5->search_code/semantic_query(duplicate variables, naming clusters) or N/A if no semantic grouping over hyprlang.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/hyprlang-graph.md`, `hyprlang-explorer.md`, `hyprlang-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 recorded as N/A for hyprlang), D5 within Group E only, Group E, the hyprlang tier.

---

### 104. dotenv — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** motdotla/dotenv (`/tmp/bench/dotenv`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, ~30k-star reference implementation of the `.env` config format; small but idiomatic, with real `.env` fixtures + a thin JS loader, so it exercises Group E's "config<->code" framing precisely.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level KEY=VALUE definitions declared in the `tests/.env` fixture (e.g. `BASIC` [verify]), and the key declared in `tests/.env.multiline` (e.g. `MULTILINE` [verify]). These keys are plain text, so grep over `tests/.env*` must find them too — this is intentionally grep-symmetric."
2. **[D2 Relationship] — N/A** "A `.env` file is a flat list of independent `KEY=VALUE` assignments: entries have no calls, imports, or cross-references to one another, so the config language itself has no inter-entry relationship graph. (Loader call-traces like `parse`->`config`->`populate` live in the JavaScript of `lib/main.js`, not in the dotenv data; testing them here would score the JS extractor under a dotenv label, so D2 is honestly marked N/A rather than forced.)"
3. **[D3 Retrieval]** "Retrieve the full definition of the multi-line entry in `tests/.env.multiline` (the `MULTILINE` value spanning multiple physical lines) [verify]; the key name is grep-findable in that fixture, so a plain grep + read can recover it too — this is intentionally grep-symmetric."
4. **[D4 Architecture]** "Describe the file/dir organization of this repo: how the `.env*` test fixtures under `tests/` are grouped relative to the loader code in `lib/` (`main.js`, `cli-options.js`, `env-options.js`) and the CLI preload `config.js` [verify]."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Find naming-pattern duplication and config<->code links: which env keys defined across the `tests/.env*` fixtures are also referenced as string literals inside `lib/` or `tests/*.js`, and which fixtures contain near-duplicate key sets (e.g. quoting/whitespace variants of the same `BASIC` key). This is openly graph/semantic-favoring."

**Expected graph tools (hint, not a script):** D1->search_graph(label/definition pattern over `tests/.env*`); D2->N/A (no relationship dimension for flat config data); D3->get_code_snippet(qualified_name for the `MULTILINE` entry); D4->get_architecture(repo tree, `tests/` vs `lib/`); D5->search_code/semantic_query(key-name literals + fixture duplication).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/dotenv-graph.md`, `dotenv-explorer.md`, `dotenv-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 contributes no score — N/A for this config language), D5 within Group E only, Group E, the dotenv tier.

---

### 105. diff — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** void-linux/void-packages (`/tmp/bench/diff`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical Void Linux source-packages collection (~15k+ srcpkgs templates), the largest idiomatic corpus of unified-diff `.patch` files in public OSS — popular, substantial, and representative of how `diff` is used in practice (downstream patch sets under `srcpkgs/*/patches/`), matching the plan's criterion of selecting a high-star, language-idiomatic, real-world repo per group.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level definitions in `srcpkgs/gcc/patches/musl-ada.patch` [verify] — i.e. the file headers it introduces: every `+++ b/<path>` (and matching `--- a/<path>`) target file the patch modifies, which are the structural 'declarations' of a unified diff." (SYMMETRIC: grep-findable — the literal tokens `+++ b/` and `--- a/` exist verbatim in the file, so plain `grep '^+++ b/'` answers this too.)
2. **[D2 Relationship]** "N/A for `diff`. A unified-diff `.patch` file has no callee/caller, import, or include relationship to other symbols — it is a flat text artifact. In xbps-src any `srcpkgs/<pkg>/patches/*.patch` is auto-applied by convention during `do_patch`; templates do not name individual patch files, so there is no explicit reference edge the indexer can extract from a patch to its consuming `template`. Forcing a relationship question here would be artificial."
3. **[D3 Retrieval]** "Retrieve the unified-diff hunk for the file `gcc/gnatlib.mgi` (or the first `@@ ... @@` block) inside `srcpkgs/gcc/patches/musl-ada.patch` [verify] — return the exact diff text of that one named, grep-locatable hunk header and its body." (SYMMETRIC: grep-findable — the patch path and `@@` hunk header are literal tokens; `grep -n '^@@' <file>` locates the hunk without any size/ranking computation.)
4. **[D4 Architecture]** "Describe the directory/file organization of how `diff` artifacts are laid out across the repo: the `srcpkgs/<pkgname>/patches/*.patch` convention, how a package's patch directory sits beside its `template`, and the naming patterns (`musl-*.patch`, `fix-*.patch`, `CVE-*.patch` [verify]) that classify patch intent." (Note: this probes repo-layout convention, the only 'architecture' a flat patch corpus exhibits.)
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING — semantic/duplication) Find near-duplicate musl-compatibility patches across different packages — cluster patches whose hunks share the same upstream fix pattern (`#include <sys/...>` additions, `__GLIBC__` / `__MUSL__` guards) such as `musl-ada.patch` vs other `musl-*.patch` files under unrelated `srcpkgs/*/patches/` — surfacing copy-pasted patch logic that text grep cannot cluster by similarity. Graph-favoring because it requires semantic-similarity clustering, not literal-string matching." (Scope limited to semantic clustering; no patch↔template edge is assumed, since xbps-src does not record one.)

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", path~="srcpkgs/gcc/patches/musl-ada.patch") — or plain grep; D2->N/A (no relationship edges for flat patch artifacts); D3->get_code_snippet(qualified_name="srcpkgs/gcc/patches/musl-ada.patch::<named-hunk>") — or grep for the `@@` header; D4->get_architecture(scope="srcpkgs"); D5->search_code(semantic_query="musl compatibility include guard patch hunk")/search_graph(semantic_query=...).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/diff-graph.md`, `diff-explorer.md`, `diff-judged.json`.
**Aggregates into:** D1–D4 cross-group rollups (D2 counted as N/A for `diff`), D5 within Group E only, Group E, the diff tier.

---

### 106. wgsl — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** gfx-rs/wgpu (`/tmp/bench/wgsl`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** gfx-rs/wgpu is the de-facto reference WebGPU/WGSL implementation (the wgsl spec's own Naga lives here); its `examples/` and `naga/` trees carry dozens of substantial, idiomatic `.wgsl` shaders (structs, bindings, entry points, polyfills), making it the canonical large/popular repo for the shader slice of Group E per the plan's "popular + idiomatic + substantial" selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level WGSL definitions declared in `examples/features/src/boids/compute.wgsl` — the `Particle` and `SimParams` structs, the three `@group(0)` resource bindings (`params`, `particlesSrc`, `particlesDst`), and the `@compute @workgroup_size(64)` entry point `main`. These are plain-text-greppable (`struct `, `fn `, `@binding`)."
2. **[D2 Relationship]** "Show the cross-file reference structure of the WGSL inverse-matrix polyfills under `naga/src/back/wgsl/polyfill/inverse/` (e.g. `inverse_2x2_f16.wgsl`, `inverse_4x4_f32.wgsl`): how the per-dimension/per-precision `.wgsl` fragments group together and how the shadow shader's `fs_main` references the `fetch_shadow` helper and the `Light` struct it consumes [verify]."
3. **[D3 Retrieval]** "Retrieve the full body of the largest function in `examples/features/src/water/water.wgsl` — the 3D simplex-noise routine `snoise` (vec3 input, f32 output), spanning roughly lines 44–123 — exactly as written."
4. **[D4 Architecture]** "Describe how WGSL shaders are organized across the wgpu workspace: per-example self-contained shaders under `examples/features/src/<demo>/*.wgsl` (boids, water, shadow, mipmap, hello_triangle), engine-internal shaders under `wgpu/src/util/blit.wgsl` and `wgpu-core/src/timestamp_normalization/*.wgsl`, and Naga's generated-backend polyfills under `naga/src/back/wgsl/polyfill/`."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across all `.wgsl` files, surface the recurring `VertexOutput` struct + `@vertex vs_main` / `@fragment fs_main` naming convention and the duplicated structural pattern in the `inverse_NxN_{f16,f32}.wgsl` polyfill family — i.e. near-duplicate definitions that differ only by matrix dimension/precision. This is a duplication / naming-pattern / config<->code-link query best served by semantic similarity rather than a single grep."

**Expected graph tools (hint, not a script):** D1->search_graph(label="struct"/"function", path~"boids/compute.wgsl"); D2->trace_call_path(symbol="fetch_shadow", direction="both") + cross-file include grouping; D3->get_code_snippet(qualified_name=".../water.wgsl::snoise"); D4->get_architecture(scope="**/*.wgsl"); D5->search_code/semantic_query("VertexOutput vs_main fs_main / inverse polyfill duplication").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/wgsl-graph.md`, `wgsl-explorer.md`, `wgsl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the wgsl tier.

---

### 107. kdl — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** zellij-org/zellij (`/tmp/bench/kdl`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Zellij (~22k stars) is the canonical large real-world KDL consumer — its `zellij-utils/assets/config/default.kdl` config and the `zellij-utils/assets/layouts/*.kdl` family are substantial, idiomatic KDL that exercises nested nodes, templates and swap layouts, matching the plan's "popular + idiomatic + substantial in the target language" repo-selection criterion.

**Authoring note (config-language honesty):** KDL is a pure config/data language — it has no call graph, no imports, and no function semantics. D1/D3/D4 map cleanly onto KDL (nodes, node bodies, file/dir organization). D2 (Relationship) is interpreted as *structural cross-file reference* (named-template definition → reuse), which genuinely exists in this repo; it is NOT a call path, so any caller-tracing tool is the wrong instrument here. D5 is the one dimension that legitimately favors the graph (similarity clustering + config-string→Rust-symbol resolution). No dimension is forced into an unnatural shape.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "In `zellij-utils/assets/config/default.kdl`, list the top-level KDL nodes that define configuration sections — specifically locate the `keybinds`, `plugins`, and `load_plugins` nodes and the per-mode keybind blocks (`normal`, `locked`, `pane`, `resize`). These are plain, grep-findable node names (symmetric: a `grep -nE '^\s*(keybinds|plugins|load_plugins|normal|locked|pane|resize)\b' default.kdl` finds them too)."
2. **[D2 Relationship — structural cross-file reference, NOT a call path]** "In the layout family, the named `tab_template name=\"ui\"` is defined inside the `.swap.kdl` swap layouts (e.g. `default.swap.kdl`, `strider.swap.kdl`) and its `tab_template`/`pane_template` constructs are reused across the `swap_tiled_layout`/`swap_floating_layout` blocks within those files. Identify which layout files define a `tab_template` and how its `ui` body (the borderless `tab-bar`/`status-bar` panes) is reused across the swap variants. Note: KDL has no call graph — this is a name→reuse reference, gradable by grep on the `tab_template` name too, so it is authored symmetrically." [verify]
3. **[D3 Retrieval]** "Retrieve the full body of the `keybinds` node from `zellij-utils/assets/config/default.kdl` — the single largest definition in the file, containing all per-mode `bind` blocks and `SwitchToMode`/`NewPane` actions. The node name `keybinds` is grep-findable; the task is returning its exact, complete body."
4. **[D4 Architecture]** "Describe the organization of KDL assets under `zellij-utils/assets/`: how the `config/` directory (single `default.kdl`) relates to the `layouts/` directory (the `classic`/`compact`/`strider`/`default` families plus their `.swap.kdl` counterparts, and extras like `welcome.kdl`, `no-plugins.kdl`, `disable-status-bar.kdl`). What is the file/dir grouping convention?"
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Two distinct facts to connect: (a) across `zellij-utils/assets/layouts/*.kdl`, the bare-string `plugin location=\"tab-bar\"` / `plugin location=\"status-bar\"` blocks recur near-identically (cluster the duplicates); (b) the matching `zellij:`-prefixed aliases (`tab-bar location=\"zellij:tab-bar\"`, `status-bar location=\"zellij:status-bar\"`) are declared in the `plugins` block of `config/default.kdl`. Resolve each `zellij:` plugin URL and each keybind action string (e.g. `SwitchToMode`, `NewPane`) to the Rust symbol that implements it in `zellij-server`. Explicitly graph-favoring: relies on similarity clustering plus config-string→code resolution across the KDL/Rust boundary that plain grep cannot rank." [verify]

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="keybinds|plugins|load_plugins|normal|locked|pane|resize", label="Definition"); D2->search_graph(name_pattern="tab_template|pane_template") then query_graph for same-name reuse across files (NOT trace_call_path — KDL has no CALLS edges); D3->get_code_snippet(qualified_name="default.kdl::keybinds"); D4->get_architecture(scope="zellij-utils/assets"); D5->search_code(semantic_query="duplicated layout pane/plugin blocks; zellij: plugin alias -> Rust handler in zellij-server").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/kdl-graph.md`, `kdl-explorer.md`, `kdl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the kdl tier.

---

### 108. json5 — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** json5/json5 (`/tmp/bench/json5`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical reference implementation of the JSON5 spec (~6k GitHub stars, ~70M weekly npm downloads); it dogfoods its own format via a real `package.json5`, making it an idiomatic, substantial Group-E config/data corpus for the plan's "popular + representative of the language" selection criteria.

**Honesty note (config/data language):** JSON5 is a pure data/config format. A `.json5` file has no functions, calls, or imports, so the code graph's relationship edge types (CALLS/IMPORTS/HANDLES/IMPLEMENTS) do not apply to it. Dimensions that depend on those edges are marked **N/A** with a reason rather than forced into unnatural graph-favoring questions. Only D1 (top-level keys as "definitions") and D3 (retrieve a definition block) map cleanly; D4 maps via the repo's directory/file structure.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Enumerate the top-level keys defined in `package.json5` (e.g. `name`, `version`, `main`, `module`, `scripts`, `dependencies`, `devDependencies`) and confirm each is a top-level definition in the file." — grep-findable identifiers (`"scripts"`, `"devDependencies"`) appear verbatim in the source, so this is answerable by plain grep as well as the graph (symmetric authoring).
2. **[D2 Relationship]** "**N/A** — JSON5 is a data/config format with no inter-symbol relationships the code graph models. The `main` key naming `lib/index.js` is a string value, not a CALLS/IMPORTS edge; the graph builds no config→target edge for it. (A grep for `"main"` resolves the value directly; there is no graph relationship to trace.)"
3. **[D3 Retrieval]** "Retrieve the full `scripts` block from `package.json5` verbatim (the largest single nested object in the file), including the `build`, `test`, `lint`, and `coverage` script entries." — `scripts` is the largest grep-findable object literal in the config, so it is retrievable by plain grep/`sed` on the file as well as by the graph (symmetric authoring).
4. **[D4 Architecture]** "Describe the repository's file/directory organization: the split between `lib/` (source: `index.js`, `parse.js`, `stringify.js`, `cli.js`, `unicode.js` [verify]), `test/` (fixtures + specs), `dist/` (generated bundle [verify]), and the root-level build/config files (`package.json`, `package.json5`, `.babelrc` [verify], `rollup.config.js` [verify])." — structural/organizational, answerable from the directory tree (grep/`find`) and from the graph's CONTAINS_FOLDER/CONTAINS_FILE edges.
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring; partial N/A) Detect the duplication/redundancy between `package.json` and `package.json5`: both encode the same package manifest (one in JSON, one dogfooding JSON5). Identify keys present in one file but not the other. **N/A caveat:** there is no graph config→code edge from `main`/`bin` to `lib/` targets for a data file — that part is not resolvable by the graph and is excluded. The answerable graph-favoring part is similarity/near-duplicate detection across the two manifest files, which a plain grep cannot summarize (grep finds keys but cannot diff two structured docs for semantic equivalence)."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", file="package.json5") or grep `^\s*\w\+:`; D2->N/A (no relationship edge; grep the `"main"` value if needed); D3->get_code_snippet(qualified_name="package.json5::scripts") or grep/`sed` the `scripts` block; D4->get_architecture(scope="repo") or `find . -maxdepth 2`; D5->search_code/semantic similarity across `package.json` vs `package.json5` (manifest near-duplicate diff).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/json5-graph.md`, `json5-explorer.md`, `json5-judged.json`.
**Aggregates into:** D1, D3, D4 cross-group rollups (D2 excluded as N/A); D5 within Group E only; Group E; the json5 tier.

---

### 109. jsonnet — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** grafana/jsonnet-libs (`/tmp/bench/jsonnet`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Grafana's flagship Jsonnet monorepo — thousands of stars, the canonical real-world corpus of idiomatic `.libsonnet` libraries, mixins, and `jsonnetfile.json` manifests, matching the plan's "popular + substantial + idiomatic" repo-selection criteria.

> **Indexing note (fairness):** jsonnet is parsed via tree-sitter only — there is **no LSP** for it in this tool (no Deep-dive X/S block applies). The jsonnet lang spec captures `import`/`importstr`, `functioncall`, `conditional`, and `local_bind` nodes, but its only definition node type is `anonymous_function` — there is **no name resolution for object fields or local binds**. Consequently the public API surface of a `.libsonnet` (object fields like `obj.foo(args)::` and top-level `local` functions) is **not** indexed as named, qualified definitions. D1/D3 below are therefore authored to be answerable by **plain grep** (literal `fieldname(args)::` / `local name =` lines), and the expected-tool hints are honest that the graph does not trivially win these.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level field/function members exported by `ksonnet-util/util.libsonnet` — e.g. `serviceFor`, `configMapVolumeMount`, `manifestYaml`, `rbac`, `resourcesRequests` [verify] — i.e. the public API a consumer pulls in via `(import 'ksonnet-util/util.libsonnet')`. (These are object fields, found equally well by grepping `^\s*\w+(.*)::` — the graph does not index them as named defs.)"
2. **[D2 Relationship]** "Starting from a library's `main.libsonnet` (or a mixin's `mixin.libsonnet` [verify]), show the cross-file `import`/`importstr` reference graph: which `.libsonnet` files it pulls in and which downstream libraries those in turn reference (both inbound consumers and outbound dependencies)."
3. **[D3 Retrieval]** "Retrieve the full definition of the `dashboard(title, uid, datasource, datasource_regex)` constructor in `grafana-builder/grafana.libsonnet` [verify] — return exactly that one named field/function, not the surrounding file. (The `dashboard.new` form belongs to grafonnet, a different repo; grafana-builder exposes `dashboard(...)` as a function. The field name is a literal grep target.)"
4. **[D4 Architecture]** "Describe the directory/file organization of the repo: how libraries are grouped (one folder per library/mixin with `main.libsonnet`/`mixin.libsonnet` + `jsonnetfile.json` + optional `config.libsonnet`), and how `jsonnetfile.json` / `jsonnetfile.lock.json` manifests map the dependency layout."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Surface duplication and naming-pattern structure across mixins: which libraries repeat the same `_config` / `_images` object convention, and which entrypoints expose the near-identical `prometheusAlerts` / `prometheusRules` / `grafanaDashboards` triad — a config-pattern similarity query plain grep can locate but cannot rank by structural similarity."

**Expected graph tools (hint, not a script):** D1->grep-competitive; graph fallback search_graph(name_pattern=".*util.*", project="jsonnet") only finds the file/import nodes, not field defs — expect the explorer (grep) to match here. D2->trace_call_path(qualified_name=".../main.libsonnet", direction="both") over IMPORTS edges (graph's real strength). D3->grep-competitive; get_code_snippet cannot resolve `dashboard` as a qualified def (jsonnet fields aren't named in the graph) — expect grep parity. D4->get_architecture(project="jsonnet"). D5->search_code/semantic_query("_config object + prometheusAlerts/prometheusRules/grafanaDashboards triad across mixins").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/jsonnet-graph.md`, `jsonnet-explorer.md`, `jsonnet-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the jsonnet tier.

---

### 110. ron — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** ron-rs/ron (`/tmp/bench/ron`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** RON (Rusty Object Notation) is the canonical, widely-adopted (~3.9k-star) Rust data/config serialization format; its own implementation is compact yet idiomatic, making it a representative "config/data format" subject for Group E per the plan's popularity-plus-idiomatic repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Enumerate the public top-level definitions of the RON data model and entry-point API: the `Value` enum and its variants in `src/value/mod.rs`, the `Number` type (`src/value/number.rs`), the `Map` type (`src/value/map.rs`), and the crate-level `from_str` / `to_string` functions re-exported in `src/lib.rs`." (All are grep-findable identifiers, e.g. `grep -rn 'pub enum Value' src/`.)
2. **[D2 Relationship]** "Starting from the public `from_str` entry point, trace the parse path through `Options::from_str` and into the `Deserializer` (`src/de/mod.rs`) and the low-level `Parser` (`src/parse.rs`): which functions does `from_str` reach, and which callers/tests reach it inbound?"
3. **[D3 Retrieval]** "Retrieve the full definition of the `Value` enum from `src/value/mod.rs` — the central type of the data model, including every variant (`Bool`, `Char`, `Map`, `Number`, `Option`, `String`, `Bytes`, `Seq`, `Unit`)."
4. **[D4 Architecture]** "Describe the module/file organization of the crate: how the serialize side (`src/ser/mod.rs`), deserialize side (`src/de/mod.rs`), shared data model (`src/value/` — `mod.rs`, `map.rs`, `number.rs`, `raw.rs`), error types (`src/error.rs`), and configuration (`src/options.rs`, `src/extensions.rs`) are split into directories and how they depend on each other."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the cross-cutting links between the `Extensions` bitflags config surface (`src/extensions.rs`) and the parser/deserializer code that consumes each extension (e.g. `implicit_some`, `unwrap_newtypes`, `unwrap_variant_newtypes`) — i.e. config-flag → code-path coupling — and surface naming-pattern duplication between the `ser` and `de` error-handling code that plain text search would scatter."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*Value.*|.*from_str.*", label="Class/Function"); D2->trace_call_path(name="from_str", direction="both"); D3->get_code_snippet(qualified_name="ron::value::Value"); D4->get_architecture(project="ron"); D5->search_code/semantic_query(["extension","implicit_some","deserializer flag"]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/ron-graph.md`, `ron-explorer.md`, `ron-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the ron tier.

---

### 111. thrift — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** apache/thrift (`/tmp/bench/thrift`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Apache Thrift is the canonical, high-star (~10k) cross-language IDL/RPC toolchain; its `.thrift` schema files are large, idiomatic, and exercised across dozens of language bindings — the most representative substantial Thrift IDL corpus, matching the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level Thrift definitions (`struct`, `service`, `exception`, `enum`, `const`, `typedef`) declared in the test IDL — e.g. the `Xtruct` and `Xtruct2` structs and the `ThriftTest` service in `test/ThriftTest.thrift`. Are they surfaced as discoverable definitions?" (grep-findable: `grep -nE '^(struct|service|exception|enum)' test/ThriftTest.thrift`)
2. **[D2 Relationship]** "Trace cross-file `include` references between IDL files: e.g. `tutorial/tutorial.thrift` does `include "shared.thrift"` and references `shared.SharedStruct` / `shared.SharedService`; the test suite also chains includes (e.g. `include "Include.thrift"` / `include "Recursive.thrift"` [verify]). Resolve the include graph and show which definitions one file pulls from another."
3. **[D3 Retrieval]** "Retrieve the full definition of the `ThriftTest` service from `test/ThriftTest.thrift` — the largest service in the test suite, including methods such as `testString`, `testStruct`, `testMapMap`, and `testException`. Return its exact span verbatim." (grep-findable anchor: `grep -n 'service ThriftTest' test/ThriftTest.thrift`)
4. **[D4 Architecture]** "Describe the IDL/schema file & directory organization: the `test/` cross-language conformance schemas (`ThriftTest.thrift`, `DebugProtoTest.thrift`), the `tutorial/` schemas (`tutorial.thrift`, `shared.thrift`), and how the `compiler/cpp/` parser tree relates to these. Is the file/folder grouping recovered?"
5. **[D5 Cross-cutting/Semantic — GRAPH-FAVORING]** "(Graph-favoring; excluded from grep baseline parity.) Find naming-pattern / duplication links across the IDL corpus: `namespace` declarations repeated across files (`namespace cpp ...`, `namespace java ...`, `namespace py ...`) and structurally near-duplicate definitions (e.g. `Xtruct` vs `Xtruct2`; `SharedStruct` defined in `shared.thrift` and reused via `tutorial.thrift`). Surface the cross-file clusters of repeated `namespace` targets and the near-duplicate struct families across the corpus."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition"/"struct"/"service", name_pattern=".*Xtruct.*|ThriftTest"); D2->trace_call_path(direction="both") over IMPORTS/include edges (or query_graph on INCLUDES edges between .thrift files); D3->get_code_snippet(qualified_name="ThriftTest"); D4->get_architecture(scope=test/ + tutorial/); D5->search_code (namespace repetition) + search_graph(name_pattern=".*Xtruct.*|.*Shared.*") for near-duplicate struct families.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/thrift-graph.md`, `thrift-explorer.md`, `thrift-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the thrift tier.

---

### 112. capnp — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** capnproto/capnproto (`/tmp/bench/capnp`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Reference implementation of Cap'n Proto; its `.capnp` files (schema meta-schema, RPC protocol) are the canonical, substantial, idiomatic corpus of the schema language — exactly the popular + representative repo the plan's selection criteria call for.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level struct/interface/enum definitions declared in `c++/src/capnp/rpc.capnp` — e.g. `Message`, `Call`, `Return`, `Bootstrap`, `Finish`, `Resolve` — and report which file/line each is declared at." (Grep-findable: each is a literal `struct`/`interface`/`enum` token at column 0.)
2. **[D2 Relationship]** "Trace the cross-file `using import` references for `c++/src/capnp/persistent.capnp`: which schema files does it pull in (e.g. `c++.capnp` for the `namespace` annotation [verify]) and which other `.capnp` files import `persistent.capnp` in turn?"
3. **[D3 Retrieval]** "Retrieve the full definition of the `Node` struct in `c++/src/capnp/schema.capnp` (the largest definition in the meta-schema, including its `union` of `struct`/`enum`/`interface`/`const`/`annotation` variants and `nestedNodes`)." (Grep-findable: `struct Node` is a literal token; the block is locatable from there.)
4. **[D4 Architecture]** "Describe how the `.capnp` schema files are organized under `c++/src/capnp/` — the core meta-schema (`schema.capnp`), the RPC layer (`rpc.capnp`, `rpc-twoparty.capnp`), capability/persistence (`persistent.capnp`), the C++ codegen annotations (`c++.capnp`), and the JSON compat layer (`compat/json.capnp`) — and how they group into logical layers."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Across all `.capnp` files, find duplicated or near-duplicated structural patterns: e.g. structs that re-declare an `id @0 :UInt64` identifier field, and the recurring `@0xHEXID` file-id naming convention. (Graph-favoring: relies on semantic/structural similarity across files, not a single literal text match.) [verify: whether the graph also exposes schema-struct -> generated-C++-type linkage via the `$Cxx.namespace` / `namespace` annotation; if no such edge exists, treat that sub-part as N/A rather than scoring it.]"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Struct|Interface|Enum", path~"rpc.capnp"); D2->trace_path(name="persistent.capnp", direction="both", edge="IMPORTS") [verify IMPORTS traversal is supported; else query_graph Cypher on IMPORTS edges]; D3->get_code_snippet(qualified_name=".*schema.capnp.*Node"); D4->get_architecture(scope="c++/src/capnp"); D5->search_code/query_graph("duplicate field patterns; namespace-annotation usage").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/capnp-graph.md`, `capnp-explorer.md`, `capnp-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the capnp tier.

---

### 113. properties — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** spring-projects/spring-petclinic (symlink java) (`/tmp/bench/properties`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** The canonical Spring Boot reference app (~7k stars), it carries idiomatic, substantial `.properties` usage — a real `application.properties` plus a full `messages*.properties` i18n bundle family — matching the plan's "popular + idiomatic + non-trivial" repo-selection criteria for the properties tier. The symlink reuses the already-cloned java repo.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List top-level configuration keys defined in `src/main/resources/application.properties` — e.g. `spring.thymeleaf.mode` (`HTML`), `spring.jpa.hibernate.ddl-auto` (`none`), `spring.jpa.open-in-view` (`false`), and `spring.messages.basename` (`messages/messages`). Which file declares each, and what is the declared value?" (All four keys are plain, grep-findable literal strings in `application.properties`; this dimension must be answerable by plain grep too, not graph-only.)
2. **[D2 Relationship]** "Cross-file reference structure: `spring.messages.basename` in `application.properties` names the `messages/messages` bundle base. Which `messages*.properties` files (default + locale variants such as `messages_de`, `messages_es`, `messages_ko`) make up that bundle family, and which keys are shared across all of them? (Structural cross-file/cross-bundle framing. NB: `.properties` has no call graph, and `basename`→bundle resolution is a Spring-framework convention, not a generic code-graph edge; so this is a file→file / key-overlap reference web, surfaced via CONTAINS_FILE + key-set comparison, not a `CALLS`/`HANDLES` traversal. A reviewer could partly recover it with grep over the `messages/` directory.)"
3. **[D3 Retrieval]** "Retrieve the full key/value definition for the i18n key `welcome` from `src/main/resources/messages/messages.properties` (the default-locale bundle; the verified value is `welcome=Welcome`)." (`welcome` is a single, real, grep-findable key — symmetric with text search.)
4. **[D4 Architecture]** "Describe the file/directory organization of the properties configuration: where the single root `application.properties` sits relative to the `messages/` bundle directory, and how the locale variants (`messages_de`, `messages_en`, `messages_es`, `messages_fa`, `messages_ko`, `messages_pt`, `messages_ru`, `messages_tr`) are grouped under `src/main/resources/`." (Structural framing.)
5. **[D5 Cross-cutting/Semantic]** "GRAPH-FAVORING: Across the `messages*.properties` bundle family, identify message keys that are duplicated with identical values across locale files versus keys present only in the default bundle (e.g. `required`, `notFound`, `duplicate`, `nonNumeric`, `typeMismatch.date`), and surface the naming-pattern / key-set-overlap structure of the bundle. (This duplication / naming-pattern analysis across many files is where the graph is expected to beat plain grep.) A config↔code link from `spring.messages.basename` to an explicit `MessageSource`/`@Value` injection point is NOT expected here — spring-petclinic auto-configures `MessageSource` via Spring Boot and contains no explicit `MessageSource` reference in Java [verify: 0 matches in repo] — so any such config-to-code edge is out of scope and must not be credited as a graph win."

**Expected graph tools (hint, not a script):** D1->search_graph(label="ConfigKey", name_pattern="spring\\.(thymeleaf|jpa|messages).*", project="properties") [verify the graph emits ConfigKey/Definition nodes for `.properties` keys; fall back to search_code on the literal keys]; D2->query_graph on CONTAINS_FILE over the `messages/` bundle + key-set comparison (fallback: search_code, since `.properties` has no call/reference graph); D3->get_code_snippet(qualified_name="messages/messages.properties::welcome" [verify .properties keys are snippet-addressable]); D4->get_architecture(scope="src/main/resources", project="properties"); D5->search_code/semantic_query("duplicated i18n message keys across locale bundles + key-set overlap").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/properties-graph.md`, `properties-explorer.md`, `properties-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the properties tier.

---

### 114. sshconfig — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** mathiasbynens/dotfiles (`/tmp/bench/sshconfig`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred dotfiles repos on GitHub (idiomatic, widely-forked); its INI/stanza-style config files (`.gitconfig`, plus any `~/.ssh/config`-style host blocks) are the closest substantial, real-world ssh_config-family material the selection criteria allow — popular + structurally representative of host/section-keyed config.

**Caveat (authoring honesty):** This is a Bash-centric dotfiles repo, NOT a repo built around a canonical `~/.ssh/config`. Confirmed against the pinned commit: there is **no `~/.ssh/config`** and **no `IdentityFile`/`Host`/`HostName` ssh_config stanzas** in this repo, so ssh_config-native symbols are unavailable. The closest stanza/section-keyed material is `.gitconfig` (real `[alias]`, `[core]`, `[color]`, `[url "..."] insteadOf`, `[init]`, etc.). All ssh_config-family symbols below are tagged `[verify]` and are confirmed absent; questions that depend on ssh_config-only semantics are written `N/A` with a reason and excluded from the mean rather than posed as forced questions.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level stanza/section headers defined across the repo's INI/stanza config files — i.e. the bracketed sections of `.gitconfig` (`[alias]`, `[core]`, `[color]`, `[commit]`, `[diff]`, `[push]`, `[init]`, and the several `[url \"...\"]` blocks), plus any `Host` blocks in a `~/.ssh/config` [verify] (confirmed absent in this repo). Are all section/host headers enumerated?" (All cited `.gitconfig` headers are plain-text grep-findable via `^\[`; `^Host ` returns nothing here.)
2. **[D2 Relationship]** "Within `.gitconfig`, identify the URL-rewrite relationships that connect different remote forms: the `[url \"git@github.com:\"] insteadOf` (and the `git://`, gist variants) blocks rewrite one URL scheme into another that the repo's git operations rely on. Map those rewrite edges (source pattern → target). NOTE: cross-FILE include/reference edges (e.g. a git `[include] path = ...` or `IdentityFile ~/.ssh/...` [verify]) are **N/A — confirmed absent**: this `.gitconfig` has no `[include]` directive, no `IdentityFile`, and no reference into files managed by `bootstrap.sh`, so the only real relationship edges are the intra-file `insteadOf` rewrites." (`[url \"...\"]` and `insteadOf` are grep-findable anchors.)
3. **[D3 Retrieval]** "Retrieve the full text of the single largest definition block in the config set — the `[alias]` section of `.gitconfig` (the largest section, ~27 alias entries). Return the complete block with all its entries verbatim." (`[alias]` in `.gitconfig` is a real, grep-findable anchor: `^\[alias\]`.)
4. **[D4 Architecture]** "Describe how configuration is organized across the repo: which dotfiles hold INI/stanza config (`.gitconfig`, `.editorconfig`) vs shell config (`.bashrc`, `.bash_profile`, `.aliases`, `.exports`, `.functions`, `.bash_prompt`), how `bootstrap.sh` symlinks/copies them into `$HOME`, and confirm that no `~/.ssh/` material sits in the repo [verify]. Give the file/dir layout." (Structural framing permitted for D4.)
5. **[D5 Cross-cutting/Semantic — GRAPH-FAVORING]** "Find config↔code and duplication links a grep cannot cheaply surface: which `.gitconfig` `[alias]` shortcuts or `[url] insteadOf` rewrites correspond semantically to helpers in `.functions`/`.aliases` (e.g. a git workflow alias mirrored by a shell function), and flag near-duplicate or naming-pattern-clustered config entries across files. Explicitly graph/semantic-favoring (config→code association + similarity), not a literal-string match."

**Expected graph tools (hint, not a script):** D1->search_graph(section headers, name_pattern="^\\["); D2->trace_call_path / query_graph(intra-file insteadOf rewrite edges; cross-file refs confirmed N/A); D3->get_code_snippet(qualified_name=".gitconfig#[alias]"); D4->get_architecture(config vs shell layout, bootstrap symlinks); D5->search_code/semantic_query(config↔code association + duplicate-cluster detection).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/sshconfig-graph.md`, `sshconfig-explorer.md`, `sshconfig-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the sshconfig tier.

---

### 115. bibtex — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** JabRef/jabref (`/tmp/bench/bibtex`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** JabRef is the most-starred open-source BibTeX/reference manager on GitHub and ships a large, idiomatic corpus of real `.bib` databases (the example library `jablib/src/main/resources/Chocolate.bib` plus many parser test fixtures under `jablib/src/test/resources/`), making it the substantial, widely-recognized bibtex repo our repo-selection criteria call for.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level BibTeX entry definitions in the example library `jablib/src/main/resources/Chocolate.bib` — i.e. each `@article{...}` declaration and its citation key (this file is an all-`@article` bibliography of ~16 entries; e.g. `Corti_2009`, `Di_Renzo_2012`, `Fulton_1969`). Are every entry type and citation key enumerated?"
2. **[D2 Relationship]** "For an entry that uses `crossref` in the JabRef test fixture `jablib/src/test/resources/testbib/crossref.bib`, resolve the cross-entry reference: which parent entry key does the child point to (e.g. `DBLP:conf/wicsa/ZimmermannWKG15 -> DBLP:conf/wicsa/2015`), and does the tool surface that `crossref -> citationkey` link?"
3. **[D3 Retrieval]** "Retrieve the full text of one of the largest entry definitions in `jablib/src/main/resources/Chocolate.bib` (an entry with the most fields, e.g. `Di_Renzo_2012`) exactly as written, including all field assignments. If several entries tie on field count, pick the first one in file order."
4. **[D4 Architecture]** "Describe how `.bib` data files are organized in the repository: the example/demo libraries under `jablib/src/main/resources/` vs. the parser test fixtures under `jablib/src/test/resources/` (e.g. the `testbib/` directory), and how `.bib` files are grouped by directory and module (`jablib` vs `jabgui`)."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across all `.bib` files in the repo, find duplicate or near-duplicate entries (same DOI/title under different citation keys) and recurring `@string`/abbreviation patterns; also link a `.bib` field name (e.g. `journaltitle`, `crossref`) to the Java field constant that defines it in `jablib/src/main/java/org/jabref/model/entry/field/StandardField.java` (config<->code). Plain grep cannot cluster near-duplicates or join `.bib` fields to code symbols."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", file=".*\\.bib"); D2->trace_call_path(direction="both") over crossref references in `crossref.bib`; D3->get_code_snippet(qualified_name="Chocolate.bib:<largest-entry-key>"); D4->get_architecture(); D5->search_code/semantic_query(".*@string.*" / near-duplicate clustering + field<->`StandardField` code join).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/bibtex-graph.md`, `bibtex-explorer.md`, `bibtex-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the bibtex tier.

---

### 116. starlark — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** bazelbuild/rules_go (`/tmp/bench/starlark`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Canonical, widely-depended-on Bazel ruleset (~1.5k stars, the de-facto Go support for Bazel) written with a large idiomatic Starlark `.bzl` surface (≈40% of the repo); substantial multi-file rule/provider/toolchain definitions exercise structural queries on a real config/build language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the top-level rule and macro definitions for building Go code: where are `go_library`, `go_binary`, and `go_test` defined (their `rule(...)` declarations / wrapper-macro `go_binary_macro`), and how does `go/def.bzl` re-export them via `load(...)` from `go/private/rules/`?" (grep-findable: `go_library`, `go_test`, and the `load(...)` lines in `go/def.bzl` are plain text.)
2. **[D2 Relationship]** "Trace the `load(...)` / reference graph for the `GoInfo` and `GoArchive` providers (declared in `go/private:providers.bzl`): which rule implementations (e.g. `_go_library_impl`) load these providers, require them on `deps`/`embed`, and return them — both inbound and outbound?"
3. **[D3 Retrieval]** "Retrieve the full definition of the `go_context` function in `go/private/context.bzl` — the helper that assembles toolchain/SDK context for rule implementations — including its parameters and the struct it returns." (grep-findable: `def go_context(` is a literal symbol.)
4. **[D4 Architecture]** "Describe the file/directory organization of the ruleset: how are public entrypoints (`go/def.bzl`, `go/deps.bzl`, `go/extensions.bzl`) separated from the `go/private/` implementation tree (`rules/`, `tools/`, `providers.bzl`, `context.bzl`), and where do toolchain definitions live (the `go/toolchain/` directory and `go/private:go_toolchain.bzl`)?"
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Find duplication / naming-pattern clusters across rule implementations: which `_*_impl` implementation functions share near-identical attribute-handling or action-registration boilerplate, and how do the public macro/rule names re-exported by `go/def.bzl` map back to their private `rule()` objects in `go/private/rules/` (config↔code link) — the similarity/cross-reference ranking grep cannot produce?"

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*go_(library|binary|test).*", label="Definition"); D2->trace_call_path(name="GoInfo", direction="both"); D3->get_code_snippet(qualified_name="go_context"); D4->get_architecture(scope="go/"); D5->search_code/semantic_query("rule impl attribute boilerplate duplication", then search_graph name_pattern=".*_impl").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/starlark-graph.md`, `starlark-explorer.md`, `starlark-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the starlark tier.

---

### 117. bicep — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** Azure/bicep-registry-modules (`/tmp/bench/bicep`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, high-star Azure Verified Modules (AVM) monorepo — thousands of idiomatic, production-grade `.bicep` modules with deep cross-module references, satisfying the plan's "popular + substantial + idiomatic" repo-selection criterion for the bicep tier.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "In `avm/res/storage/storage-account/main.bicep`, list the top-level definitions: the `param` declarations (e.g. `name`, `skuName`, `blobServices`, `customerManagedKey`, `secretsExportConfiguration`) and the user-defined `type` declarations (e.g. `networkAclsType`, `secretsExportConfigurationType`, `blobServiceType`). Are all top-level params and types enumerated?"  (grep-findable: `^param `, `^type `)
2. **[D2 Relationship]** "Trace the cross-file `module` references out of `avm/res/key-vault/vault/main.bicep`: which child modules does it instantiate (e.g. `keyVault_secrets` -> `secret/main.bicep`, `keyVault_accessPolicies` -> `access-policy/main.bicep`, `keyVault_keys` -> `key/main.bicep`) and which external registry module does `keyVault_privateEndpoints` pull (`br/public:avm/res/network/private-endpoint`)? Show the include/reference edges in both directions."
3. **[D3 Retrieval]** "Retrieve the full definition of the `storageAccount` resource declaration (`resource storageAccount 'Microsoft.Storage/storageAccounts@...'`) in `avm/res/storage/storage-account/main.bicep` — the largest single definition in the file — with exact start/end boundaries."  (grep-findable: `^resource storageAccount `)
4. **[D4 Architecture]** "Describe the file/directory organization of the AVM resource module convention under `avm/res/<provider>/<resource>/`: the standard `main.bicep` + `README.md` + `version.json` triad, the nested child-module subfolders (e.g. `key-vault/vault/secret/`, `.../key/`, `.../access-policy/`), and the `tests/e2e/` test layout. How is the repo partitioned across resource providers (storage, network, key-vault, compute, ...)?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across all `main.bicep` modules, surface the duplicated/boilerplate `avmTelemetry` deployment resource and the repeated `enableTelemetry` param + `enableReferencedModulesTelemetry` var pattern — i.e. find structurally near-identical telemetry blocks copy-pasted across modules, and link the `builtInRoleNames` var convention to the `roleAssignments` param it feeds. This is similarity/duplication + config<->config linkage that plain grep approximates only by literal string match."

**Expected graph tools (hint, not a script):** D1->search_graph(label=Definition, file=".../storage-account/main.bicep"); D2->trace_call_path(symbol="keyVault", direction=both); D3->get_code_snippet(qualified_name=".../storage-account/main.bicep::storageAccount"); D4->get_architecture(path="avm/res"); D5->search_code/semantic_query("avmTelemetry telemetry boilerplate block").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/bicep-graph.md`, `bicep-explorer.md`, `bicep-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the bicep tier.

---

### 118. csv — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** vega/vega-datasets (`/tmp/bench/csv`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, widely-mirrored sample-data distribution for the Vega/Vega-Lite ecosystem (thousands of GitHub stars, vendored into countless viz tutorials); its `data/*.csv` files are the de-facto idiomatic corpus for tabular CSV, satisfying the plan's "popular + substantial + idiomatic" repo-selection criteria for Group E data.

**Note on a pure-data language:** CSV files carry no code-level symbols (no functions, classes, methods, or imports). A code knowledge graph indexes CSV files as File nodes and at most their containing folders — it does not parse row content or model columns as definitions. Per the plan's N/A-honesty rule, dimensions that presuppose code-level definitions are marked **N/A** with a reason rather than forced into unnatural questions.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "What are the header (column) field names of `data/seattle-weather.csv` — `date`, `precipitation`, `temp_max`, `temp_min`, `wind`, `weather` — and does the index surface this file at all as a discoverable File node?" (grep-findable: the header line is plain text on line 1; the symmetric expectation is that the graph can at least locate the file by name, since CSV has no richer 'definition' to extract.)
2. **[D2 Relationship]** "Which manifest/metadata files reference `seattle-weather.csv` as a data resource (e.g. `datapackage.json` [verify] and/or `SOURCES.md` [verify]) — i.e. the descriptor->data-file reference edges, if the indexer models them?" (grep-findable too: the filename string appears in the descriptor; the dimension is only meaningful if such a manifest exists in the indexed snapshot.)
3. **[D3 Retrieval]** "**N/A.** Reason: D3 retrieves the body of a code definition (function/class/struct) by qualified name. CSV files contain tabular data, not code definitions; `get_code_snippet` has no definition block to return for a `.csv` file (the graph would, at best, echo the raw file, which is a Read, not a structural retrieval). No natural D3 target exists for a pure-data CSV repo."
4. **[D4 Architecture]** "Describe the file/directory organization of the repo: the `data/` directory holding the CSV/JSON datasets versus the top-level build/metadata files (`scripts/` [verify], `datapackage.json` [verify], `README.md`). How are datasets grouped at the folder level?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Cluster datasets that share column-naming patterns — e.g. the weather family (`seattle-weather.csv`, `weather.csv` [verify]) sharing `temp_max`/`temp_min`/`precipitation`, or geo files sharing `latitude`/`longitude`. FAIRNESS NOTE: this is only graph-favoring if the indexer models CSV columns as nodes/properties; if it indexes CSV only as opaque File nodes, this becomes equally hard for graph and grep (grep can find the columns but not cluster; the graph cannot see columns at all). Score honestly on what the index actually contains rather than crediting a win the graph cannot earn."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="seattle-weather.*", label="File"); D2->search_graph(name_pattern="datapackage.*|SOURCES.*") then inspect any reference edges (trace_path only if descriptor->data edges are modeled); D3->N/A (no qualified-name target); D4->get_architecture(); D5->query_graph for shared-column clustering IF columns are indexed, else fall back to grep + manual clustering (expected: neither side wins cleanly).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/csv-graph.md`, `csv-explorer.md`, `csv-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D3 contributes an N/A marker, not a score), D5 within Group E only, Group E, the csv tier.

---

### 119. requirements — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** huggingface/transformers (`/tmp/bench/requirements`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** One of the most-starred ML repos on GitHub and the de-facto reference for Python ML packaging; its many `examples/pytorch/*/requirements.txt` pin files are idiomatic, substantial, and varied, matching the plan's "popular + idiomatic + non-trivial" repo-selection criteria for the requirements tier.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level requirement (package) entries declared across the example pin files — e.g. confirm that specifiers such as `datasets`, `accelerate`, `evaluate`, `sentencepiece`, and `torch` are surfaced as distinct dependency definitions." (All grep-findable as literal package names in `requirements.txt` lines.)
2. **[D2 Relationship]** "Which other files reference or include the same dependency declared in `examples/pytorch/_tests_requirements.txt` — i.e. map the cross-file references where the same package (e.g. `datasets`) is pinned in multiple `examples/pytorch/*/requirements.txt` files, and how those relate to the central `setup.py` `_deps` declaration."
3. **[D3 Retrieval]** "Retrieve the full contents of the consolidated test pin file — `examples/pytorch/_tests_requirements.txt` (the repo's aggregated example test pin set) — as one block." (File name is grep-findable; this is the single largest pin file under `examples/`.)
4. **[D4 Architecture]** "Describe the file/directory organization of dependency declarations across the repo: how each task subdirectory under `examples/pytorch/` (e.g. `text-classification/`, `translation/`, `token-classification/`) carries its own `requirements.txt`, how `examples/pytorch/_tests_requirements.txt` aggregates them, and how these relate to the root packaging in `setup.py`'s `_deps`."
5. **[D5 Cross-cutting/Semantic]** "GRAPH-FAVORING: Detect duplication and config↔code linkage — find packages pinned with diverging version specifiers across multiple `examples/pytorch/*/requirements.txt` files (e.g. `datasets >= 1.8.0` vs `datasets >= 1.13.3`) and link each pinned package back to where it is consumed (imported) in the example scripts. Plain grep can list occurrences but cannot resolve the config→code consumption edge."

**Expected graph tools (hint, not a script):** D1->search_graph(label/kind="dependency", name_pattern="datasets|accelerate|evaluate|sentencepiece|torch"); D2->trace_path(direction=both, from the pinned package node to its other declaring files / setup.py _deps); D3->get_code_snippet(qualified_name="examples/pytorch/_tests_requirements.txt"); D4->get_architecture(scope="examples/pytorch/* requirements + setup.py _deps"); D5->search_code/semantic_query(duplicate version specifiers + package→import consumption).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/requirements-graph.md`, `requirements-explorer.md`, `requirements-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the requirements tier.

---

### 120. hlsl — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** microsoft/DirectX-Graphics-Samples (`/tmp/bench/hlsl`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Microsoft's flagship D3D12 sample suite is the canonical, high-star HLSL corpus — its MiniEngine carries a substantial, idiomatic `.hlsl`/`.hlsli` shader tree (entry points, cbuffers, include chains), matching the plan's "popular + idiomatic + substantial" repo-selection bar for the shader/HDL slice of Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level HLSL definitions in `MiniEngine/Model/Shaders/DefaultPS.hlsl` — the shader entry point `main`, the constant buffers `MaterialConstants` and `GlobalConstants`, and the input struct `VSOutput`. Are these surfaced as discoverable definitions?" (grep-findable: each name appears verbatim as a `cbuffer`/`struct`/function token.)
2. **[D2 Relationship]** "Resolve the `#include` chain reaching `MiniEngine/Core/Shaders/ShaderUtility.hlsli`: it includes `ColorSpaceUtility.hlsli` and `PixelPacking.hlsli`, and is itself pulled in (directly or transitively) by post-process compute shaders. Show the cross-file include graph in both directions (who includes ShaderUtility.hlsli, and what it includes)."
3. **[D3 Retrieval]** "Retrieve the full body of the BRDF lighting helper `ApplyDirectionalLight`, defined in `MiniEngine/Model/Shaders/Lighting.hlsli` (the header `DefaultPS.hlsl` pulls in via `Common.hlsli`) — one of the core directional-light shading routines, called from the `ShadeLights` aggregator." (grep-findable: function name `ApplyDirectionalLight` is a literal identifier.)
4. **[D4 Architecture]** "Describe the shader-tree organization of MiniEngine: the split between shared `Core/Shaders/*.hlsli` utility headers (e.g. `ColorSpaceUtility.hlsli`, `GenerateMipsCS.hlsli`) and per-sample shaders under `Model/Shaders/` (e.g. the light-grid headers `FillLightGridCS.hlsli` / `Lighting.hlsli`) and `ModelViewer/Shaders/`. How are `.hlsl` entry-point files vs `.hlsli` include-only headers grouped?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across the HLSL tree, surface near-duplicate / shared-pattern definitions: color-space and pixel-packing helpers reused via includes (e.g. `RGBToLuminance`, `ApplyDisplayProfile`, `ConvertColor` in `ShaderUtility.hlsli`) and the repeated `main`/`CSMain` entry-point convention across compute shaders. Which utility definitions are referenced from the most shaders, and where is logic duplicated rather than shared via `.hlsli`?"

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="main|MaterialConstants|GlobalConstants|VSOutput", path~="MiniEngine/Model/Shaders/DefaultPS.hlsl"); D2->trace_call_path(symbol="ShaderUtility.hlsli", direction=both) / query_graph IMPORTS edges over `.hlsli`; D3->get_code_snippet(qualified_name="ApplyDirectionalLight"); D4->get_architecture(scope="MiniEngine/**/Shaders"); D5->search_code/semantic_query("color space / luminance / pixel packing helpers reused across shaders").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/hlsl-graph.md`, `hlsl-explorer.md`, `hlsl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the hlsl tier.

---

### 121. vhdl — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** VUnit/vunit (`/tmp/bench/vhdl`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** VUnit is the most-starred VHDL test framework on GitHub with a large, idiomatic VHDL runtime (`vunit/vhdl/` ships the `check`, `run`, `logging`, `com`, `data_types` libraries) — substantial, hand-written VHDL packages that match the plan's "popular + idiomatic + substantial" repo-selection criteria for HDL.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the public declarations of the check package spec in `vunit/vhdl/check/src/check_api.vhd` — specifically the overloaded `check` and `check_equal` procedures. (grep-findable: literal tokens `procedure check`, `procedure check_equal` in `check_api.vhd`; the package-spec file is `check_api.vhd` and the implementation lives in `checker_pkg.vhd` / `checker_pkg-body.vhd` — there is no `check_pkg.vhd` [verify].)"
2. **[D2 Relationship]** "Starting from `test_runner_setup` (declared in `vunit/vhdl/run/src/run_api.vhd`), recover its relationship to the other run-control subprograms (`test_suite`, `run`, `test_runner_cleanup`, also declared in `run_api.vhd`) and to the implementing body in `run.vhd` (cross-reference between the `run_api` spec and the `run` body package). Note: in fast (non-LSP) mode the VHDL extractor's subprogram call/reference edges across `*_api.vhd` ↔ body packages are typically sparse or absent [verify], so a `trace_call_path` may surface few/no edges; report which side (graph CALLS/reference edges vs grep over `procedure test_runner_setup` / `impure function run` declaration + call sites) actually surfaces each link."
3. **[D3 Retrieval]** "Retrieve the full source of the `logger_t`-returning function `get_logger` (and its overloads) declared in `vunit/vhdl/logging/src/logger_pkg.vhd`. Return exactly that declaration/those overloads, not the whole file. (grep-findable token: `get_logger`; declared in `logger_pkg.vhd`, not in `log_handler_pkg.vhd` [verify].)"
4. **[D4 Architecture]** "Describe the directory/library organization of the VHDL runtime under `vunit/vhdl/` — how the per-library `src/` folders (`check/`, `run/`, `logging/`, `com/`, `data_types/`) and their `*_api.vhd` spec vs `*-body.vhd` implementation packages are arranged, and how the top-level `vunit_context.vhd` / `vunit_run_context.vhd` context files tie libraries together."
5. **[D5 Cross-cutting/Semantic]** "[GRAPH-FAVORING] Find the families of near-duplicate generated/templated packages — e.g. the `integer_array_pkg`, `integer_vector_ptr_pkg`, `string_ptr_pkg`, `queue_pkg` data-type packages (each with its `*-body*.vhd` and per-standard `-93`/`-2002p`/`-2008p` variants) and the many overloaded `check_*`/`push_*`/`pop_*` subprograms — and surface naming-pattern clusters plus the `*_api.vhd`/spec ↔ `*-body.vhd` duplication that plain text search cannot cluster semantically. Note: in fast (non-LSP) mode `SIMILAR_TO`/`SEMANTICALLY_RELATED` edges are typically absent [verify], so the clustering leans on Package/Function-node naming families plus `search_code`, not on a dedicated similarity edge."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*check(_equal)?.*", label="Function|Package"); D2->trace_call_path(name="test_runner_setup", direction="both") best-effort (VHDL subprogram reference edges sparse in fast mode [verify], so cross-check against grep declaration/call sites in `run_api.vhd`/`run.vhd`); D3->get_code_snippet(qualified_name=".*get_logger"); D4->get_architecture(path="vunit/vhdl"); D5->search_code/search_graph(naming-family clustering of duplicated data-type pointer packages and overloaded check/push/pop families; semantic edges sparse in fast mode [verify]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/vhdl-graph.md`, `vhdl-explorer.md`, `vhdl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the vhdl tier.

---

### 122. systemverilog — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** lowRISC/ibex (`/tmp/bench/systemverilog`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ibex is a widely-cited, production-grade RISC-V core (lowRISC/OpenTitan flagship); it is large, idiomatic SystemVerilog (modules, packages, typedefs, cross-file `import` references), matching the plan's "popular + substantial + idiomatic" repo-selection criteria for the HDL slot in Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level definitions of the `ibex_alu` module: confirm the `ibex_alu` module declaration exists in `rtl/ibex_alu.sv` and surface other grep-findable module declarations such as `ibex_decoder`, `ibex_controller`, and the `ibex_pkg` package definition."
2. **[D2 Relationship]** "Show the cross-file reference structure around `ibex_pkg`: which `.sv` modules reference it via `import ibex_pkg::*;` (e.g. `ibex_alu`, `ibex_id_stage`, `ibex_cs_registers`), and which modules instantiate `ibex_alu`/`ibex_decoder` (expect `ibex_ex_block` and `ibex_id_stage` respectively)."
3. **[D3 Retrieval]** "Retrieve the full definition of the `alu_op_e` typedef enum declared in `rtl/ibex_pkg.sv` (the ALU operation opcode enumeration)."
4. **[D4 Architecture]** "Describe the file/directory organization of the RTL: how the core decomposes into pipeline-stage files (`ibex_if_stage`, `ibex_id_stage`, `ibex_ex_block`, `ibex_wb_stage`), shared package files (`ibex_pkg`, `ibex_tracer_pkg`), and the `ibex_core`/`ibex_top` top-level wrappers under `rtl/`."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find naming-pattern / duplication clusters across the RTL: the three interchangeable register-file implementations (`ibex_register_file_ff`, `ibex_register_file_fpga`, `ibex_register_file_latch`) and the two multiplier/divider variants (`ibex_multdiv_fast`, `ibex_multdiv_slow`), and link the `*_e` enum types in `ibex_pkg` to their consuming modules — a config<->code / similarity query grep cannot resolve structurally."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="ibex_(alu|decoder|controller).*|ibex_pkg"); D2->trace_call_path(name="ibex_pkg", direction="both") + search_graph(relationship="IMPORTS"); D3->get_code_snippet(qualified_name="ibex_pkg::alu_op_e"); D4->get_architecture(scope="rtl/"); D5->search_code/semantic_query("register file / multdiv variant duplication; enum-to-module usage").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/systemverilog-graph.md`, `systemverilog-explorer.md`, `systemverilog-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the systemverilog tier.

---

### 123. devicetree — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** u-boot/u-boot (`/tmp/bench/devicetree`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** U-Boot is the canonical open-source bootloader and ships one of the largest, most idiomatic corpora of `.dts`/`.dtsi` devicetree sources in the world (thousands of board/SoC files under `arch/*/dts/`), satisfying the plan's "popular + substantial + idiomatic for the language" repo-selection criterion for the devicetree subset of Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level node definitions in `arch/arm/dts/` source files that declare a `chosen` node and an `aliases` node — both are grep-findable keywords (`chosen {`, `aliases {`) that mark the standard top-level devicetree definitions a board file must provide." (symmetric: plain grep for `chosen {` / `aliases {` finds the same targets)
2. **[D2 Relationship]** "Starting from a board file such as `arch/arm/dts/imx6q.dtsi`, map the cross-file `/include/` and `#include` reference chain (which `.dtsi` it pulls in, e.g. `imx6dl.dtsi` / `imx6qdl.dtsi`) and which board-level `.dts` files in turn include it — i.e. the include dependency graph in both directions."
3. **[D3 Retrieval]** "Retrieve the full body of the `soc` node (`soc {`) defined in `arch/arm/dts/imx6qdl.dtsi` [verify] — this is the largest single subtree in the file and the canonical 'retrieve one named definition' target. The anchor (`soc {`) is grep-findable, so plain text search can locate the starting line too; the retrieval/boundary task is what is being measured." (symmetric: anchor is grep-findable)
4. **[D4 Architecture]** "Describe how devicetree sources are organized in this repo: the `arch/<arch>/dts/` directory layout, the split between shared SoC `.dtsi` includes and per-board `.dts` files, and the role of `arch/arm/dts/Makefile` in selecting which boards are built."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Identify duplication and config<->code links: which `compatible = "..."` strings used across the `.dts`/`.dtsi` corpus correspond to driver bindings declared in the C source (e.g. `U_BOOT_DRIVER(...)` / `.compatible` of_match entries), and which `compatible` values are repeated across many board files — a semantic/similarity question text search cannot answer structurally."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern="chosen|aliases"); D2->trace_path(function_name="imx6q.dtsi", direction="both", edge_types=["IMPORTS"]); D3->get_code_snippet(qualified_name="arch/arm/dts/imx6qdl.dtsi::soc"); D4->get_architecture(project="devicetree"); D5->search_code(pattern="compatible") + query_graph(Cypher matching `compatible` strings to C `of_match`/U_BOOT_DRIVER bindings). (Note: there is no `trace_call_path` or `semantic_query` tool — the real tools are `trace_path`, `search_code`, and `query_graph`.)

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/devicetree-graph.md`, `devicetree-explorer.md`, `devicetree-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the devicetree tier.

---

### 124. linkerscript — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** zephyrproject-rtos/zephyr (`/tmp/bench/linkerscript`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Zephyr is the dominant open-source RTOS (>10k stars, broad SoC support) and carries one of the largest idiomatic GNU-ld linker-script corpora in OSS — per-arch `linker.ld` plus a deep tree of reusable `.ld` include fragments — matching the plan's "popular + substantial + idiomatic in this language" repo-selection criterion.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level output-section / symbol definitions declared inside the SECTIONS block of `include/zephyr/linker/common-ram.ld` and `include/zephyr/linker/common-rom.ld`, e.g. the `.bss` / `.noinit` output sections and the `ITERABLE_SECTION_RAM(...)` invocations — name each definition and its source file." (Plain grep can also locate these: the section names and macro tokens are literal strings in the `.ld` text, so this is a symmetric, not graph-only, target.)
2. **[D2 Relationship]** "Starting from an architecture top-level script such as `include/zephyr/arch/arm/cortex_m/scripts/linker.ld` [verify], resolve the cross-file `#include` chain (e.g. into `include/zephyr/linker/common-rom.ld`, `common-ram.ld`, `kobject-data.ld` [verify]) and report which fragments it pulls in — i.e. the include/reference graph in both directions."
3. **[D3 Retrieval]** "Retrieve the full text of the single largest output-section definition: the `.text` output-section block (the `_TEXT_SECTION_NAME` / `.text :` body) in the main `include/zephyr/arch/arm/cortex_m/scripts/linker.ld` [verify] — return the whole block verbatim, not a summary." (Symmetric: the block is anchored by the literal `.text` / `_TEXT_SECTION_NAME` tokens that grep finds too; the test is whether the tool returns the *exact span*.)
4. **[D4 Architecture]** "Describe how Zephyr organizes its linker-script tree: the split between per-architecture top-level scripts under `include/zephyr/arch/<arch>/.../scripts/linker.ld` and the shared, included fragments under `include/zephyr/linker/` (common-ram.ld, common-rom.ld, kobject-*.ld, thread-local-storage.ld [verify]). Summarize the directory/file layout and the layering it implies."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across the `.ld` fragments under `include/zephyr/linker/`, which output sections / `ITERABLE_SECTION_*` macro invocations and `GROUP_START`/`GROUP_END` pairs recur across multiple files? Report the cross-file recurrence and grouping structure of these section/macro patterns. (Graph-favoring because it asks for the multi-file co-occurrence structure of section definitions in one query; grep can find each token but must be re-run per pattern and cannot present the cross-file grouping directly. NOTE: this stays *within* the linker-script corpus — no `.ld`→`.h` semantic linkage is claimed, since the indexer does not build cross-language edges from linker section macros to C header APIs.)"

**Expected graph tools (hint, not a script):** D1->search_graph(label/name_pattern over .ld section/symbol definitions); D2->trace_path(direction=both over the #include/reference chain); D3->get_code_snippet(qualified_name / span of the largest .text section block); D4->get_architecture(linker subtree); D5->search_graph + query_graph(cross-file recurrence of section/macro patterns within the .ld corpus).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/linkerscript-graph.md`, `linkerscript-explorer.md`, `linkerscript-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the linkerscript tier.

---

### 125. gn — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** flutter/buildroot (`/tmp/bench/gn`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Flutter Engine's canonical GN/Ninja bootstrap — large, idiomatic `.gn`/`.gni`/`BUILD.gn` corpus with real toolchain templates and config targets, matching the plan's "popular + substantial + idiomatic" repo-selection criteria for GN.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the top-level GN definitions in the build tree: the `template("gcc_toolchain")` declaration in `build/toolchain/gcc_toolchain.gni`, and the `config("compiler")` target in `build/config/compiler/BUILD.gn`. Are the template and config definitions enumerated as graph nodes, with their defining file and line?" (well-known, grep-findable identifiers: the literal tokens `template("gcc_toolchain")` and `config("compiler")` are plain text. Note: per-platform toolchains such as `clang_x64`/`clang_arm64` in `build/toolchain/*/BUILD.gn` are *instantiations* of `gcc_toolchain(...)`, not separate template declarations.)
2. **[D2 Relationship]** "Starting from the root dotfile `.gn` (`buildconfig = \"//build/config/BUILDCONFIG.gn\"`), trace the cross-file reference/include chain: which `.gni` files does `BUILDCONFIG.gn` pull in via `import(...)` (e.g. `//build/toolchain/custom/custom.gni`, `//build/config/ios/ios_sdk.gni`), and which `build/toolchain/*/BUILD.gn` targets instantiate the `gcc_toolchain` template? (structural cross-file `import`/reference edges, direction=both)"
3. **[D3 Retrieval]** "Retrieve the full body of the `template(\"gcc_toolchain\")` definition in `build/toolchain/gcc_toolchain.gni` — exactly as written, with no surrounding `import(...)` boilerplate. (grep-findable: the literal token `gcc_toolchain` locates the definition site; the value tested is exact-boundary body retrieval, not symbol discovery.)"
4. **[D4 Architecture]** "Describe the file/dir organization of the GN build tree: the `build/config/` vs `build/toolchain/` split, where `BUILDCONFIG.gn` sits relative to the root `.gn`, and how per-platform toolchains/configs are grouped (e.g. the `build/toolchain/{linux,mac,win,android,fuchsia,...}/` subtree)."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Find duplicated/near-duplicate `config(...)` blocks and repeated compiler-flag patterns (e.g. `cflags`/`ldflags` lists) across `build/config/**/BUILD.gn`, and surface config<->toolchain links (which `gcc_toolchain` instantiations pull in which `config` targets) that plain grep cannot cluster. Similarity/cross-reference ranking over config nodes."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*gcc_toolchain.*|compiler", label="Definition"/"Template"); D2->trace_call_path(from=".gn"/"BUILDCONFIG.gn", direction=both, edge=IMPORTS/references); D3->get_code_snippet(qualified_name=".*gcc_toolchain"); D4->get_architecture(scope="build/"); D5->search_code/semantic_query("duplicate config cflags/ldflags blocks", then config<->toolchain links).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/gn-graph.md`, `gn-explorer.md`, `gn-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the gn tier.

---

### 126. kconfig — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** buildroot/buildroot (`/tmp/bench/kconfig`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Buildroot is the canonical, large-scale Kconfig codebase (thousands of `Config.in`/`Kconfig` files, ~3000+ packages), making it the most idiomatic and substantial real-world test of Kconfig handling, matching the plan's "popular + substantial + idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level Kconfig definitions: locate the `config BR2_PACKAGE_BUSYBOX` symbol and the `config BR2_TOOLCHAIN_BUILDROOT` symbol, and identify which `Config.in` file each is declared in." (Both are real, grep-findable `config` symbol names; symmetric — answerable by plain grep too.)
2. **[D2 Relationship]** "Map the include graph from the root `Config.in`: which sub-files does it pull in via `source` (e.g. `package/Config.in`, `toolchain/Config.in`, `fs/Config.in`, `boot/Config.in`, `Config.in.legacy`), and which file ultimately `source`s `package/busybox/Config.in`?"
3. **[D3 Retrieval]** "Retrieve the full menu definition block for the `BR2_PACKAGE_BUSYBOX` config entry from `package/busybox/Config.in`, including its prompt, `select`/`depends on` lines, and help text." (One real named symbol; symmetric — grep-findable starting point.)
4. **[D4 Architecture]** "Describe the Config.in file/directory organization: how are the top-level menus (Target options, Build options, Toolchain, System configuration, Filesystem images, Bootloaders, Target packages) wired together, and where does the per-package `Config.in` tree live under `package/`?"
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Cross-cutting / semantic over the `BR2_PACKAGE_*` symbol family: group packages by the `BR2_PACKAGE_*` naming pattern across the `package/` Config.in tree, and surface near-duplicate help-text blocks (e.g. boilerplate prompt/help wording reused across many package `Config.in` files). [verify] If the index links Kconfig symbols to their sibling `package/<name>/<name>.mk` build recipe, also flag symbols whose `.mk` recipe is missing or mismatched; this config<->Make cross-language link may not be modeled, so treat the `.mk` cross-check as best-effort, not a required graph win."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="BR2_PACKAGE_BUSYBOX|BR2_TOOLCHAIN_BUILDROOT", label="Definition"); D2->trace_path(qualified_name="Config.in", direction="both") for `source` include edges; D3->get_code_snippet(qualified_name="package/busybox/Config.in::BR2_PACKAGE_BUSYBOX"); D4->get_architecture(scope="Config.in tree"); D5->search_code(query="BR2_PACKAGE_* family grouping; duplicated help-text blocks") + query_graph for the naming-pattern grouping (and, if modeled, the Config.in<->.mk link).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/kconfig-graph.md`, `kconfig-explorer.md`, `kconfig-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the kconfig tier.

---

### 127. bitbake — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** openembedded/meta-openembedded (`/tmp/bench/bitbake`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** meta-openembedded is the canonical, most-active community layer collection for the Yocto/OpenEmbedded build system — thousands of idiomatic BitBake `.bb` recipes plus `.bbclass`/`.inc` shared logic across `meta-oe`, `meta-python`, `meta-networking`, etc., making it the large, popular, maintainer-authored BitBake corpus the plan's repo-selection criteria call for. The extractor parses BitBake via a vendored tree-sitter grammar (functions: `function_definition`/`python_function_definition`/`recipe`; vars: `variable_assignment`; includes via `require`/`include`), so `.bb`/`.bbclass` symbols land in the graph as real Definition and import nodes — D1–D4 are answerable structurally, not just by text.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API — grep-symmetric]** "In `meta-oe/recipes-support/htop/htop_*.bb` [verify], locate the shell task definitions `do_compile` and `do_install` and the `PACKAGECONFIG` variable assignment. These are literal tokens the extractor captures as Definition/Variable nodes. Do the graph results (`search_graph(name_pattern=...)`) match exactly what a plain `grep -n 'do_install\|do_compile\|PACKAGECONFIG'` over the same `.bb` file returns? Both tools should find the identical set — this is the symmetric anchor."
2. **[D2 Relationship]** "Show the include/require reference graph for `meta-python/recipes-devtools/python/python3-cython_*.bb` [verify]: which `.bbclass`/`.inc` files it pulls in (e.g. `setuptools3`, `pypi` [verify]) and which other recipes pull in the same shared includes. NOTE: the extractor's BitBake import set captures `require`/`include`/`include_directive` as graph edges — `inherit` may be visible only to grep, so report which links are graph-backed vs text-only. Traverse both directions (includes upward, dependents downward)."
3. **[D3 Retrieval — grep-symmetric]** "Retrieve the full body of the recipe `meta-networking/recipes-connectivity/networkmanager/networkmanager_*.bb` [verify] — one of the larger single `.bb` definitions in the tree — exactly as written, including its `SRC_URI`, `DEPENDS`, `PACKAGECONFIG`, and `do_install` block, with no surrounding layer boilerplate. The file path is grep-findable; the test is whether `get_code_snippet` reproduces the recipe body verbatim."
4. **[D4 Architecture]** "Describe the structural organization of the layer collection: how the top-level `meta-oe`, `meta-python`, `meta-networking`, `meta-multimedia`, and `meta-filesystems` layers are partitioned, how each layer's `conf/layer.conf` and `recipes-*/<pkg>/<pkg>_<ver>.bb` convention is arranged, and where shared `classes/*.bbclass` sit relative to the recipes that include them."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Across all `.bb` recipes, surface the cluster of near-duplicate Python recipes in `meta-python/recipes-devtools/python/` that share the same `SRC_URI`/checksum/`do_install` boilerplate family, and find recipes in other layers that re-implement the same pattern. This is a similarity / shared-include clustering query where the graph's structural grouping should outperform line-oriented grep; if the index lacks semantic vectors for this corpus, mark this dimension N/A with that reason rather than forcing a text-grep answer."

**Expected graph tools (hint, not a script):** D1->`search_graph(project="bitbake", name_pattern=".*(do_install|do_compile|PACKAGECONFIG).*", file_pattern="*htop*")`; D2->`trace_path(project="bitbake", function_name="python3-cython", mode="calls", direction="both", edge_types=["IMPORTS"])` (plus `search_code` for `require`/`inherit` lines); D3->`search_graph` to get the exact qualified_name then `get_code_snippet(project="bitbake", qualified_name="networkmanager_*")`; D4->`get_architecture(project="bitbake")`; D5->`search_graph(project="bitbake", semantic_query=["setuptools3","pypi","SRC_URI","do_install","boilerplate"])` or `search_code` fallback. (No LSP Deep-dive block: BitBake is a tree-sitter-only Group E config/build DSL with no language server in the bench, so cross-repo X1/X2 and semantic S1/S2 LSP probes are N/A.)

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/bitbake-graph.md`, `bitbake-explorer.md`, `bitbake-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the bitbake tier.

---

### 128. smali — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** JesusFreke/smali (`/tmp/bench/smali`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Canonical, widely-used Android dex assembler/disassembler; its `**/src/test/**/*.smali` and `**/tests/**/*.smali` fixtures are the de-facto reference corpus for the smali assembly grammar, making it the most idiomatic and substantial body of real `.smali` source available.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level structural definitions declared across the `.smali` fixtures — i.e. every `.class` declaration and its `.method`/`.field` members (grep-findable directives like `.class`, `.method`, `.field`). Do the discovered class names match the grep count of `^\.class` directives?"
2. **[D2 Relationship]** "N/A — `.smali` fixtures are standalone Dalvik-bytecode listings; their `invoke-*` opcodes name target descriptors (e.g. `Ljava/lang/Object;->hashCode()I`) that resolve to the Android framework or to dex symbols, not to other fixture files. There is no fixture-to-fixture call relationship for the graph to model as edges, so a relationship question would be forced and unanswerable. (If one only wanted the raw `invoke-*` target strings inside a file, that is a plain grep over `invoke-` lines, not a graph relationship.)"
3. **[D3 Retrieval]** "Retrieve the full body of a named `.method` definition from a fixture — e.g. a method whose signature contains `main([Ljava/lang/String;)V` [verify], or any single `.method ... .end method` block found via grep on `^\.method` — returning the exact span from its `.method` directive to its matching `.end method` (grep-findable boundary; the retrieval test is exact-span fidelity)."
4. **[D4 Architecture]** "Describe how the `.smali` fixtures are organized across the module test trees (`smali/src/test/`, `baksmali/src/test/`, `dexlib2/src/test/`, and any `tests/` dir [verify]) and how that directory layout reflects the assembler / disassembler / lib split of the project (structure-of-folders question; the tree itself is also ls/grep-findable)."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find near-duplicate / templated `.smali` fixtures — files sharing the same `.class`/`.super`/`.registers` skeleton with only the opcode payload differing — and surface naming-pattern clusters (e.g. families of opcode-specific test files). Label: semantic/duplication query, not reachable by plain grep."

**Expected graph tools (hint, not a script):** D1->search_graph(label/kind for `.class`/`.method` definitions, cross-checked vs grep `^\.class`); D2->N/A (no fixture-to-fixture relationship edges for standalone bytecode listings); D3->get_code_snippet(qualified_name of the target `.method`) or grep `^\.method`.. `^\.end method`; D4->get_architecture(test-tree structure); D5->search_code/semantic_query(duplication + naming clusters).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/smali-graph.md`, `smali-explorer.md`, `smali-judged.json`.
**Aggregates into:** D1/D3/D4 cross-group rollups (D2 excluded as N/A for smali), D5 within Group E only, Group E, the smali tier.

---

### 129. tablegen — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** llvm/llvm-project (subset lib/Target/X86 *.td) (`/tmp/bench/tablegen`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** LLVM's X86 backend `.td` files are the canonical, large-scale TableGen corpus (instruction/register/schedule descriptions) — popular, idiomatic, and substantial, matching the plan's "real, widely-used, non-trivial repo" selection criterion.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the top-level TableGen definitions for the X86 general-purpose 32-bit register class `GR32` and the feature record `FeatureAVX512`. Where are they declared and what are their backing files?" (Both names are real, grep-findable string tokens — `def GR32` in `X86RegisterInfo.td`, `def FeatureAVX512` in `X86.td`; symmetric — answerable by plain grep too.)
2. **[D2 Relationship]** "Starting from `X86InstrInfo.td`, map the cross-file `include`/reference structure: which other `*.td` files does it pull in (e.g. `X86InstrFormats.td`, `X86InstrArithmetic.td`, `X86InstrSSE.td`), and which records inherit from the base instruction class `X86Inst`?"
3. **[D3 Retrieval]** "Retrieve the full definition body of the processor-model record `def : ProcModel<\"skylake\", ...>` — or, if absent under that exact spelling, the `ProcessorFeatures` group record it derives from. Return the complete record, not a snippet." (Single named symbol; `ProcModel`/`ProcessorFeatures` are real, grep-findable starting points; symmetric.)
4. **[D4 Architecture]** "Describe the file/directory organization of the X86 TableGen subset: how are register definitions, instruction formats, per-extension instruction tables (SSE/AVX512), and scheduling models partitioned across files like `X86RegisterInfo.td`, `X86InstrFormats.td`, `X86InstrSSE.td`, `X86InstrAVX512.td`, and `X86Schedule.td`?"
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Identify naming-pattern / duplication clusters across the instruction `.td` files — e.g. the family of `Feature*` records, repeated `MRM*` format multiclasses, or near-identical SSE-vs-AVX instruction definition templates — and surface config<->code links between a `Feature*` predicate and the instruction records gated on it. This leans on similarity/semantic grouping beyond a single grep token."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="GR32|FeatureAVX512", label="Definition"); D2->trace_path(qualified_name="X86Inst", direction="both") + include/reference edges; D3->get_code_snippet(qualified_name="ProcessorFeatures" / "skylake"); D4->get_architecture(scope="lib/Target/X86"); D5->search_code/query_graph("Feature predicate gating instruction record; MRM/SSE-vs-AVX duplication").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/tablegen-graph.md`, `tablegen-explorer.md`, `tablegen-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the tablegen tier.

---

### 130. ispc — B (Systems & Low-level)

**Repo:** ispc/ispc (`/tmp/bench/ispc`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Intel's SPMD compiler is a widely-starred, production LLVM-based C++ codebase whose `src/` tree (codegen context, type system, parser, optimizer) is idiomatic, substantial systems C++ — matching the plan's Group B criterion of popular, real-world low-level code; the bundled `examples/*.ispc` also exercise the ispc grammar itself.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the definition sites and signatures for the `FunctionEmitContext` class and its method `FunctionEmitContext::EmitFunctionParameterDebugInfo`, and surface the free-standing optimizer entry `Optimize` (declared in the `ispc` namespace in `opt.h` for module-level passes). All three are literal identifiers in `src/ctx.{h,cpp}` and `src/opt.{h,cpp}`, so they are grep-findable too — this question is symmetric, not graph-only."
2. **[D2 Relationship]** "Trace the call relationships (callers and callees, both directions) of `Module::CompileFile` — show how it reaches the parser entry (`parse` / `preprocessAndParse`, which drive `yyparse`) and downstream emission/optimization (`ast->GenerateIR`, then the free-standing `Optimize`)."
3. **[D3 Retrieval]** "Retrieve the full source of the single method `Module::CompileFile` with its exact line boundaries. This is one named symbol that also exists verbatim in `src/module.cpp`, so it is grep-findable too — the test is precise-boundary retrieval, not graph-only discovery."
4. **[D4 Architecture]** "Describe the architecture of `src/`: how the front-end (`lex.ll`/`parse.yy`, `decl.cpp`, `expr.cpp`, `stmt.cpp`), the type system (`type.cpp`, `sym.cpp`), the codegen context (`ctx.cpp`), and the optimizer (`opt.cpp`) are organized and depend on each other."
5. **[D5 Cross-cutting/Semantic — GRAPH-FAVORING]** "Semantic/cross-cutting: find the routines that emit LLVM IR or build `llvm::Value*`s for masked/varying SPMD operations (e.g. the masked-store / gather-scatter / `*Inst` emitters in `ctx.cpp`) and any duplicated mask-handling logic across `ctx.cpp` and `expr.cpp` — relate these emitters back to the `Type`/`AtomicType` they specialize. Graph-favoring because it requires semantic clustering by behavior + type-link, not a single grep token."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*FunctionEmitContext.*|^Optimize$", label="Class/Function"); D2->trace_call_path(qualified_name="Module::CompileFile", direction="both"); D3->get_code_snippet(qualified_name="Module::CompileFile"); D4->get_architecture(scope="src/"); D5->search_code/semantic_query("emit masked store / gather / varying value for SPMD type").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/ispc-graph.md`, `ispc-explorer.md`, `ispc-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the ispc tier.

---

### 131. cairo — A (Class-based OOP & Contracts)

**Repo:** OpenZeppelin/cairo-contracts (`/tmp/bench/cairo`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, audited reference implementation of Starknet smart-contract standards (~900+ stars, idiomatic Cairo `#[starknet::component]` OOP-style modules), satisfying the plan's "popular + idiomatic + substantial" repo-selection criteria for the cairo tier.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find the public entry-point functions of the ERC-20 implementation: locate `transfer`, `transfer_from`, and `approve` defined inside `ERC20Component` (packages/token/src/erc20/erc20.cairo), and the `assert_only_owner` function of `OwnableComponent` (packages/access/src/ownable/ownable.cairo). Are these definitions surfaced as nodes?"
2. **[D2 Relationship]** "Take `transfer_from` in `ERC20Component`: trace its relationships in both directions — what internal helpers it calls (e.g. `_spend_allowance`, `_transfer`, and the `update` core mover) and which callers/embedders reach it. Does the bidirectional call graph reconstruct the transfer→update chain?"
3. **[D3 Retrieval]** "Retrieve the exact source of the `_transfer` internal function defined in `ERC20Component` (packages/token/src/erc20/erc20.cairo), returning only that function body with correct boundaries."
4. **[D4 Architecture]** "Describe the structural organization of the repo: it is a Scarb workspace whose real implementations live under `packages/<pkg>/src/…` (e.g. `packages/token/src/{erc20,erc721,erc1155}`, `packages/access/src/{ownable,accesscontrol}`, `packages/introspection/src/src5`, plus `packages/security`, `packages/account`, `packages/upgrades`, `packages/utils`, `packages/presets`, `packages/governance`, `packages/finance`, `packages/merkle_tree`, `packages/interfaces`, `packages/macros`), with the root `src/lib.cairo` acting only as a meta re-export. Capture the component-vs-interface file layering. Does the architecture view recover this workspace/package grouping (not a single flat `src/` tree)?"
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Semantic/cross-cutting: find the recurring `InternalTrait` / `InternalImpl` (`#[generate_trait]`) pattern repeated across components (ERC20, ERC721, Ownable, AccessControl) and the duplicated `assert_only_*` authorization guards. Surface these naming-pattern clusters and the interface↔component implementation links (`IERC20` ↔ `ERC20Component`) that plain text search cannot cluster. Labeled graph-favoring."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*(transfer|transfer_from|approve|assert_only_owner).*", label="Function"); D2->trace_call_path(qualified_name="...ERC20Component...transfer_from", direction="both"); D3->get_code_snippet(qualified_name="...ERC20Component..._transfer"); D4->get_architecture(); D5->search_code/semantic_query("InternalTrait authorization guard component impl").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/cairo-graph.md`, `cairo-explorer.md`, `cairo-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the cairo tier.

---

### 132. move — A (Class-based OOP & Contracts)

**Repo:** econia-labs/econia (`/tmp/bench/move`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Econia is the canonical large-scale Aptos Move codebase (on-chain order book, ~3k+ GitHub stars, heavily documented), giving idiomatic, substantial Move modules — structs, `public`/`public entry fun`, `acquires`, generics — that exercise the plan's "real, idiomatic, popular" repo-selection criteria for a contract-oriented language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "In the `econia::market` module (`src/move/econia/sources/market.move`), list the public order-entry API: find the `public entry fun` declarations such as `place_limit_order_user_entry`, `place_market_order_user_entry`, and `cancel_order_user`, plus the underlying `public fun place_limit_order`. Report each name, its module path, and whether it is an entry function." (grep-findable: literal `fun place_limit_order`, `public entry fun place_limit_order_user_entry`.)
2. **[D2 Relationship]** "Show the call graph around `econia::market::place_limit_order` in both directions: which entry/wrapper functions (e.g. `place_limit_order_user`, `place_limit_order_custodian`, `place_limit_order_user_entry`) reach it, and which internal helpers it calls down into (e.g. order-book mutation and the `avl_queue` insert path). Distinguish callers from callees."
3. **[D3 Retrieval]** "Retrieve the full source of the single function `econia::market::place_limit_order`, including its full signature, generic type parameters, and `acquires` clause — exactly that one symbol, not its wrappers." (grep-findable identifier: `place_limit_order`.)
4. **[D4 Architecture]** "Describe the module/file organization of `src/move/econia/sources`: enumerate the top-level modules (`market`, `user`, `registry`, `incentives`, `avl_queue`, `tablist`, `resource_account`, `assets`) and summarize how the data-structure modules (`avl_queue`, `tablist`) sit beneath the domain modules (`market`, `user`) via `use` imports."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Semantic/cross-cutting: across all Econia modules, surface the input-validation / abort pattern — functions that assert on error constants like `E_PRICE_0`, `E_SIZE_TOO_SMALL`, or `E_INVALID_MARKET_ID` [verify] (`assert!(..., E_...)`). Identify clusters of similar validation logic and any near-duplicate guard sequences shared between the user-facing and custodian-facing order paths. (Labeled graph-favoring: relies on semantic similarity + cross-file pattern grouping rather than a single literal lookup.)"

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*place_(limit|market)_order.*", label="Function"); D2->trace_call_path(qualified_name="econia::market::place_limit_order", direction="both"); D3->get_code_snippet(qualified_name="econia::market::place_limit_order"); D4->get_architecture(scope="src/move/econia/sources"); D5->search_code/semantic_query("input validation assert error constant E_ across order paths").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/move-graph.md`, `move-explorer.md`, `move-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the move tier.

---

### 133. squirrel — C (Dynamic & Scripting)

**Repo:** albertodemichelis/squirrel (`/tmp/bench/squirrel`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical reference implementation of the Squirrel scripting language — a small, self-contained C/C++ VM + compiler + stdlib that is widely embedded (games, MMOs) and substantial enough to exercise definition, call-graph, and architecture queries; idiomatic for the "dynamic & scripting language host" niche of Group C.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the implementations of the public embedding-API entry points `sq_open`, `sq_compile`, and `sq_call`. Report the declaring file and full signature of each. (Note: these are *declared* in `include/squirrel.h` but *defined* — with bodies — in `squirrel/sqapi.cpp`.)"
2. **[D2 Relationship]** "Show the call relationships around `SQVM::Execute` (the bytecode interpreter loop) in `squirrel/sqvm.cpp` — both its callers (e.g. `sq_call` / `SQVM::Call`) and the opcode-handler helpers it invokes (both directions)."
3. **[D3 Retrieval]** "Retrieve the full source of the lexer token routine `SQLexer::Lex` in `squirrel/sqlexer.cpp`."
4. **[D4 Architecture]** "Describe the top-level module structure of the project: how `squirrel/` (VM, compiler, lexer, GC), `sqstdlib/` (standard library), `include/` (public headers), and `samples/` relate to each other."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Semantically locate the reference-counting / garbage-collection machinery — find the `SQRefCounted` / `__Release` / mark-and-sweep code (e.g. `SQCollectable`, `SQVM::Finalize`) that is spread across `sqobject.cpp`, `sqstate.cpp`, and `sqvm.cpp` without relying on an exact name match."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="sq_open|sq_compile|sq_call", label="Function"); D2->trace_call_path(qualified_name="SQVM::Execute", direction="both"); D3->get_code_snippet(qualified_name="SQLexer::Lex"); D4->get_architecture(project="squirrel"); D5->search_code/semantic_query("reference counting and garbage collection release path").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/squirrel-graph.md`, `squirrel-explorer.md`, `squirrel-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the squirrel tier.

---

### 134. func — A (Class-based OOP & Contracts)

**Repo:** ton-blockchain/token-contract (`/tmp/bench/func`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical reference Jetton/NFT smart-contract suite for TON, widely forked and the de-facto teaching example for idiomatic FunC contract code — a substantial, multi-file FunC corpus that matches the plan's "popular + idiomatic + substantial" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the message-handler and getter functions defined in the Jetton wallet contract — specifically `recv_internal`, `send_tokens`, `receive_tokens`, `burn_tokens`, `on_bounce`, and the `get_wallet_data` method_id getter. Are all six surfaced as distinct definitions with their file location?"
2. **[D2 Relationship]** "Trace the call relationships around `recv_internal` in `ft/jetton-wallet.fc` (direction=both): which op-dispatch branch functions does it call (`send_tokens`, `receive_tokens`, `burn_tokens`, `on_bounce`), and which helpers do those in turn invoke (`load_data`, `save_data`)?"
3. **[D3 Retrieval]** "Retrieve the full source of the `mint_tokens` function defined in `ft/jetton-minter.fc`."
4. **[D4 Architecture]** "Describe the structure of the `ft/` directory: how do the contract files (`jetton-minter.fc`, `jetton-wallet.fc`, `jetton-minter-discoverable.fc`, `jetton-minter-ICO.fc`) relate to the shared include files (`op-codes.fc`, `params.fc`, `jetton-utils.fc`)?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Both `ft/jetton-minter.fc` and `ft/jetton-wallet.fc` define structurally parallel `load_data`/`save_data` storage-serialization pairs (same begin_cell/end_cell persistence shape, differing storage fields). Use semantic/similarity search to surface this duplicated persistence pattern across the contracts and flag it as a copy-paste / shared-helper candidate."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*(recv_internal|send_tokens|receive_tokens|burn_tokens|on_bounce|get_wallet_data).*"); D2->trace_call_path(qualified_name="recv_internal", direction="both"); D3->get_code_snippet(qualified_name="mint_tokens"); D4->get_architecture(path="ft/"); D5->search_code/semantic_query(query="load_data save_data storage serialization pair").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/func-graph.md`, `func-explorer.md`, `func-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the func tier.

---

### 135. regex — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** tests/eval-fixtures/regex (fixture corpus, see plan section 8.1) (`/tmp/bench/regex`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** No idiomatic standalone regex project exists; per §8.1 the language is exercised against a small curated, version-controlled fixture corpus of representative regex pattern files — the most reproducible stand-in the selection criteria allow for a data/grammar-fragment language. **Indexability precondition [verify]:** the current binary registers the regex grammar but does **not** map any `.regex` file extension in the discovery `EXT_TABLE` (regex is vendored primarily as an embedded/injection grammar). The corpus must therefore be wired to an extension the indexer routes to `CBM_LANG_REGEX` (or that wiring added) before it indexes at all; confirm at execution that the corpus files actually produce nodes.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "For each pattern file in the corpus, report the single `pattern` node the index extracted and the file it lives in (the regex grammar's root node is `pattern`, so expect exactly one per file, named/anchored by its file). Confirm the file→node mapping is 1:1 across the corpus." [verify — exact fixture filenames AND one-pattern-node-per-file behavior confirmed grep-first at execution; both are plain-grep-findable since the files and their contents are flat text]
2. **[D2 Relationship]** "N/A — the regex grammar exposes no call/import/reference edges (only a single root `pattern` node per file is indexed; there are no `include_directive`s or cross-file references in flat pattern files), so a call/relationship graph does not apply. Excluded from this language's mean."
3. **[D3 Retrieval]** "Return the full source of the single largest pattern file in the corpus — expected to be the multi-branch alternation in `url.regex` that matches scheme/host/path. (Plain `cat`/grep can also produce this, by design — D3 is authored grep-symmetric.)" [verify — `url.regex` existing and being the largest pattern confirmed grep-first]
4. **[D4 Architecture]** "Describe how the fixture corpus is organized: which pattern files exist, how they are grouped under `tests/eval-fixtures/regex/`, and whether patterns are split one-per-file or bundled. (Directory-listing question; thin by nature for a flat data corpus — scored accordingly.)"
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Identify near-duplicate or naming-pattern-related patterns across files (e.g. two fixtures that both encode an IPv4 octet sub-expression, or several `*_date.regex` variants), surfacing structural duplication that plain text search would not cluster. Labeled graph-favoring; D5 aggregates within Group E only. [verify — that the curated corpus actually contains such near-duplicate pairs; if it does not, this question is unfair to ask and must be reworded or marked N/A.]"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Module", project="regex") [Module = the `pattern` root node]; D2->trace_call_path(...) [N/A — no edges]; D3->get_code_snippet(qualified_name="<largest pattern file QN>" [verify]); D4->get_architecture(project="regex"); D5->search_code/search_graph(semantic_query=...).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/regex-graph.md`, `regex-explorer.md`, `regex-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 excluded as N/A), D5 within Group E only, Group E, the regex tier.

---

### 136. jsdoc — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** lodash/lodash (`/tmp/bench/jsdoc`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Most-depended-on JS utility lib (60k+ stars); `lodash.js` is exhaustively JSDoc-annotated and the `lib/main/build-doc.js` toolchain consumes those comments via `docdown` — an idiomatic, substantial JSDoc-heavy corpus matching the plan's "popular + idiomatic for the language" selection rule.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level documentation-build entry points defined under `lib/` — specifically the `build-doc.js` module(s) in `lib/main/` (and `lib/fp/build-doc.js` [verify]) and the build-script functions they expose. Are the JSDoc-driven doc generators discoverable as definitions?" (Symmetric / grep-findable: filenames `build-doc.js`, npm script names `doc` / `doc:fp` [verify] in `package.json` — `grep -rn build-doc lib/` and `grep -n '"doc' package.json` locate the same symbols without the graph.)
2. **[D2 Relationship]** "Show the cross-file reference graph for the doc pipeline: how does `lib/main/build-doc.js` reference the `docdown` dependency and the source it documents (`lodash.js`), and how does `lib/fp/build-doc.js` [verify] reference `lib/fp/template/` [verify]? Trace the `require`/include edges in both directions."
3. **[D3 Retrieval]** "Retrieve the full definition of `lib/main/build-doc.js` (the largest doc-generation module in the repo) — its `require` block, the `github`/`site` mode branching, and the docdown invocation." (Symmetric / grep-findable: the file is at a known path; `find lib -name build-doc.js` + a plain read returns the same body the graph snippet would.)
4. **[D4 Architecture]** "Describe the `lib/` build/docs directory organization: the `common/` [verify], `main/`, `fp/` split and how the `build-*.js` roles (`build-dist` [verify], `build-doc`, `build-modules` [verify], `build-site` [verify]) plus `fp/template/` [verify] partition responsibilities."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Identify duplication / parallel-naming between the `main` and `fp` doc toolchains — the near-identical `build-doc.js`/`build-dist.js`/`build-modules.js` [verify] triples under `lib/main/` vs `lib/fp/` [verify] — and link the npm `scripts` config entries (`doc`, `doc:fp` [verify]) to the script files they invoke (config<->code)."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*build-doc.*", label="File|Function"); D2->trace_call_path(name="build-doc", direction="both"); D3->get_code_snippet(qualified_name="lib/main/build-doc.js"); D4->get_architecture(scope="lib/"); D5->search_code/semantic_query("doc build duplication main vs fp; package.json scripts -> lib/*/build-*.js").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/jsdoc-graph.md`, `jsdoc-explorer.md`, `jsdoc-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the jsdoc tier.

---

### 137. rst — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** sphinx-doc/sphinx (`/tmp/bench/rst`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Sphinx is the canonical, most-starred reStructuredText toolchain on GitHub and its own `doc/` tree is large, idiomatic, hand-written `.rst` (toctrees, directives, confval/glossary definitions, cross-file labels) — exactly the substantial, popular Group-E corpus the plan's repo-selection criteria call for.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level definition directives declared in `doc/usage/configuration.rst` — i.e. the `.. confval::` entries such as `extensions`, `html_theme`, `exclude_patterns`, and `root_doc` [verify]. Are they surfaced as named definitions?" (grep-symmetric: `.. confval::` lines are plain text greppable, so the graph holds no unfair advantage on D1.)
2. **[D2 Relationship]** "Starting from `doc/index.rst`, follow the `toctree` includes: which `.rst` documents does the root document pull in (e.g. `usage/index` [verify], `development/index` [verify]), and which documents reference back into `glossary.rst` via the `:term:` role or `.. _label:` cross-references?"
3. **[D3 Retrieval]** "Retrieve the full `.. glossary::` definition block in `doc/glossary.rst` (the single directive that defines terms like `builder`, `domain`, `directive`, and `configuration directory`) — return it verbatim with its boundaries." (grep-symmetric: the block is locatable by grepping `glossary::`, so this is not graph-only.)
4. **[D4 Architecture]** "Describe the organization of the `doc/` tree: the top-level `.rst` files (`index.rst`, `glossary.rst`, `faq.rst`, `latex.rst` [verify]) and the major subdirectories (`usage/`, `development/`, `extdev/`, `internals/`, `man/`, `tutorial/` [verify]). How is the documentation partitioned?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Link the markup to the code that implements it: which `.. confval::` names defined in `doc/usage/configuration.rst` correspond to actual config registrations in the Python source (`app.add_config_value(...)`), and which documented confvals have no matching registration (doc/code drift)? Also flag near-duplicate cross-reference labels (e.g. `_html-options` vs `_html_options` [verify]) — a similarity/config<->code task grep cannot resolve."

**Expected graph tools (hint, not a script):** RST itself is a grep/read corpus — the graph holds no special structural edges for `.rst` markup (no toctree/term edges are modeled), so for D1-D4 a fair run uses `search_code`/`search_graph` text matching plus `get_code_snippet` on `.rst` files and is not expected to beat a careful grep baseline. D1->`search_code(pattern="confval::", file_pattern="*.rst", project="rst")` / `search_graph(name_pattern=".*confval.*", project="rst")`; D2->`search_code(pattern="toctree", project="rst")` + `search_code(pattern=":term:|.. _", regex=true, project="rst")` (read the include/cross-ref lines — these are textual, not graph edges); D3->`search_code(pattern="glossary::", file_pattern="*.rst", project="rst")` then `get_code_snippet` on the block; D4->`get_architecture(project="rst")` for the code side plus directory listing of `doc/` (the doc tree is folder-structured, not a call/import cluster). D5 (the genuinely graph-favoring one)->`search_graph(name_pattern=".*add_config_value.*", project="rst")` + `search_graph(semantic_query=["confval","config","value"], project="rst")` to bridge the markup-name<->`add_config_value` registration set, then text diff for documented-but-unregistered confvals and near-duplicate labels.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/rst-graph.md`, `rst-explorer.md`, `rst-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the rst tier.

---

### 138. beancount — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** beancount/beancount (`/tmp/bench/beancount`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Beancount is the canonical, widely-starred plain-text double-entry accounting system; its example ledgers plus the Bison/Flex grammar (`grammar.y`/`lexer.l`) and the Python parser/options layer are a large, idiomatic body of data/markup + schema artifacts, satisfying the plan's "popular + substantial + idiomatic for the language" repo-selection criterion for Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level directive keywords recognized by the beancount syntax (e.g. `open`, `close`, `balance`, `pad`, `price`, `commodity`, `option`, `plugin`, `include`) as they are declared as tokens/grammar rules in `beancount/parser/grammar.y`. Each keyword is a literal token in the grammar file, so plain text search must surface the same set — graph and grep should agree."
2. **[D2 Relationship]** "N/A for this data/markup language — beancount ledger files have no call/import relationships and the indexer builds no relationship edges among ledger directives, so there is no symmetric 'who-references-whom' structure for the graph to win or lose on. (We keep one genuine relationship probe in code, not data: in `beancount/parser/grammar.y` / `beancount/parser/grammar.c` [verify], which grammar actions invoke the C builder callbacks declared in `beancount/parser/grammar.h` [verify]?)"
3. **[D3 Retrieval]** "Retrieve the full text of the `option(...)` and `plugin(...)` header block (the leading directive cluster, before the first transaction) emitted by the example-ledger generator `examples/example.py` [verify], exactly as written. This is literal source text, so grep over the generator (or over a generated `*.beancount` ledger) must return the same block."
4. **[D4 Architecture]** "Describe how the beancount repo organizes its data/markup and schema artifacts: the example-ledger generator and fixtures under `examples/`, the parser schema (`grammar.y`, `lexer.l`, generated `grammar.c`/`grammar.h`) under `beancount/parser/`, and how `.beancount` data flows from generator → parser → in-memory directives."
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING) Link each ledger-level `option`/`plugin` declaration to the parser code that consumes it: which option names handled in `beancount/parser/options.py` [verify] correspond to the `option` directive tokens parsed in `beancount/parser/grammar.y`, and which plugins named by `plugin` directives are loaded by `beancount/loader.py` [verify]? This config↔code linkage spans the data layer and the Python parser/loader layer — a connection grep cannot assemble in one step."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern="open|close|balance|pad|price|commodity|option|plugin|include"); D2->N/A (no relationship edges in data files; optional code-side probe via trace_path on grammar.c builder callbacks); D3->get_code_snippet(qualified_name="examples/example.py:<header-emitting block>"); D4->get_architecture(scope="examples/ + beancount/parser/"); D5->search_code + trace_path("option/plugin directive ↔ options.py / loader.py consumers").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/beancount-graph.md`, `beancount-explorer.md`, `beancount-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the beancount tier.

---

### 139. mermaid — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** mermaid-js/mermaid (`/tmp/bench/mermaid`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ~70k-star, the de-facto diagram-as-code standard; a large, idiomatic monorepo whose Group-E surface (Markdown docs under `packages/mermaid/src/docs`, `.mmd` diagram fixtures under `docs/diagrams/` and `cypress/platform/dev-diagrams/`, plus the YAML config schema and VitePress build config) mirrors the plan's "popular + substantial + idiomatic markup/config" selection criterion.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Enumerate the per-diagram-type doc pages under `packages/mermaid/src/docs/syntax/` — e.g. `flowchart.md`, `sequenceDiagram.md`, `classDiagram.md`, `stateDiagram.md`, `gantt.md` — and the top-level diagram-type headings each declares. Which markup definitions exist as top-level doc/diagram definitions? (Symmetric: grep-findable too — these filenames and their `# <Diagram>` H1 headings are literal text; this dimension must be answerable by plain grep, not graph-only. Caveat: whether the graph emits `Definition` nodes for Markdown headings is itself dubious for a docs language [verify].)"
2. **[D2 Relationship]** "N/A — mermaid's Group-E surface is Markdown docs + `.mmd`/YAML data. Doc pages do cross-reference each other (VitePress sidebar entries in `.vitepress/config.ts` [verify], inter-page Markdown links, embedded code-fence diagram snippets), but these are markup link strings and build-config arrays, not CALLS/IMPORTS/reference edges the code graph models between `.md`/`.mmd` files — there is no doc-include or doc-link edge for `trace_path` to traverse. Forcing a `trace_call_path`-style 'include graph' question would invent a graph capability that does not exist for plain Markdown and would set the graph up to fail; the genuine doc↔snippet and config↔code linkage is instead surfaced as the explicitly graph/semantic-favoring D5 below. (Best-effort, non-scoring sub-probe: which pages does `intro/index.md` link to — answerable by either tool from literal Markdown link text.)"
3. **[D3 Retrieval]** "Retrieve a single large diagram-definition fixture verbatim — the `docs/diagrams/flowchart-code-flow.mmd` file (the flowchart `.mmd` under the repo's `docs/diagrams/` directory), or a large flowchart fixture under `cypress/platform/dev-diagrams/` [verify] — and show its complete node/edge declaration block. (Symmetric: grep-findable too — the `.mmd` filename is a literal anchor; verbatim retrieval is the test, not structure. NOTE: `demos/` contains only `.html` demo files, not `.mmd`, so do not cite `demos/*.mmd`.)"
4. **[D4 Architecture]** "Describe the file/dir organization of the documentation + diagram-fixture surface: how `packages/mermaid/src/docs/` (syntax/, config/, ecosystem/, intro/, .vitepress/), the `.mmd` fixtures under `docs/diagrams/` and `cypress/platform/dev-diagrams/`, and the `demos/*.html` examples are arranged, and how the docs source maps to the generated site tree."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Across the docs + fixtures, find duplicated or near-duplicate diagram examples (the same flowchart/sequence snippet copied across multiple `.md`/`.mmd` files) and surface config↔code links: which doc-config keys documented under `packages/mermaid/src/docs/config/` (e.g. `flowchart.curve`, `theme`, `securityLevel`) correspond to actual config schema fields defined in the source (`packages/mermaid/src/config.type.ts` and `packages/mermaid/src/schemas/config.schema.yaml` [verify])? Explicitly graph/semantic-favoring (snippet similarity + config-key↔schema-field mapping is not visible to plain grep)."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern=".*flowchart.*|.*sequence.*", project="mermaid") [verify the graph emits Definition nodes for Markdown headings; fall back to search_code on the literal filenames/H1s]; D2->N/A (no call/reference graph between Markdown/.mmd docs — acknowledge, do not invoke trace_call_path; see D2 note); D3->get_code_snippet(qualified_name="docs/diagrams/flowchart-code-flow.mmd" [verify .mmd files are snippet-addressable; else search_code on the filename]); D4->get_architecture(project="mermaid", scope="docs + diagrams + demos tree"); D5->search_code / search_graph(semantic_query="near-duplicate diagram snippets + doc-config-key↔config.schema.yaml field linkage").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/mermaid-graph.md`, `mermaid-explorer.md`, `mermaid-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 counted as N/A for mermaid), D5 within Group E only, Group E, the mermaid tier.

---

### 140. puppet — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** puppetlabs/puppetlabs-apache (`/tmp/bench/puppet`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, most-installed Puppet Forge module (Apache httpd), large and idiomatic, exercising classes, defined types, params patterns, and ERB templates across many `.pp` manifests.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level definitions in `manifests/` — specifically the classes and defined types named `apache` (`manifests/init.pp`), `apache::params` (`manifests/params.pp`), `apache::service` (`manifests/service.pp`), `apache::vhost` (`manifests/vhost.pp`), and `apache::mod` (`manifests/mod.pp`). Are both the `class` and `define` constructs surfaced as definitions (e.g. `apache::vhost` and `apache::mod` are `define`d types, `apache`/`apache::service` are classes)?"
2. **[D2 Relationship]** "Show the cross-file relationships for `apache::vhost`: which manifests it references (e.g. its dependence on `apache::params` defaults and the `apache::mod`/`apache::mod::*` resources it can pull in) and which definitions reference it back. Traverse references in both directions."
3. **[D3 Retrieval]** "Retrieve the full definition of the defined type `apache::vhost` (`manifests/vhost.pp`) — by far the largest definition in this module, with hundreds of parameters."
4. **[D4 Architecture]** "Describe the file/directory organization of the module: the role of `manifests/`, `templates/`, `files/`, `lib/`, and `spec/`, and how the `manifests/mod/` subdirectory groups the many `apache::mod::*` sub-classes."
5. **[D5 Cross-cutting/Semantic]** "[graph-favoring] Find duplication and naming-pattern structure across the `apache::mod::*` family (e.g. `apache::mod::php`, `apache::mod::ssl`, `apache::mod::proxy`): which sub-classes share the same `apache::mod { ... }` resource scaffold, and which manifests pair a `.pp` class under `manifests/mod/` with a same-named ERB template under `templates/mod/`. This config<->template linkage is what plain grep cannot stitch together in one shot."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern="apache(::params|::service|::vhost|::mod)?"); D2->trace_path(name="apache::vhost", direction="both") [verify: Puppet relationship edges depend on IMPORTS/DEFINES coverage, not CALLS]; D3->get_code_snippet(qualified_name="apache::vhost"); D4->get_architecture(scope="manifests"); D5->search_graph(name_pattern="apache::mod::.*") + search_code for the `templates/mod/*.erb` pairing.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/puppet-graph.md`, `puppet-explorer.md`, `puppet-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the puppet tier.

---

### 141. po — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** django/django (`/tmp/bench/po`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Django ships hundreds of GNU gettext `.po` catalogs (`django/conf/locale/*/LC_MESSAGES/django.po`, `djangojs.po`, plus per-contrib-app catalogs) — the largest, most idiomatic, most-starred corpus of real-world PO data in OSS, matching the plan's "popular + substantial in-language" repo-selection criterion.

**Indexer note (sets fair expectations):** `.po` is registered as a parsed-but-structurally-empty language in the indexer — its lang spec uses `empty_types` for defs/imports/calls and only `{"source_file"}` as a module type. The graph therefore produces a **file/containment node per `.po` catalog and nothing finer** (no `msgid` defs, no `msgid`↔source edges, no config↔code edges). Questions below are authored to that reality: D1/D3 are grep-symmetric content lookups, D4 leverages the real folder/file containment tree, and D2/D5 are honest N/A because the graph indexes no relationships or call-site joins for this pure data format.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level header metadata fields and the first translatable `msgid` units in `django/conf/locale/de/LC_MESSAGES/django.po` — e.g. the header keys `Project-Id-Version`, `Content-Type`, `Plural-Forms`. These are plain `msgid`/`msgstr` tokens findable by grep as well as by reading the file node the graph records for this catalog." [verify]
2. **[D2 Relationship]** "N/A — `.po` is a flat gettext data catalog and the indexer extracts no symbol-to-symbol relationships from it (empty def/import/call types; only a file node). The `#:` source-reference comments are plain text, not graph edges, so there is no in-graph `msgid`↔source-location relationship to traverse. Forcing a `trace_call_path` question here would test a capability the PO indexer does not provide."
3. **[D3 Retrieval]** "Retrieve the single largest translation unit in `django/conf/locale/de/LC_MESSAGES/django.po` — the multi-line `msgid \"\"`/`msgstr \"\"` block for a long admin help message such as `\"You are authenticated as %(username)s, but are not authorized...\"` — including its full `msgctxt`/comment header. A grep for the message text plus surrounding-line read locates it; the graph contributes only the catalog file node." [verify]
4. **[D4 Architecture]** "Describe the locale catalog organization the graph's folder/file containment tree exposes: how `.po` files are partitioned by language directory (`conf/locale/<lang>/LC_MESSAGES/`), by domain (`django.po` vs `djangojs.po`), and by contrib app (`contrib/admin/locale/`, `contrib/auth/locale/`, `contrib/gis/locale/`). How many language directories and domain files exist?"
5. **[D5 Cross-cutting/Semantic]** "N/A — there is no honest graph-favoring cross-cutting question for `.po` in this indexer. The tempting framing (link duplicated `msgid` strings back to their originating `gettext()`/`_()` call sites as a config↔code edge) describes an edge the PO indexer does **not** build: `.po` files yield no defs and no cross-file edges, so any such join would be done by grep/text-similarity over file content, not by the graph. Marking N/A rather than rigging a question toward a capability the graph lacks."

**Expected graph tools (hint, not a script):** D1->read the catalog file node + grep `msgid`/header keys (graph stores the file node, not per-`msgid` defs); D2->N/A (no relationship edges for `.po`); D3->locate the largest `msgid`/`msgstr` block via grep + file read (no per-block qualified name in the graph); D4->get_architecture (real `conf/locale` containment tree by lang/domain/app); D5->N/A (no config↔code or duplicate-join edges; any such analysis is grep/similarity over content).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/po-graph.md`, `po-explorer.md`, `po-judged.json`.
**Aggregates into:** D1 & D3 cross-group content-retrieval rollups, D4 architecture rollup, Group E, the po tier. D2/D5 recorded as N/A (excluded from scored aggregates).

---

### 142. gitattributes — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** alexkaratarakis/gitattributes (`/tmp/bench/gitattributes`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, widely-starred community collection of `.gitattributes` templates (the de-facto counterpart to `github/gitignore`); substantial, idiomatic, and purely declarative config — an honest stress test of structural retrieval on attribute/glob data with no call graph, matching the plan's "popular + idiomatic + group-representative" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the per-language/per-platform `.gitattributes` template files defined in this collection (e.g. `Common.gitattributes`, `Web.gitattributes`, `CSharp.gitattributes`) — i.e. the attribute definitions a consumer would pick from, including those under the `Global/` and `community/` subdirectories." (grep-findable: filenames + the literal `text=auto` / `eol=lf` directives inside them.)
2. **[D2 Relationship]** "N/A — `.gitattributes` files are standalone glob→attribute declarations with no include/import mechanism; they do not reference one another, so there is no cross-file reference/call graph to traverse. Excluded from the mean."
3. **[D3 Retrieval]** "Retrieve the full contents of `Common.gitattributes` (the shared baseline that opens with `* text=auto` normalization, then sets `diff=astextplain`/`-text`/`binary` rules for documents, graphics, archives and other binary file types)." (grep-findable: the filename and its `* text=auto` first line.)
4. **[D4 Architecture]** "Describe the layout of the collection: the root-level `*.gitattributes` templates (one per language/platform), the `Global/` directory (editor/IDE templates such as `VisualStudio.gitattributes`), the `community/`-contributed templates, the `README.md`, and the `.github/` + `check.sh` contribution/CI tooling — how the repo is organized for a consumer browsing by language/platform." [verify: exact subfolder set]
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring — semantic/duplication) Across all templates, find duplicated or near-duplicated rule blocks and recurring attribute patterns — e.g. the same `* text=auto` header, the `binary`/`-text` macro usage, and `diff=astextplain` / `linguist-*` directives repeated across many files — and surface which templates are structurally most similar. This rewards similarity/duplication detection over plain text search."

**Expected graph tools (hint, not a script):** D1->search_graph(label="File"/top-level definitions, name_pattern=".*\\.gitattributes"); D2->trace_call_path(...) [N/A — no edges]; D3->get_code_snippet(qualified_name="Common.gitattributes"); D4->get_architecture(...); D5->search_code/semantic_query(duplication & recurring-attribute patterns).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/gitattributes-graph.md`, `gitattributes-explorer.md`, `gitattributes-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 excluded as N/A), D5 within Group E only, Group E, the gitattributes tier.

---

### 143. gitignore — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** github/gitignore (`/tmp/bench/gitignore`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, ~160k-star community collection of `.gitignore` templates — the most idiomatic and substantial gitignore corpus in existence, matching the plan's "most-popular + idiomatic + substantial" repo-selection criteria for Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level language template files this repo defines (e.g. `Python.gitignore`, `Node.gitignore`, `Java.gitignore`, `C++.gitignore`) and confirm each is a distinct top-level template entry, not a duplicate of a `Global/` template."
2. **[D2 Relationship]** "Identify cross-file reference structure: does any template in `Global/` (e.g. `macOS.gitignore`, `JetBrains.gitignore` [verify]) get composed/referenced by language templates, or are the `community/` subcategory templates self-contained with no include relationships? Characterize whether gitignore templates form a reference graph at all."
3. **[D3 Retrieval]** "Retrieve the full contents of the largest single template definition — `Global/JetBrains.gitignore` [verify] (or `Android.gitignore` if larger [verify]) — and report its rule count."
4. **[D4 Architecture]** "Describe the repository's file/dir organization: the top-level language templates vs the `Global/` directory (editor/OS templates) vs the `community/` directory and its subcategories. How many entries live at each tier?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find near-duplicate or naming-pattern-clustered templates: which language templates share substantially overlapping ignore patterns (e.g. multiple JVM templates emitting `*.class`, `target/`; multiple JS templates emitting `node_modules/`), and which `community/` templates duplicate a top-level template's intent under a different name?"

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition"/file pattern `*.gitignore`); D2->trace_call_path(direction=both) — expected to return ~no edges, confirming the N/A-leaning nature of references here; D3->get_code_snippet(qualified_name="Global/JetBrains.gitignore"); D4->get_architecture(); D5->search_code/semantic_query for duplicated ignore-rule clusters.

**Note on D2/D5 applicability:** gitignore templates are largely self-contained flat rule lists with no native include/reference mechanism, so D2 is expected to be near-N/A (excluded from the mean if the graph surfaces no genuine cross-file edges) — this is a fair structural test of whether the tool over-claims relationships. D5 is the openly graph-favoring dimension (duplication/naming-cluster detection across many flat files), scored within Group E only.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/gitignore-graph.md`, `gitignore-explorer.md`, `gitignore-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the gitignore tier.

---

### 144. slang — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** shader-slang/slang (`/tmp/bench/slang`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** shader-slang/slang is the reference open-source Slang shading-language compiler (high-star, NVIDIA-backed, Khronos-adopted), and its `.slang` core/stdlib `.meta.slang` modules are the largest idiomatic body of real Slang source in the wild — matching the plan's "popular + substantial + idiomatic" repo-selection criteria for an HDL/shader Group-E target.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level `interface` definitions declared in `source/slang/core.meta.slang` — e.g. does the graph surface `IDifferentiable`, `IFloat`, `IArithmetic`, and `IComparable` as definition nodes?" (all grep-findable as literal `interface I...` declarations)
2. **[D2 Relationship]** "Show the cross-file conformance relationship from `diff.meta.slang` to `core.meta.slang`: which types in `diff.meta.slang` conform to / reference `IDifferentiable` declared in `core.meta.slang` — e.g. `NullDifferential` and the autodiff extensions on `Array`/`Optional`/`Tuple`? (structural: conformance/reference edges between the two stdlib modules; note Slang's core stdlib has no explicit `import`/`#include` lines, so this is a name-resolution relationship the graph must reconstruct.)"
3. **[D3 Retrieval]** "Retrieve the full definition of the `struct DifferentialPair` declaration from `source/slang/core.meta.slang`." (single named symbol, grep-findable as `struct DifferentialPair`)
4. **[D4 Architecture]** "Describe the organization of the Slang standard-library `.meta.slang` modules under `source/slang/` (`core.meta.slang`, `hlsl.meta.slang`, `glsl.meta.slang`, `diff.meta.slang`) and how they partition core types vs. target-specific bindings vs. autodiff support."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find the differentiable-arithmetic interface family — definitions semantically related to `IDifferentiable` (e.g. `IFloat`, `IArithmetic`, `__BuiltinFloatingPointType`) and the conformance/naming pattern (`I*` interface prefix) that links the core type hierarchy across the `.meta.slang` modules. Surfaces duplication/naming-pattern links a plain grep for one name would miss."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Interface"/"Definition", name_pattern="I.*", file=".*core\\.meta\\.slang"); D2->trace_path(from="NullDifferential"/diff.meta.slang autodiff extensions, to="IDifferentiable", direction=both) + search_graph(relationship conformance to IDifferentiable); D3->get_code_snippet(qualified_name="DifferentialPair"); D4->get_architecture(scope="source/slang", focus=".meta.slang modules"); D5->search_code/semantic_query("differentiable arithmetic interface family I* prefix").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/slang-graph.md`, `slang-explorer.md`, `slang-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the slang tier.

---

### 145. llvm_ir — B (Systems & Low-level)

**Repo:** llvm/llvm-project (subset .ll tests) (`/tmp/bench/llvm_ir`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical LLVM source tree; its `test/` `.ll` corpus is the largest body of idiomatic LLVM IR in existence, exercising every IR construct (define/declare/call/invoke/global/type) — ideal for stressing IR symbol extraction per the plan's "popular + substantial + idiomatic" repo criterion. (Note: many `.ll` files carry `; NOTE: Assertions have been autogenerated by utils/update_test_checks.py` headers, so the corpus mixes hand-written and tool-generated IR.)

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the function definitions in the IR — e.g. find the `@main` define and any `@test`/`@foo`/`@bar` functions declared with `define` in the indexed `.ll` files, and distinguish them from external `declare`d symbols like `@printf` or `@llvm.memcpy.p0.p0.i64` [verify] (the exact intrinsic mangling is LLVM-version-specific)." (grep-findable: `define`, `@main`, `@printf` are plain text tokens. Note: the indexer extracts Function nodes from `function_header`; whether the stored node name retains the `@` sigil (`@main`) or is stripped to `main` [verify] affects exact-name lookups below.)
2. **[D2 Relationship]** "For a function such as `@main`, show its call graph in both directions: which functions it reaches via `call`/`invoke` instructions, and which functions call into it. Verify a representative `call @callee` edge resolves to a `define @callee` in the same module." (The LLVM lang-spec models `call`/`invoke` as CALL edges, so this is graph-supported; use the indexed function name form — `@main` or `main` [verify] — when invoking trace_call_path.)
3. **[D3 Retrieval]** "Retrieve the full body of the single named symbol `@main` (or the largest `define` in the file) — its complete instruction sequence from the opening `{` to the terminating `ret`/`unreachable`." (`@main` is a grep-findable identifier; pass the indexed name form — `@main` or `main` [verify] — to get_code_snippet.)
4. **[D4 Architecture]** "Describe the structure of the indexed IR subset: how `.ll` test files are organized under `test/` (e.g. `test/CodeGen/`, `test/Transforms/`, `test/Verifier/`), and per file the split between defined functions, external declarations, global variables (`@.str`), and named struct types (`%struct.*`)."
5. **[D5 Cross-cutting/Semantic]** "[GRAPH-FAVORING] Find IR functions that are structurally/semantically similar — e.g. near-duplicate `define`s that differ only in a constant or type, or recurring patterns like the `entry:`/`for.body:`/`for.cond:` loop-block skeleton — and surface clusters a plain grep cannot, linking RUN-line comments (`; RUN: opt -passes=...` or `; RUN: llc ...`) to the IR they exercise. Note: in fast (non-LSP) mode `SIMILAR_TO`/`SEMANTICALLY_RELATED` edges are typically absent [verify], so the clustering leans on Function-node naming families plus `search_code` semantic queries, not on a dedicated similarity edge."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Function", name_pattern="@?(main|test.*|foo|bar)"); D2->trace_call_path(name="@main" or "main" [verify], direction="both"); D3->get_code_snippet(qualified_name="@main" or "main" [verify]); D4->get_architecture(path="test/"); D5->search_code(semantic_query="duplicate IR loop skeleton")/search_graph(semantic_query=...) (similarity edges sparse in fast mode [verify]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/llvm_ir-graph.md`, `llvm_ir-explorer.md`, `llvm_ir-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group B only, Group B, the llvm_ir tier.

---

### 146. smithy — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** smithy-lang/smithy (`/tmp/bench/smithy`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Smithy is AWS's widely-adopted interface-definition language (IDL) and code-gen toolkit (>2k stars, basis for AWS SDK modeling); its large corpus of idiomatic `.smithy` schema files plus a substantial Gradle Kotlin-DSL multi-module Java build makes it the canonical "schema + build config" exemplar for Group E, matching the plan's criterion of popular, idiomatic, substantial repos.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level prelude shape definitions declared in `smithy-model/src/main/resources/software/amazon/smithy/model/loader/prelude.smithy` — at minimum the scalar shapes `string String` and `integer Integer`, the string-typed trait `string documentation`, and the structure traits `structure required` and `structure http` — and report the shape ID and shape kind (string / integer / structure) for each." (grep-symmetric: `string String`, `integer Integer`, `structure http`, `@trait` are all plain-text findable in the prelude file.)
2. **[D2 Relationship]** "For the `smithy.api#http` trait defined in the prelude, map the cross-file `use` / `apply` reference relationships: which `.smithy` model and test files reference `smithy.api#http` (via `@http` application or an explicit `use smithy.api#http`), and how prelude trait shapes are pulled into service/operation models — direction=both." (Note: trait shapes live in `prelude.smithy` under the loader resources dir; there is no separate `traits.smithy` in that directory [verify].)
3. **[D3 Retrieval]** "Retrieve the full definition of the `smithy.api#http` trait shape — the `structure http` with its `method`, `uri`, and `code` members — from `prelude.smithy`. One named symbol." (grep-symmetric: `structure http {` is plain-text findable in the prelude file.)
4. **[D4 Architecture]** "Describe the module/directory organization of the repository: the Gradle multi-module layout (`smithy-model`, `smithy-build`, `smithy-cli`, `smithy-codegen-core`, `smithy-aws-traits`, etc.), where `.smithy` schema resources live under `src/main/resources/...` vs. Java sources under `src/main/java/...`, and how `settings.gradle.kts` (Gradle Kotlin DSL) wires the modules."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Surface config↔code links and duplication across the schema/implementation boundary: which prelude trait IDs have a corresponding Java implementation class under `smithy-model/src/main/java/software/amazon/smithy/model/traits/` (e.g. `smithy.api#http` ↔ `HttpTrait.java`, `smithy.api#documentation` ↔ `DocumentationTrait.java`) [verify], and surface near-duplicate `structure`/member naming patterns across model files. Labeled graph-favoring: this is a cross-language schema-to-implementation link plus semantic-similarity ranking that plain grep cannot resolve by name alone."

**Expected graph tools (hint, not a script):** D1->search_graph(label/kind="definition", file=".*prelude\\.smithy"); D2->trace_call_path(direction="both", symbol="smithy.api#http") plus query_graph for IMPORTS/`use` edges (smithy has DEFINES + reference edges, not CALLS); D3->get_code_snippet(qualified_name="smithy.api#http"); D4->get_architecture(scope="modules+resource-dirs"); D5->search_code/semantic_query("smithy trait declaration vs Java trait implementation class").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/smithy-graph.md`, `smithy-explorer.md`, `smithy-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the smithy tier.

---

### 147. wit — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** bytecodealliance/wit-bindgen (`/tmp/bench/wit`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical Bytecode Alliance WIT binding generator — the most-starred, most-idiomatic corpus of hand-written `.wit` interface-definition files, exercising the full WIT surface (package/world/interface/record/variant/resource), matching the plan's "popular + substantial + idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Enumerate the top-level WIT definitions in this repo: list the `world` and `interface` declarations, e.g. the `imports`/`exports` worlds and named interfaces under `tests/codegen/*.wit` and `crates/*/tests/**/*.wit`. (grep-findable: lines starting with `world ` / `interface ` / `package `.)"
2. **[D2 Relationship]** "For an interface that is consumed elsewhere, map its cross-file reference graph: which worlds/interfaces pull it in via `use pkg/iface.{...}` or `import`/`export`, and which `package` namespace it belongs to (e.g. references into a shared `types`/`resources` interface)? [verify exact interface name against the pinned commit]"
3. **[D3 Retrieval]** "Retrieve the full body of the largest single WIT definition in `tests/codegen/resources.wit` — its `resource` block(s) with their constructor and method declarations. [verify file path/name `resources.wit`]"
4. **[D4 Architecture]** "Describe the file/dir organization of the WIT corpus: how `.wit` files are grouped across `tests/codegen/`, `crates/<lang-backend>/tests/`, and any top-level `wit/` directory, and how `package <ns>:<name>` declarations partition the namespace."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Surface duplication and naming-pattern links across the `.wit` corpus: which interfaces/worlds are near-duplicate fixtures (same record/variant shapes under different package names) and where the same `record`/`enum` name recurs across files — the kind of similarity/config↔fixture link a grep cannot cluster."

**Expected graph tools (hint, not a script):** D1→search_graph(label/top-level definition pattern `world|interface|package`); D2→trace_call_path(direction=both, on the chosen interface's cross-file `use`/`import` edges); D3→get_code_snippet(qualified_name of the `resources.wit` resource block); D4→get_architecture(scope=`.wit` tree); D5→search_code/semantic_query(duplicate-fixture & recurring-record clustering).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/wit-graph.md`, `wit-explorer.md`, `wit-judged.json`.
**Aggregates into:** D1–D4 cross-group rollups, D5 within Group E only, Group E, the wit tier.

---

### 148. tlaplus — D (Functional & Formal)

**Repo:** tlaplus/Examples (`/tmp/bench/tlaplus`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The canonical, community-curated collection of TLA+ specifications (Paxos, TwoPhase, DieHard, dijkstra-mutex), idiomatic and substantial — satisfies the plan's "popular + representative formal-language corpus" selection criterion.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Find every TLA+ module that declares an action operator named `Next` (the standard top-level next-state relation) and an `Init` predicate — list the modules defining both. Both `Init` and `Next` are literal `==`-defined operators, so grep can find them too."
2. **[D2 Relationship]** "Starting from the `Spec` definition in `specifications/Paxos/Paxos.tla`, trace its operator-reference graph in both directions: which operators `Spec` references (e.g. `Init`, `Next`) and which sub-actions (`Phase1a`, `Phase1b`, `Phase2a`, `Phase2b`) are reached transitively through `Next`."
3. **[D3 Retrieval]** "Retrieve the full definition of the `TypeOK` invariant operator in `specifications/DieHard/DieHard.tla`. `TypeOK` is a literal `==`-defined operator name, so plain grep (`grep -n 'TypeOK ==' specifications/DieHard/DieHard.tla`) can locate its definition line too."
4. **[D4 Architecture]** "Describe the directory/module organization of the `specifications/` tree: how individual protocol specs (Paxos, TwoPhase, transaction_commit) are grouped, and how `.tla` modules relate to their accompanying `.cfg` model-check configs."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Across all specs, find modules that semantically implement a two-phase-commit / consensus pattern — i.e. distinct files defining structurally similar `Init`/`Next`/message-passing operators — even when operator names differ. This relies on semantic similarity and cross-file naming patterns that plain grep cannot cluster."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="Next|Init", label="Definition"); D2->trace_call_path(qualified_name=".../Paxos/Spec", direction="both"); D3->get_code_snippet(qualified_name=".../DieHard/TypeOK"); D4->get_architecture(scope="specifications/"); D5->search_code/semantic_query("two-phase commit / consensus Init Next message-passing").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/tlaplus-graph.md`, `tlaplus-explorer.md`, `tlaplus-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group D only, Group D, the tlaplus tier.

---

### 149. pkl — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** apple/pkl-pantry (`/tmp/bench/pkl`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Apple's official monorepo of first-party Pkl packages — the canonical, idiomatic, substantial pkl corpus (real schema/config modules, classes, typealiases), satisfying the plan's "popular + idiomatic + non-trivial size" repo-selection criteria for the language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level module-level definitions (classes / typealiases / properties) declared in the `org.json_schema` package's `JsonSchema.pkl` module — e.g. the `JsonSchema` class and the `Schema` typealias [verify]. Are they surfaced as graph nodes the way grep finds their `class`/`typealias` keyword declarations?"
2. **[D2 Relationship]** "Starting from the `org.json_schema.contrib` package's converter module (`generate.pkl` / `Generator.pkl` [verify]), trace the cross-file `import` (and source-level `amends`/`extends`) references both ways via IMPORTS edges: which modules it pulls in (e.g. `import \"@json_schema/JsonSchema.pkl\"` [verify]) and which modules import it back. (Note: pkl emits IMPORTS edges, not CALLS — this is a module-dependency relationship, not a function call graph.)"
3. **[D3 Retrieval]** "Retrieve the full source of the single largest definition in `org.openapi.v3` — the `Schema` class in `Schema.pkl` [verify] — by its qualified name, returning only that class body, not the whole file."
4. **[D4 Architecture]** "Describe the package/directory organization of pkl-pantry: how the per-package roots (`packages/<name>/PklProject`) map to module files and the `tests/` trees, and how `PklProject` / `PklProject.deps.json` tie a package to its dependencies."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find structurally duplicated or near-duplicate schema definitions across packages — e.g. recurring `nullable`/`Boolean` property shapes or the repeated `output { renderer = ... }` config<->renderer link pattern — and cluster modules by naming/shape similarity. Plain grep can match the literal keyword but cannot cluster by structural similarity."

**Expected graph tools (hint, not a script):** D1->search_graph(label/name_pattern for class/typealias in JsonSchema.pkl); D2->trace_path(relationship=IMPORTS, direction=both on the converter module) or query_graph over IMPORTS edges; D3->get_code_snippet(qualified_name of org.openapi.v3 Schema class); D4->get_architecture(package/dir rollup); D5->search_code/semantic_query(structural duplication + config<->renderer link).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/pkl-graph.md`, `pkl-explorer.md`, `pkl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the pkl tier.

---

### 150. gomod — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** go-chi/chi (symlink go) (`/tmp/bench/gomod`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** go-chi/chi is one of the most-starred idiomatic Go HTTP routers, and its root `go.mod` is a deliberately **dependency-free** manifest (`module github.com/go-chi/chi/v5` + a `go` version directive, with no `require` block and no populated `go.sum`). That makes it a clean, popular exemplar of the *minimal* gomod manifest format — and an honest stress test of whether the tool over-claims `require`/`go.sum`/cross-module structure that simply isn't there.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level directives declared in this repo's root `go.mod`: the `module` path (`github.com/go-chi/chi/v5`) and the `go` language-version directive — both grep-findable tokens on dedicated lines. Note: chi declares **no** `require` directives (it is dependency-free), so confirm the absence of a `require` block rather than enumerating non-existent entries."
2. **[D2 Relationship]** "N/A — chi's `go.mod` declares no `require`/`replace` directives and its `go.sum` is empty, and chi is a single-module repo with no nested manifests; there are therefore no checksum lines to correlate, no nested `go.mod` to cross-reference, and (as a library) no import sites of its own module path within its tree. A gomod manifest has no native call/reference graph to traverse here. Excluded from the mean. (A fair test of whether the tool fabricates module-resolution edges that don't exist.)"
3. **[D3 Retrieval]** "Retrieve the **entire** root `go.mod` verbatim with its delimiters — for chi this is the full (tiny) file: the `module` line and the `go` directive. (grep-findable: the filename and the literal `module github.com/go-chi/chi/v5` first line.) There is no grouped `require ( ... )` block to extract."
4. **[D4 Architecture]** "Describe the module/build organization expressed by the manifest: chi is a **single Go module** rooted at one `go.mod`, with `middleware/` as a same-module subpackage (not a nested module) and no separate per-directory `go.mod`. Characterize how the directory layout maps to the declared module path `github.com/go-chi/chi/v5`, and confirm there are no nested manifests — a single-module finding is itself the correct structural answer."
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) N/A for this repo — a config↔code drift / duplicate-`require` analysis needs declared dependencies and (ideally) multiple manifests to join on, and chi has neither (zero `require`s, single manifest). With no config↔code dependency edges to join, there is nothing for similarity/drift detection to surface; report this honestly rather than inventing unused-dependency findings. Scored within Group E only; excluded from the mean as N/A. (On a dependency-heavy multi-module repo this dimension would become the openly graph-favoring config↔code-edge join.)"

**Note on D2/D5 applicability:** chi's `go.mod` is the *minimal* manifest shape — dependency-free and single-module — so D2 (cross-module/cross-file reference resolution) and D5 (config↔code drift / cross-manifest duplication) are genuinely N/A here and are excluded from the mean. This is intentional: the gomod tier tests honest reporting on a manifest with nothing to resolve, not graph cleverness on manufactured structure. D1 (directives) and D3 (verbatim retrieval) remain grep-findable and symmetric.

**Expected graph tools (hint, not a script):** D1->search_graph(label="Module"/"Definition", name_pattern="(module|go)"); D2->trace_call_path(...) [N/A — no module-resolution edges expected; passing = the tool surfaces none]; D3->get_code_snippet(qualified_name="go.mod"); D4->get_architecture(project="gomod"/scope="modules"); D5->search_code/semantic_query(...) [N/A — no config↔code dependency edges to join].

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/gomod-graph.md`, `gomod-explorer.md`, `gomod-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 excluded as N/A), D5 within Group E only (excluded as N/A), Group E, the gomod tier.

---

### 151. apex — A (Class-based OOP & Contracts)

**Repo:** trailheadapps/ebikes-lwc (`/tmp/bench/apex`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Salesforce's flagship LWC sample app (~840 stars, official trailheadapps), with idiomatic Apex controllers backing the Lightning Web Components — `@AuraEnabled` service classes, a `PagedResult` DTO, and a `Cacheable` SOQL layer — matching the plan's "popular + idiomatic + non-trivial OOP" repo-selection criteria for the class-based tier. (Note: this repo is controller-oriented; it ships no Apex triggers or trigger-handler framework, so questions are anchored to the real controller/DTO classes.)

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the public methods exposed by the `ProductController` class (`getProducts`, `getSimilarProducts`) and confirm which are annotated `@AuraEnabled` (and with what cache settings, e.g. `Cacheable=true`)." — symbols are grep-findable identifiers in `ProductController.cls`.
2. **[D2 Relationship]** "Show the full caller/callee context of `ProductController.getProducts`: which inner type it consumes (`ProductController.Filters`), which class it returns/constructs (`PagedResult`), and confirm whether `getSimilarProducts` shares any of those callees."
3. **[D3 Retrieval]** "Retrieve the exact source of `ProductRecordInfoController.getRecordInfo` with precise line boundaries." — exact qualified name, grep-findable in `ProductRecordInfoController.cls`.
4. **[D4 Architecture]** "Describe how the Apex layer is organized under `force-app/main/default/classes/`: the controller classes (`ProductController`, `OrderController`, `ProductRecordInfoController`, `CommunitiesLandingController`), the `PagedResult` DTO and `HeroDetailsPositionCustomPicklist` helper, and the `Test*` / `*Test` test classes — and how the test classes map onto the controllers."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across the controller classes, find the methods that share the same repeated SOQL-against-`Product__c` + `WITH USER_MODE` retrieval pattern (e.g. in `ProductController` and `ProductRecordInfoController`), surfacing near-duplicate query intent that does not share a method name." — semantic/similarity query; not reliably reachable by plain grep.

**Expected graph tools (hint, not a script):** D1->search_graph(label="Method", name_pattern=".*ProductController.*"); D2->trace_call_path(qualified_name="ProductController.getProducts", direction="both"); D3->get_code_snippet(qualified_name="ProductRecordInfoController.getRecordInfo"); D4->get_architecture(path="force-app/main/default/classes"); D5->search_code/semantic_query("repeated Product__c SOQL WITH USER_MODE retrieval pattern").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/apex-graph.md`, `apex-explorer.md`, `apex-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the apex tier.

---

### 152. soql — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** trailheadapps/ebikes-lwc (symlink apex) (`/tmp/bench/soql`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** Salesforce's flagship LWC sample app (~3k stars, maintained by the official trailheadapps org); its Apex layer is dense with idiomatic *embedded* SOQL, making it the most substantial real-world SOQL corpus available where queries live inside `.cls` host files (indexed via the `apex` symlink).

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the SOQL `SELECT` statements that query the `Product__c` custom object across the Apex classes — which methods embed them (e.g. in `ProductController`)?" (grep-findable: the literal `Product__c` and `SELECT` tokens appear verbatim in source, as do the method names.)
2. **[D2 Relationship]** "For the embedded query inside `ProductController.getProducts`, show the cross-file relationship: which `__c` sObjects / fields it references and which other Apex method or LWC binding consumes its result (direction=both)."
3. **[D3 Retrieval]** "Retrieve the full text of the embedded SOQL query in `OrderController.getOrderItems` [verify] — return the complete `SELECT … FROM … WHERE …` block, not just the method signature." (grep-findable: the method name and its `SELECT` keyword appear verbatim in the source `.cls` file; this asks for one named method's query, not a ranked/'largest' one, so plain grep can locate and read it too.)
4. **[D4 Architecture]** "Describe how SOQL is organized in this repo: which directory holds the query-bearing Apex (`force-app/main/default/classes/`), and how query logic is split across controller vs. test classes (`*Test.cls`) — and whether any batch/queueable class (`BatchEBikesUpdater` [verify]) carries SOQL."
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find near-duplicate SOQL queries — i.e. multiple `SELECT` blocks selecting overlapping field sets from the same custom object — and link each embedded query back to the custom-object schema definitions (`*.object-meta.xml` / field metadata) it depends on. This config↔code linkage and similarity clustering is not expressible by plain text search."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*getProducts.*"|".*Product__c.*", label="Method")+search_code(pattern="Product__c"); D2->trace_call_path(qualified_name="ProductController.getProducts", direction="both"); D3->get_code_snippet(qualified_name="OrderController.getOrderItems"); D4->get_architecture(path="force-app/main/default/classes"); D5->search_code(semantic_query="SELECT fields FROM Product__c")/search_graph(semantic_query=...).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/soql-graph.md`, `soql-explorer.md`, `soql-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the soql tier.

---

### 153. sosl — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** trailheadapps/ebikes-lwc (symlink apex) (`/tmp/bench/sosl`)   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** ebikes-lwc is Salesforce's flagship, heavily-starred LWC sample app, indexed via the shared `apex` symlink (it is the Apex/SOQL/SOSL host corpus). **Honest caveat (verified):** every database access in this repo is *SOQL* (`SELECT … FROM …` in `ProductController`, `OrderController`, `ProductRecordInfoController`) — there is **no embedded SOSL** (`[FIND … IN … RETURNING …]`) anywhere in `force-app/main/default/classes`. SOSL is therefore an **absent language** for this repo; the canonical idiomatic-SOSL corpus is the sibling `trailheadapps/apex-recipes` (`SOSLRecipes`), not ebikes-lwc. Because the plan pins the repo/symlink, this chapter is authored as an **absence-aware** probe: the fair question for SOSL here is whether each tool *correctly reports no SOSL* rather than hallucinating fabricated `[FIND … RETURNING …]` statements. Several dimensions are honestly **N/A** below.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List every embedded SOSL search expression (`[FIND … IN … RETURNING …]`) defined in the Apex classes of this repo, returning each enclosing method's qualified name and file." Expected truthful answer: **none** — the repo's search/query methods (e.g. `ProductController.getProducts`, `ProductController.getSimilarProducts`) use SOQL `SELECT`, not SOSL. (Symmetric/grep-checkable: a grader can confirm with `grep -rE '\[\s*FIND' force-app/.../classes` → 0 hits, while `grep -r 'SELECT' ` is non-empty; the tool must not invent SOSL.)
2. **[D2 Relationship] — N/A.** SOSL relationship edges (a `RETURNING <sObject>` clause linking to that sObject's metadata) cannot exist because there are zero SOSL clauses in the repo. *Reason: no SOSL statements to relate.* (The SOQL→sObject relationship is covered by the soql chapter §152, D2; not duplicated here.)
3. **[D3 Retrieval]** "Retrieve the source of the search method that backs the product-search UI — `ProductController.getProducts` [verify] — with exact line span, and state which query language its embedded statement actually uses." Expected truthful answer: the method body contains an embedded **SOQL** `[SELECT … FROM Product__c …]` (via `Database.query`), **not** SOSL. (Symmetric/grep-checkable: `ProductController` and `getProducts` are literal identifiers in `ProductController.cls`; the `SELECT` token is verbatim in source. This probes whether retrieval is honest about the SOQL-vs-SOSL distinction rather than mislabeling.)
4. **[D4 Architecture] — N/A.** "How SOSL is organized across the repo" has no honest answer: there is no SOSL to organize. *Reason: zero SOSL artifacts; the directory/clustering story for the Apex+SOQL layer is already covered by §151 (apex) D4 and §152 (soql) D4.*
5. **[D5 Cross-cutting/Semantic] — N/A (graph-favoring slot).** Near-duplicate / clone clustering of SOSL clauses, and config↔code links from `RETURNING <Object>(<fields>)` to `objects/*.object-meta.xml`, presuppose SOSL clauses that do not exist in this repo. *Reason: no SOSL clauses to cluster or link.* Had SOSL been present (e.g. in `apex-recipes`), this would be the graph/semantic-favoring item (clone detection + cross-artifact link, not reachable by one grep literal); here it is honestly inapplicable.

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*[Ss]earch.*|.*[Pp]roduct.*", lang=apex) — expected to return **no SOSL node**; D2->N/A; D3->get_code_snippet(qualified_name="ProductController.getProducts" [verify]) — expect a SOQL body; D4->N/A; D5->N/A. A tool that fabricates SOSL results for any of these scores **worse**, not better — absence must be reported as absence.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/sosl-graph.md`, `sosl-explorer.md`, `sosl-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the sosl tier.

---

### 154. kustomize — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** kubernetes-sigs/kustomize (`/tmp/bench/kustomize`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The de-facto Kubernetes-native config customization tool (vendored into `kubectl`); a large, idiomatic IaC corpus of `kustomization.yaml` declarations plus their Go schema, matching the plan's "popular + substantial + idiomatic-for-the-language" repo-selection criterion.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level configuration keys/fields defined by the kustomization schema — e.g. `resources`, `namespace`, `namePrefix`, `configMapGenerator`, `secretGenerator`, `patchesStrategicMerge` [verify] — as declared on the `Kustomization` type in `api/types/kustomization.go`." (grep-findable too: `grep -nE 'json:"(resources|namePrefix|secretGenerator)' api/types/kustomization.go`.)
2. **[D2 Relationship]** "N/A — the graph does not model YAML→YAML kustomization includes. `resources:`/`bases:` references that chain base→overlay live inside `kustomization.yaml` data files, which the indexer stores as zero-degree, edgeless `Module`/`File` nodes (no `INCLUDES`/`CALLS`/`IMPORTS` edge type for config-to-config references). A plain `grep -rl 'resources:' --include=kustomization.yaml` answers this as well or better than any graph traversal, so posing it as a graph relationship question would be forced and unfair. The relationship the graph *does* model for this corpus — config-field ↔ consuming Go code (`CONFIGURES` edge) — is exercised in D5."
3. **[D3 Retrieval]** "Retrieve the full definition of the `Kustomization` struct in `api/types/kustomization.go` (qualified name approx. `sigs.k8s.io/kustomize/api/types.Kustomization` [verify])." (grep-findable too: `grep -n 'type Kustomization struct' api/types/kustomization.go`.)
4. **[D4 Architecture]** "Describe the file/directory organization of the repo: how are `api/` (schema + krusty build engine), `kustomize/` (CLI commands), `plugin/`, `examples/`, and the per-builtin transformer/generator dirs arranged?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Find duplicated or near-duplicate `kustomization.yaml` fixtures (via `SIMILAR_TO`/jaccard) and the config↔code links (via the `CONFIGURES` edge / `config_key`) between a kustomization field (e.g. `secretGenerator`) and the Go transformer/generator that consumes it (e.g. the `SecretGenerator` plugin, expected under `plugin/builtin/secretgenerator/` and code-generated into `api/builtins/` [verify])."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Field"/"Struct", name_pattern=".*Kustomization.*"); D2->N/A (no graph edge models YAML→YAML includes; grep is the fair baseline here); D3->get_code_snippet(qualified_name="sigs.k8s.io/kustomize/api/types.Kustomization" [verify]); D4->get_architecture(...); D5->search_graph(relationship="CONFIGURES"/"SIMILAR_TO") + search_graph(semantic_query=["secretGenerator","transformer","duplicate fixture"]) / search_code.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/kustomize-graph.md`, `kustomize-explorer.md`, `kustomize-judged.json`.
**Aggregates into:** D1/D3/D4 cross-group rollups (D2 is N/A for this language and is excluded from the D2 rollup), D5 within Group E only, Group E, the kustomize tier.

---

### 155. k8s — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** kubernetes/examples (symlink yaml) (`/tmp/bench/k8s`)   **Symlink:** yes (k8s tier re-indexes the same kubernetes/examples corpus as the `yaml` tier; the `k8s` working tree is a symlink to it)
**Indexed in:** fast   **Why this repo:** Canonical, high-star Kubernetes example app manifests — idiomatic, substantial YAML/config corpus (Deployments, Services, StatefulSets, PVs/PVCs across dozens of self-contained demos), matching the plan's "popular + idiomatic + substantial" repo-selection criteria for Group E.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the top-level Kubernetes resource definitions whose `kind` is `Service` in the guestbook example (e.g. the `frontend` and `redis-master` Services, historically in `guestbook/frontend-service.yaml` and `guestbook/redis-master-service.yaml` [verify]). These are grep-findable via the literal `kind: Service` and `name:` keys, so a plain text scan can locate them too."
2. **[D2 Relationship]** "N/A — k8s manifests are a config/data corpus: the knowledge graph models them as documents/keys, not as symbols carrying call or reference edges. The relationship a reviewer would actually want (a Service's `spec.selector` linking to a workload's `template.metadata.labels`) is Kubernetes-semantic, not a generic code-graph edge — there is no `CALLS`/`HANDLES`/`IMPLEMENTS`/`IMPORTS` relationship between YAML keys for the graph to traverse. Forcing a `trace_call_path`-style question here would be unnatural and would not exercise a real graph capability."
3. **[D3 Retrieval]** "Retrieve the full largest single resource definition — the Cassandra StatefulSet manifest, historically `cassandra/cassandra-statefulset.yaml` (the `cassandra` StatefulSet [verify]) — returning the complete definition including its `volumeClaimTemplates` and container spec. The file is a stable, grep-findable named path, so retrieval is reproducible without the graph."
4. **[D4 Architecture]** "Describe the directory/file organization of kubernetes/examples: how example apps are grouped into per-app folders (e.g. `guestbook/`, `cassandra/`, `mysql-wordpress-pd/`, `storage/` [verify]), which top-level folders hold the most manifests, and whether any YAML is shared across folders via symlink (discoverable with `find -type l`)."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "Across all example apps, find near-duplicate / naming-pattern manifests — e.g. the recurring `*-service.yaml` + `*-deployment.yaml`/`*-controller.yaml` pairing and the shared `app:`/`tier:` label conventions — and surface config<->config links created by `selector`/`label` matching. Explicitly graph/semantic-favoring: it relies on structural similarity and cross-file pattern clustering that a plain text scan cannot rank."

**Expected graph tools (hint, not a script):** D1->search_graph(label/kind="Service", name_pattern=".*", project="k8s"); D2->N/A (no relationship edges between YAML manifest keys in a generic code graph; see D2 note); D3->get_code_snippet(qualified_name="...cassandra (StatefulSet)"); D4->get_architecture(project="k8s"); D5->search_code / search_graph(semantic_query="duplicated service+deployment selector/label pairing across example apps").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/k8s-graph.md`, `k8s-explorer.md`, `k8s-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups (D2 counted as N/A for k8s), D5 within Group E only, Group E, the k8s tier.

---

### 156. pine — C (Dynamic & Scripting)

**Repo:** pinecoders/pine-utils (`/tmp/bench/pine`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** The de-facto community reference for reusable Pine Script (the dynamic, scripting-style language for TradingView), maintained by the PineCoders group; small but the most idiomatic public corpus of Pine functions, satisfying the plan's "most representative substantial repo per language" criterion for a niche language.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the user-defined Pine functions exposed by this repo and their entry signatures — e.g. the `f_print` debug-label helper and other `f_`-prefixed utility functions [verify] — and confirm `f_print` is reported as a definition (it is a grep-findable `f_print(...) =>` token)."
2. **[D2 Relationship]** "Within a single utility study, what is the call relationship around `f_print` — does any other function or a plot/label block invoke it [verify], and does it call any intrinsic (e.g. `label.new`) in turn? Resolve callers and callees in both directions. NOTE: this corpus is largely single-function-per-snippet with few internal cross-calls, so a shallow or empty caller set is the honest expected answer — credit a correct 'no internal callers' result, do not penalize it."
3. **[D3 Retrieval]** "Retrieve the full source of the single function `f_print` exactly as defined, including its `=>` body and the `label.new(...)` call it wraps — boundaries only, no surrounding file noise. (`f_print` is a plain grep-findable symbol; both grep and graph must be able to locate it.)"
4. **[D4 Architecture]** "Describe the repository's structural organization: the top-level layout (README catalog + per-utility `.pine` files / snippet sections), how utilities are grouped, and where the function definitions physically live relative to the documentation index."
5. **[D5 Cross-cutting/Semantic]** "(GRAPH-FAVORING) Group the repo's utility functions by naming convention and structural cohesion — surface the de-facto clusters of related helpers (e.g. functions that share a `f_`/idiom prefix or repeated debug-label patterns) using the graph's architecture/community view rather than a flat text match. The win condition is producing grouped clusters with a cohesion signal, which a single grep cannot synthesize. Label: graph-favoring (structural clustering, not true semantic dedup — the toolset has no semantic near-duplicate detector)."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern="f_.*", label="Function"); D2->trace_path(function_name="f_print", direction="both"); D3->get_code_snippet(qualified_name=".*f_print"); D4->get_architecture(); D5->get_architecture() (Leiden clusters) + search_code(pattern="f_") for naming-pattern grouping.

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/pine-graph.md`, `pine-explorer.md`, `pine-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group C only, Group C, the pine tier.

---

### 157. qml — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** lirios/lirios (`/tmp/bench/qml`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** Liri OS is a popular, actively-maintained Qt Quick desktop project whose UI is written idiomatically and substantially in QML (declarative object trees, properties, signals, imports) — matching the plan's "popular + idiomatic + substantial in the target language" repo-selection criterion for Group E markup.

> **Indexer note (fairness):** the CBM QML spec parses with the `qmljs` grammar (a TypeScript superset). It extracts **`property` and `signal` members** (`ui_property`/`ui_signal` → field Definition nodes), **`import` statements** (`ui_import`/`import_statement` → IMPORTS edges), explicit **inline `component` declarations** (`ui_inline_component`) and any embedded JS functions/classes. It does **not** create a Definition node for the anonymous QML **root object** (`ui_object_definition` has no name field), so a QML file's root type has **no qualified name** — root-object retrieval falls back to file/line lookup, exactly like CSS selector blocks. Questions are authored honestly around this: D1/D3 are written so the property/signal/import structure the graph genuinely captures is testable, while the root-object parts are flagged as grep/file-retrieval parity rather than graph wins.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "List the declared `property` and `signal` members of the shell UI components — e.g. the `property`/`signal` declarations in `Indicator.qml` and `StatusBar.qml` [verify] — and any explicit `component`-keyword inline component definitions. (Symmetric: these are equally findable by `grep -nE '^\s*(property|signal|component) '`. Note the *anonymous root object type* of each `.qml` file is **not** a graph Definition node, so for the root type itself the graph does no better than grep — only the property/signal members and inline components are graph nodes.)"
2. **[D2 Relationship]** "For the shell component defined in `Indicator.qml` [verify], which modules does it pull in via `import` (e.g. `QtQuick`, `Fluid.Controls`, `Liri.Shell` [verify]), and — following the indexer's IMPORTS edges — which other QML files import the same modules or reference it? Show the import edges in both directions. (Note: cross-*file* instantiation of one .qml type by another via bare element usage is **not** modeled as a graph edge — module-level `import` IS. Answer should distinguish the two.)"
3. **[D3 Retrieval]** "Retrieve the full source of the root object in `Shell.qml` [verify] — its root type body, properties and child elements — verbatim with its boundaries. (Symmetric / parity: the QML root object is anonymous and exposes **no qualified-name snippet target**, so `get_code_snippet` has no symbol to resolve and both engines fall back to file/line retrieval, locatable by `grep -n` for the filename and root element. The graph IS expected to win when retrieving a named `property`/`signal`/inline-`component` member instead of the root object.)"
4. **[D4 Architecture]** "Describe the file/directory organization of the QML UI: how the shell, settings/preferences modules, and reusable controls are grouped into directories, and where `qmldir`/module boundaries fall. [verify]"
5. **[D5 Cross-cutting/Semantic]** "(Graph-favoring) Find near-duplicate QML components and recurring property/anchor declaration patterns across the shell (e.g. repeated `anchors.fill: parent` + `MouseArea` button idioms), and surface config<->code links where a QML element's property is driven by a `Settings`/`.conf` key. Label: openly semantic/similarity-favoring — duplication clustering plus property↔config linkage are not reachable by a single exact-string grep."

**Expected graph tools (hint, not a script):** D1->search_graph(label="Definition", name_pattern="property|signal") + search_code (root object type is not a Definition node — grep parity is the honest baseline there); D2->search_graph(name_pattern="Indicator") then trace_call_path/IMPORTS-edge inspection for module import edges (cross-file element instantiation is not graphed); D3->search_code / file retrieval for the `Shell.qml` root object (no qualified-name target for the root; get_code_snippet only resolves named members); D4->get_architecture(scope="qml UI tree"); D5->search_code(semantic_query="duplicate QML components, anchor/MouseArea idioms, Settings-bound properties").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/qml-graph.md`, `qml-explorer.md`, `qml-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the qml tier.

---

### 158. cfscript — A (Class-based OOP & Contracts)

**Repo:** ortus-solutions/coldbox-platform (`/tmp/bench/cfscript`)   **Symlink:** no
**Indexed in:** fast   **Why this repo:** ColdBox is the dominant, widely-starred CFML/CFScript MVC framework by Ortus Solutions — a large, idiomatic `.cfc` codebase rich in class-based OOP (components, inheritance, base-class contracts, DI), satisfying the plan's "popular + substantial + idiomatic" repo-selection criteria for the cfscript tier.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the definition of the core `Controller` component (`system/web/Controller.cfc`) and list its public accessor methods such as `getInterceptorService()` and `getHandlerService()` — are the component declaration and its method signatures discoverable by name?"
2. **[D2 Relationship]** "Show the call relationships (both inbound and outbound) around `HandlerService.cfc`'s `registerHandlers()` method (`system/web/services/HandlerService.cfc`) — which components invoke it and what does it call in turn (e.g. `getHandlerListing()`)?"
3. **[D3 Retrieval]** "Retrieve the full source of the WireBox `Injector.cfc` component's `getInstance()` method (`system/ioc/Injector.cfc`) as a single targeted snippet."
4. **[D4 Architecture]** "Describe the top-level architecture under `system/` — how are `web/`, `ioc/` (WireBox), `logging/` (LogBox), and `cache/` (CacheBox) organized into component packages, and where does the framework bootstrapper `system/Bootstrap.cfc` sit relative to the user-facing application config DSL (`config/ColdBox.cfc`)?"
5. **[D5 Cross-cutting/Semantic]** "(graph-favoring) Across the framework, find the `*Service.cfc` components that follow the service-object pattern (e.g. `HandlerService`, `InterceptorService`, `ModuleService`, `RoutingService`) and identify the shared base-class contract they extend — note that ColdBox services extend `BaseService.cfc` [verify] and interceptors extend the `coldbox.system.Interceptor` base class (`system/Interceptor.cfc`) — surfacing cross-cutting naming + inheritance conventions a text grep would only partially recover."

**Expected graph tools (hint, not a script):** D1->search_graph(name_pattern=".*Controller.*", label="Class"); D2->trace_call_path(name="registerHandlers", direction="both"); D3->get_code_snippet(qualified_name="...Injector.getInstance"); D4->get_architecture(); D5->search_code/semantic_query([".*Service.cfc","interceptor","base class contract"]).

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/cfscript-graph.md`, `cfscript-explorer.md`, `cfscript-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group A only, Group A, the cfscript tier.

---

### 159. cfml — E (Config/Data/Markup/Schema/Build/Template/HDL/Shader/Docs)

**Repo:** ortus-solutions/coldbox-platform (symlink cfscript) (`/tmp/bench/cfml`) [verify: GitHub org for this repo currently resolves as `ColdBox/coldbox-platform`; `ortus-solutions/coldbox-platform` 404s — Ortus Solutions is the vendor, the org was renamed]   **Symlink:** yes
**Indexed in:** fast   **Why this repo:** ColdBox is the dominant, most-starred CFML/HMVC framework and a large, idiomatic component+template corpus — the canonical real-world choice for exercising the cfml extractor on `.cfc` components and `.cfm` templates, satisfying the plan's "popular, substantial, idiomatic" repo-selection criteria.

**The 5 questions** (bespoke; dimension in brackets):
1. **[D1 Definition/API]** "Locate the top-level component definitions for the core web services in `system/web/services/` — specifically `InterceptorService`, `HandlerService`, `RoutingService`, and `ModuleService` — and confirm each is declared as a `component extends="coldbox.system.web.services.BaseService"`. (These `component`/`extends=` tokens are plain-grep-findable.)"
2. **[D2 Relationship]** "Starting from `Controller.cfc` (`system/web/Controller.cfc`), map the cross-file references where it wires up the service singletons (`InterceptorService`, `HandlerService`, `RoutingService`, `ModuleService`, `SchedulerService`) and the WireBox `Injector` — i.e. which components does the Controller instantiate/`createObject`/inject, and which of those in turn `extends BaseService`?"
3. **[D3 Retrieval]** "Retrieve the full source of the `findRoute()` method on `RoutingService` (`system/web/services/RoutingService.cfc`) — the routine that resolves an incoming request against the registered routes and returns the routed struct. (`findRoute` is a grep-findable function name.)"
4. **[D4 Architecture]** "Describe the directory organization of `system/` — how the framework partitions responsibilities across `web/`, `ioc/`, `cache/`, `async/`, `aop/`, `core/`, `logging/`, `testing/`, and `exceptions/` — and where the base supertypes (`FrameworkSupertype.cfc`, `EventHandler.cfc`, `Interceptor.cfc`, `RestHandler.cfc`) and the `Application.cfm` template sit relative to the service layer."
5. **[D5 Cross-cutting/Semantic — graph-favoring]** "(Graph-favoring) Surface the naming/inheritance pattern across the service layer: which components share the `*Service` suffix and all `extends BaseService`, and link that convention to the config/code seam — e.g. how interceptor/module registration declared in config (`coldbox.cfc`-style settings) maps to the components in `system/web/services/` that consume it. Semantic/structural similarity and the config↔code link are not recoverable by a single grep, so this dimension is openly graph-favoring."

**Expected graph tools (hint, not a script):** D1->search_graph(label="component"/"definition", name_pattern=".*Service$"); D2->trace_call_path(qualified_name="...Controller", direction="both"); D3->get_code_snippet(qualified_name="coldbox.system.web.services.RoutingService.findRoute"); D4->get_architecture(scope="system/"); D5->search_code/semantic_query("*Service extends BaseService + config interceptor/module registration").

**Graph stats (filled at index time — every node label and all 32 edge types on their own row, zeros kept):**

_Index time (clone + cold index): ___ s — **KEY METRIC** (§5)_

_Node-type histogram:_

| Node label | Count |  | Node label | Count |
|---|---|---|---|---|
| Function | _ |  | Type | _ |
| Method | _ |  | Field | _ |
| Class | _ |  | Variable | _ |
| Struct | _ |  | Route | _ |
| Interface | _ |  | Module | _ |
| Trait | _ |  | Section | _ |
| Enum | _ |  | Macro | _ |
| File | _ |  | Folder | _ |
| **Total nodes** | _ |  | | |

_Edge-type histogram (all 32 edge types, zeros included):_

| Edge type | Count |  | Edge type | Count |
|---|---|---|---|---|
| CALLS | _ |  | DEPENDS_ON | _ |
| ASYNC_CALLS | _ |  | USAGE | _ |
| HTTP_CALLS | _ |  | DATA_FLOWS | _ |
| GRPC_CALLS | _ |  | SEMANTICALLY_RELATED | _ |
| GRAPHQL_CALLS | _ |  | SIMILAR_TO | _ |
| TRPC_CALLS | _ |  | TESTS | _ |
| DEFINES | _ |  | TESTS_FILE | _ |
| DEFINES_METHOD | _ |  | INFRA_MAPS | _ |
| IMPLEMENTS | _ |  | FILE_CHANGES_WITH | _ |
| INHERITS | _ |  | CONTAINS_FILE | _ |
| OVERRIDE | _ |  | CONTAINS_FOLDER | _ |
| DECORATES | _ |  | CROSS_HTTP_CALLS *(LSP pass)* | _ |
| IMPORTS | _ |  | CROSS_ASYNC_CALLS *(LSP pass)* | _ |
| HANDLES | _ |  | CROSS_GRPC_CALLS *(LSP pass)* | _ |
| CONFIGURES | _ |  | CROSS_GRAPHQL_CALLS *(LSP pass)* | _ |
| | |  | CROSS_TRPC_CALLS *(LSP pass)* | _ |
| | |  | CROSS_CHANNEL *(LSP pass)* | _ |
| **Total edges** | _ |  | | |

> Every row is reported even at `0` — a zero is critical signal: `CALLS=0` = broken call-extraction (`CALLS_MISSING`), `IMPLEMENTS`/`INHERITS`/`OVERRIDE=0` = no OO-relation edges, `SIMILAR_TO`/`SEMANTICALLY_RELATED=0` on a full-mode LSP language = semantic-index gap. `CROSS_*` rows are `0` for non-LSP languages and carry real counts only for the 9 LSP cross-repo pairs.

**Output of the analysis:** `eval-results/cfml-graph.md`, `cfml-explorer.md`, `cfml-judged.json`.
**Aggregates into:** D1-D4 cross-group rollups, D5 within Group E only, Group E, the cfml tier.
