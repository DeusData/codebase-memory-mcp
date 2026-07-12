# Design Context and Token Guide

`codebase-memory-mcp` treats design as repository knowledge, not just as CSS syntax. During normal
indexing it builds a read-only Design Context graph from human guidance, portable design tokens,
theme metadata, generated styles, and implementation usages.

## Responsibility boundary

Keep authoring and generation in dedicated design tooling. Figma, Tokens Studio, Style Dictionary,
or another pipeline may own review, transformation, and publishing. This project does not edit
`DESIGN.md`, resolve a complete token build, call those tools, or generate CSS.

`codebase-memory-mcp` owns the repository-side analysis layer:

- discover design sources already committed to the repository;
- normalize systems, tokens, components, modes, aliases, guidance, and usages into the graph;
- preserve source path and provenance;
- expose that context to agents through `get_design_context`, graph queries, and the UI.

This split keeps the index deterministic, local, and usable as a single binary while allowing teams
to choose their design authoring stack independently.

## Recommended repository layout

The defaults work without configuration:

```text
repo/
├── DESIGN.md
├── design/
│   ├── tokens/
│   │   ├── foundation.tokens.json
│   │   ├── semantic.tokens.json
│   │   └── components.tokens.json
│   └── themes.resolver.json
├── src/
│   ├── components/
│   └── styles/
│       └── tokens.css
└── .codebase-memory.json
```

Use these roles consistently:

| Artifact | Put it here | What it should contain |
|---|---|---|
| `DESIGN.md` | Repository root, or a monorepo package root | Design intent, principles, accessibility rules, component semantics, and optional portable frontmatter. |
| `*.tokens.json` | `design/tokens/` | Authoritative DTCG tokens: primitive/foundation values, semantic aliases, then component tokens. |
| `*.resolver.json` | `design/` | DTCG 2025.10 sets, modifiers, contexts, defaults, and resolution order for modes such as light/dark. |
| Generated CSS/SCSS | `src/styles/` or the existing generated-assets directory | Build output such as custom properties. Mark it as generated in config. |
| Component code | The normal application source tree | Consumption through `var(--token-name)`; the index records file-to-token usage. |

In a monorepo, add a nested `DESIGN.md` only when a package has genuinely different design intent.
The nearest ancestor document defines the package design scope. Directory nesting alone does not
silently redefine tokens; aliases and resolver contexts should express overrides explicitly.

## Supported inputs

### DTCG token files

Standard JSON files ending in `.tokens.json` are structurally indexed using the Design Tokens
Community Group 2025.10 format. The built-in reader recognizes group-level `$type`, explicit
`$root` token paths, `$value`, `$description`, token `$ref`, group `$extends`, and `{token.path}`
aliases. `$ref` and `$extends` are preserved as source metadata; the index does not evaluate JSON
Pointers, deep-merge groups, or produce a resolved token set.

```json
{
  "color": {
    "$type": "color",
    "blue": { "$value": "#155eef" },
    "action": {
      "$value": "{color.blue}",
      "$description": "Primary interactive action"
    }
  }
}
```

Files must be standard JSON. JSONC comments are not accepted by the built-in parser.

### DTCG resolver metadata

Files ending in `.resolver.json` with `version: "2025.10"` produce `DesignMode` nodes for modifier
contexts and `OVERRIDES` edges to already indexed local token sources. The index records resolver
metadata; it is not a conforming token-resolution/build engine.

Each `OVERRIDES` edge carries the source-specific value, token path, source path, source order, and
modifier order. A light and dark source may therefore define the same canonical token path without
losing either value. `resolutionOrder` is retained as ordering metadata, not executed as a build.

Only repository-local references are followed for graph linking. Absolute paths, parent traversal,
URLs, and other URI schemes are never fetched. Same-document references remain visible in resolver
metadata but are not expanded into generated token output.

### `DESIGN.md`

Markdown is the durable semantic layer for information that a token file cannot express well:
product principles, hierarchy, accessibility constraints, interaction intent, and component usage.
Markdown headings are linked to the design system as guidance.

An optional Google `design.md`-style YAML frontmatter block can define portable colors, typography,
spacing, rounded values, and components:

```markdown
---
version: alpha
name: Aurora
colors:
  primary: "#155eef"
spacing:
  control: "8px"
components:
  button-primary:
    backgroundColor: "{colors.primary}"
---

# Aurora design

Primary actions must remain distinguishable without relying on color alone.
```

Keep frontmatter intentionally simple. Rich narrative belongs in Markdown; machine-transformable
values belong in DTCG token files.

Typography entries are indexed as composite tokens. For example, the fields below become one
`typography.h1` token rather than separate `typography.h1.fontFamily` and
`typography.h1.fontSize` tokens:

```yaml
typography:
  h1:
    fontFamily: Inter
    fontSize: 32px
    fontWeight: 700
```

### CSS and SCSS

CSS/SCSS custom-property definitions are indexed as observed tokens when no authoritative token with
the same normalized path exists. `var(--name)` references produce `USES_TOKEN` edges from the source
file. CSS parsing is intentionally lightweight and read-only; it does not replace a browser parser or
a token compiler.

## Project configuration

Defaults discover `DESIGN.md`, `*.tokens.json`, `*.resolver.json`, and CSS/SCSS. Override discovery or
provenance in the repository-root `.codebase-memory.json`:

```json
{
  "design": {
    "documents": ["DESIGN.md", "packages/**/DESIGN.md"],
    "token_sources": ["design/**/*.tokens.json"],
    "resolvers": ["design/**/*.resolver.json"],
    "authoritative": ["DESIGN.md", "design/**/*.json"],
    "generated": ["src/styles/generated/**", "dist/**/*.css"]
  }
}
```

Patterns are repository-relative and support `*`, `**`, and `?`. Each design input is capped at 8 MiB
and normal repository ignore rules still apply. Treat `authoritative` and `generated` as provenance,
not as build instructions: the index never writes either category.

Recommended source-of-truth policy:

1. DTCG token JSON is authoritative for transformable values.
2. `DESIGN.md` is authoritative for human and agent intent.
3. Resolver JSON is authoritative for modes and composition metadata.
4. CSS generated from tokens is marked `generated` and never overwrites an authoritative token.
5. Handwritten CSS is `observed` unless explicitly classified.

## Querying design context

After `index_repository`, use the curated tool first:

```text
get_design_context(project="my-project")
get_design_context(project="my-project", scope="packages.app", token="color.action")
get_design_context(project="my-project", component="button")
get_design_context(project="my-project", limit=200, offset=200, relation_offset=800)
```

`scope` is an exact match, so `packages.app` does not include `packages.application`. Results are
ordered by qualified name and return `filtered_total`, `has_more_by_type`, and
`has_more_relations`; advance `offset` and `relation_offset` until both node and relation pages are
complete. The Design tab performs this paging automatically.

The tool returns project-local systems, tokens, components, modes, and their core relations. For
broader exploration, use graph tools:

```text
search_graph(project="my-project", label="DesignToken", name_pattern=".*action.*")
query_graph(project="my-project", query="MATCH (f:File)-[:USES_TOKEN]->(t:DesignToken) RETURN f.file_path, t.name LIMIT 50")
```

The UI-enabled binary adds a **Design** tab with scope navigation, token filtering, provenance,
values, components, modes, and connected usages/aliases.

### Graph model

| Nodes | Meaning |
|---|---|
| `DesignSystem` | A root or nested `DESIGN.md` scope. |
| `DesignToken` | A normalized token with source, type, value, and provenance. |
| `DesignComponent` | A component declared by portable design frontmatter. |
| `DesignMode` | A DTCG resolver modifier context such as `theme: dark`. |

Core relationships are `PROVIDES`, `ALIASES_TO`, `OVERRIDES`, `USES_TOKEN`, `DOCUMENTED_BY`,
`GUIDED_BY`, and `GENERATED_AS`.

## Global Memory is opt-in

Design Context belongs to the disposable project graph. Nothing is copied to Global Memory
automatically. If a design decision is durable across repositories, explicitly propose a scoped
Global Memory page/claim and attach repository qualified names as CodeRefs. Review and commit it with
the normal `memory_propose` → `memory_commit` workflow. Repository-only details should remain local.

## Adoption checklist

1. Choose and document the external authoring/generation owner.
2. Add a root `DESIGN.md` with principles, semantics, and accessibility constraints.
3. Export authoritative values as standard DTCG `*.tokens.json`.
4. Add `.resolver.json` only when modes or conditional contexts are needed.
5. Keep generated CSS in a predictable path and classify it as `generated`.
6. Add nested `DESIGN.md` files only for meaningful monorepo boundaries.
7. Index the repository and inspect the Design tab or `get_design_context` output.
8. Fix missing aliases/usages at the source; do not hand-edit the graph.

## References

- [Atlassian: portable design context in practice](https://www.atlassian.com/blog/ai-at-work/atlassians-design-md-is-here-what-we-learned-testing-portable-design-context-in-practice)
- [Atlassian `DESIGN.md`](https://atlassian.design/DESIGN.md)
- [Google Labs `design.md` specification](https://github.com/google-labs-code/design.md/blob/main/docs/spec.md)
- [DTCG 2025.10 format](https://www.designtokens.org/tr/2025.10/format/)
- [DTCG 2025.10 resolver](https://www.designtokens.org/tr/2025.10/resolver/)
