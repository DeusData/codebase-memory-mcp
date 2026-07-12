# YAML configuration path search

Nested YAML keys are indexed as dotted paths without storing their values. This makes repeated leaf names distinct while keeping secrets and other scalar values out of the graph.

```bash
codebase-memory-mcp cli search_graph '{"project":"my-project","config_path":"^services\\.api\\.timeout$","fields":["config_path"]}'
```

The same filter is available to MCP clients as `search_graph.config_path`. It is a case-insensitive regular expression and composes with `label`, `file_pattern`, degree filters, and pagination. Each matching `Variable` keeps its leaf `name` for ConfigLinker compatibility, while its qualified name and `config_path` property contain the full dotted path.

## Safety and size contract

- Scalar values are not copied into node properties or search indexes.
- Nested YAML definitions are capped at 512 per file, including multi-document YAML. A capped file
  is reported by `index_status` with coverage kind `config_path_truncated` rather than being silently
  treated as complete.
- Helm `values.yaml` and `values.yml` retain their existing top-level-only behavior to avoid chart value leaf floods.
- The current implementation covers YAML mappings that are not nested through a sequence. Sequence
  item mappings remain intentionally excluded until a stable, non-colliding path identity is
  defined. Other config languages continue to use their existing variable extraction.
- Incremental indexing removes path nodes with their source file and recreates the current set, so deleted keys do not remain as orphans.

The graph UI shows `config_path` in the selected node's properties; no separate UI-side parser or source-file read is required.
