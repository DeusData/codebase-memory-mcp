<!-- codebase-memory-mcp:start -->
# Codebase Knowledge Graph

This repository uses codebase-memory-mcp for code discovery.

1. Start broad tasks with `get_context` and a realistic token budget.
2. Prefer `search_graph` for symbols and files.
3. Use `trace_path` before changing shared code.
4. Use `get_code_snippet` for focused source reads.
5. Use `get_architecture` for broader analysis; `query_graph` requires the
   explicitly enabled advanced toolset.
6. Fall back to `rg` for text, configuration and non-code files.

The graph is a local index. It does not call an LLM or launch agents.
<!-- codebase-memory-mcp:end -->

<!-- superpowers:start -->
# Development Methodology (Superpowers)

This project follows the [Superpowers](https://github.com/obra/superpowers) workflow.
The plugin is installed globally (`superpowers@claude-plugins-official`); its skills
load on demand — invoke them at the stages below instead of improvising.

## Workflow

1. **Brainstorm before code** — for any non-trivial feature, use the `brainstorming`
   skill to refine the design through questions before writing anything. Get design
   approval first.
2. **Isolate work** — use `using-git-worktrees` for feature work so `main` stays clean.
3. **Plan in small steps** — use `writing-plans` to break work into 2–5 minute tasks
   with exact file paths and verification criteria, then `executing-plans`.
4. **Test-driven development** — write the failing test first (RED), make it pass
   (GREEN), then refactor. Never write implementation code without a failing test.
   For this repo: `go test ./...` for Go code, plus the RPC/e2e suites in `tests/`.
5. **Debug systematically** — on any bug, use `systematic-debugging`: reproduce,
   form a hypothesis, verify root cause with evidence. No shotgun fixes.
6. **Verify before claiming done** — use `verification-before-completion`: run the
   actual build/tests and observe real behavior. Evidence, not assumptions.
7. **Code review** — use `requesting-code-review` against the plan before merging;
   use `finishing-a-development-branch` for merge/PR and cleanup.

## Principles

- **TDD always** — tests first, no exceptions for "simple" changes.
- **YAGNI** — build only what the current task needs.
- **DRY** — extract duplication once it appears a third time, not before.
- **Simplicity over cleverness** — reduce complexity at every step.
- **Evidence-based completion** — a task is done when verification passes, not when
  the code "looks right".
<!-- superpowers:end -->
