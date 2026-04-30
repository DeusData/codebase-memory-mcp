---
phase: 260429-sti-check-our-current-code-against-the-remot
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - .planning/quick/260429-sti-check-our-current-code-against-the-remot/260429-sti-SUMMARY.md
autonomous: true
requirements:
  - QUICK-GIT-01
must_haves:
  truths:
    - "Current branch, upstream branch, and origin default branch are identified before comparison."
    - "Working tree, staged changes, and untracked files are inspected without modifying source code."
    - "Local HEAD and working tree differences are compared against GitHub origin."
    - "A concise findings summary states whether local code is ahead, behind, diverged, dirty, or matches origin."
  artifacts:
    - path: ".planning/quick/260429-sti-check-our-current-code-against-the-remot/260429-sti-SUMMARY.md"
      provides: "Read-only Git comparison findings"
  key_links:
    - from: "git fetch origin --prune"
      to: "origin refs"
      via: "updates remote-tracking refs before comparison"
    - from: "git status and git diff commands"
      to: "260429-sti-SUMMARY.md"
      via: "executor summarizes observed local/remote differences"
---

<objective>
Check the current repository state against GitHub `origin` without changing source code.

Purpose: Give the user a concise, trustworthy answer about how local code compares to the remote origin, including committed divergence and uncommitted work.
Output: `.planning/quick/260429-sti-check-our-current-code-against-the-remot/260429-sti-SUMMARY.md` containing findings and exact commands run.
</objective>

<execution_context>
@./.opencode/get-shit-done/workflows/execute-plan.md
@./.opencode/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@AGENTS.md
@codemap.md

Constraints for this quick plan:
- Read-only comparison only: do not push, reset, checkout, rebase, merge, amend, commit, or modify source code.
- `git fetch origin --prune` is allowed so remote-tracking refs reflect GitHub origin before comparison.
- The only planned file output is the findings summary under `.planning/quick/...`.
</context>

<tasks>

<task type="auto">
  <name>task 1: refresh origin and identify comparison refs</name>
  <files>.planning/quick/260429-sti-check-our-current-code-against-the-remot/260429-sti-SUMMARY.md</files>
  <action>Run `git fetch origin --prune`, then identify the current branch (`git branch --show-current`), current HEAD SHA (`git rev-parse HEAD`), upstream branch if configured (`git rev-parse --abbrev-ref --symbolic-full-name @{u}` with graceful handling if missing), and origin default branch (`git remote show origin` or equivalent read-only command). Do not checkout or change branches. Record these refs in the summary.</action>
  <verify>
    <automated>git remote get-url origin && git branch --show-current && git rev-parse HEAD</automated>
  </verify>
  <done>Summary identifies origin URL, current branch, HEAD SHA, upstream if present, and origin default branch.</done>
</task>

<task type="auto">
  <name>task 2: inspect local worktree and committed divergence</name>
  <files>.planning/quick/260429-sti-check-our-current-code-against-the-remot/260429-sti-SUMMARY.md</files>
  <action>Inspect local state using read-only git commands: `git status --short --branch`, `git diff --stat`, `git diff --cached --stat`, and `git ls-files --others --exclude-standard`. Compare committed local HEAD against the selected remote ref, preferring the configured upstream and falling back to `origin/<current-branch>` or origin default branch when needed. Use read-only commands such as `git rev-list --left-right --count LOCAL...REMOTE`, `git log --oneline --decorate --left-right LOCAL...REMOTE`, and `git diff --stat REMOTE...HEAD`. Do not run commands that alter the index, working tree, branch, or remote.</action>
  <verify>
    <automated>git status --short --branch && git diff --stat && git diff --cached --stat</automated>
  </verify>
  <done>Summary reports staged, unstaged, and untracked local work plus ahead/behind/diverged/matching status versus the chosen origin ref.</done>
</task>

<task type="auto">
  <name>task 3: write concise findings and safe next-step options</name>
  <files>.planning/quick/260429-sti-check-our-current-code-against-the-remot/260429-sti-SUMMARY.md</files>
  <action>Create the summary with sections: `## Verdict`, `## Refs Compared`, `## Local Working Tree`, `## Local vs Origin`, `## Commands Run`, and `## Safe Next Steps`. The verdict must be one sentence: e.g. "Local HEAD matches origin/main and the working tree is clean" or "Local has uncommitted changes and is 2 commits ahead of origin/feature". Safe next steps may suggest review/commit/pull/push choices, but must not execute them.</action>
  <verify>
    <automated>test -f .planning/quick/260429-sti-check-our-current-code-against-the-remot/260429-sti-SUMMARY.md</automated>
  </verify>
  <done>Summary is concise, includes exact refs/commands, clearly states remote comparison result, and contains no source-code changes.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| local repo → GitHub origin | `git fetch origin --prune` reads remote state and updates remote-tracking refs in `.git` only. |
| Git output → summary | Command output is interpreted into a human-readable findings file. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-260429-sti-01 | Tampering | working tree/index | mitigate | Use only read-only git commands; explicitly prohibit push, reset, checkout, merge, rebase, amend, commit, and source edits. |
| T-260429-sti-02 | Repudiation | findings summary | mitigate | Include exact commands run and refs compared so findings are reproducible. |
| T-260429-sti-03 | Information Disclosure | summary content | accept | Summary remains local under `.planning/quick`; do not include secrets or dump full diffs unless needed for file names/stats. |
</threat_model>

<verification>
Run `git status --short --branch` after the summary is written and confirm no source-code files were changed by this plan. The summary file is the only planned artifact.
</verification>

<success_criteria>
- Origin has been fetched before comparison.
- Current branch/upstream/default remote branch are identified.
- Working tree, staged, unstaged, and untracked changes are reported.
- Local HEAD is compared against the appropriate origin ref with ahead/behind/divergence status.
- No push, reset, checkout, amend, merge, rebase, commit, or source-code modification occurs.
</success_criteria>

<output>
After completion, create `.planning/quick/260429-sti-check-our-current-code-against-the-remot/260429-sti-SUMMARY.md`.
</output>
