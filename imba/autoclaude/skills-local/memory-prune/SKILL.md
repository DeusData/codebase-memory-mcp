---
name: memory-prune
description: Review the project's semantic memory for prune candidates — expired entries (expires_at in the past), duplicates, and contradictions — then delete the ones the user confirms. Use when asked to clean up memory, prune memory, review memories, or when memory has grown noisy.
---

# memory-prune

Implements the review/prune cycle of the autoclaude memory-discipline layer.
Memory should get **smaller and cleaner** over time, not just bigger.

## Steps

1. Find the project's memory dir. It is the `memory/` next to the current
   session transcript, i.e. `~/.claude/projects/<project-slug>/memory/`. If you
   know the slug from context use it; otherwise ask the user for the path.
2. Run the bundled scanner to list candidates (it never deletes):
   ```bash
   python3 "$HOME/.claude/skills/memory-prune/scan.py" <MEMORY_DIR>
   ```
3. For each candidate, decide:
   - **Expired** (`expires_at` < today): propose deletion unless still true —
     if still true, update `expires_at` instead.
   - **Duplicates** (same description): merge into the best single file, delete
     the rest, and fix any `[[links]]` that pointed at the removed names.
   - **Contradictions**: surface both to the user; keep the correct one.
4. Show the user the proposed deletions/merges as a short list and get
   confirmation before touching files (deletion is hard to reverse).
5. After pruning, update `MEMORY.md` so its one-line pointers match the files
   that remain.

Never delete without confirmation. Never prune episodic logs
(`memory/episodes/*.jsonl`) — those are raw history.
