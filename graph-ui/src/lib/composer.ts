/* The prompt composer: the bridge from what the human learned to what the
 * agent is told. A basket of cited entities becomes a prompt that names
 * exact identifiers and the MCP calls that reproduce the context. */

export type BasketItem =
  | { kind: "symbol"; id: number; name: string; qualified_name?: string; file_path?: string; start_line?: number; end_line?: number }
  | { kind: "region"; id: number; name: string; members: number; why?: string }
  | { kind: "flow"; id: number; label: string; steps: number }
  | { kind: "question"; question: string; why: string };

export function basketKey(item: BasketItem): string {
  switch (item.kind) {
    case "symbol":
      return `symbol:${item.id}`;
    case "region":
      return `region:${item.id}`;
    case "flow":
      return `flow:${item.id}`;
    case "question":
      return `question:${item.question}`;
  }
}

/* Render the basket + the human's goal into a prompt for a coding agent.
 * Structure: goal → exact citations → how to reproduce the context via MCP
 * → honesty notes. Deterministic; no timestamps. */
export function composePrompt(project: string, goal: string, items: BasketItem[]): string {
  const lines: string[] = [];
  lines.push(goal.trim() || "Help me with the code cited below.");
  lines.push("");
  lines.push(`Project (codebase-memory-mcp index): ${project}`);

  const symbols = items.filter((item) => item.kind === "symbol");
  const regions = items.filter((item) => item.kind === "region");
  const flows = items.filter((item) => item.kind === "flow");
  const questions = items.filter((item) => item.kind === "question");

  if (symbols.length > 0) {
    lines.push("");
    lines.push("Symbols involved (exact identifiers):");
    for (const s of symbols) {
      const where =
        s.file_path && s.start_line
          ? ` — ${s.file_path}:${s.start_line}${s.end_line && s.end_line !== s.start_line ? `-${s.end_line}` : ""}`
          : s.file_path
            ? ` — ${s.file_path}`
            : "";
      lines.push(`- ${s.qualified_name ?? s.name}${where}`);
    }
  }
  if (regions.length > 0) {
    lines.push("");
    lines.push("Modules (call-community regions from the graph):");
    for (const r of regions) {
      lines.push(
        `- ${r.name} (${r.members.toLocaleString("en-US")} symbols${r.why ? `; ${r.why}` : ""})`,
      );
    }
  }
  if (flows.length > 0) {
    lines.push("");
    lines.push("Flows (entry → terminal call journeys):");
    for (const f of flows) lines.push(`- ${f.label} (${f.steps} steps)`);
  }
  if (questions.length > 0) {
    lines.push("");
    lines.push("Open questions:");
    for (const q of questions) lines.push(`- ${q.question} (${q.why})`);
  }

  lines.push("");
  lines.push("To reproduce this context with the codebase-memory-mcp tools:");
  for (const s of symbols.slice(0, 5)) {
    if (s.qualified_name) {
      lines.push(
        `- get_code_snippet(qualified_name: "${s.qualified_name}", project: "${project}")`,
      );
      lines.push(
        `- trace_path(qualified_name: "${s.qualified_name}", project: "${project}", direction: "inbound")`,
      );
    }
  }
  lines.push(`- get_architecture(project: "${project}") for the module map`);
  lines.push("");
  lines.push(
    "Note: CALLS edges are resolved calls; USAGE edges mean the graph could not " +
      "prove a single target — verify before relying on them.",
  );
  return lines.join("\n");
}
