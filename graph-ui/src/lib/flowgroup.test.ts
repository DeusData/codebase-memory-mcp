import { describe, expect, it } from "vitest";
import { groupFlowsByEntry } from "./flowgroup";
import type { FlowSummary } from "./atlas";

function flow(id: number, entry: string, steps: number): FlowSummary {
  return {
    id,
    label: `${entry} → t${id}`,
    entry: { id: id * 10, name: entry },
    terminal: { id: id * 10 + 1, name: `t${id}` },
    steps,
    sink_terminated: true,
    cross_region: false,
  } as FlowSummary;
}

describe("groupFlowsByEntry", () => {
  it("merges flows sharing an entry, richest first, order preserved", () => {
    const groups = groupFlowsByEntry([
      flow(1, "main", 5),
      flow(2, "serve", 9),
      flow(3, "main", 12),
    ]);
    expect(groups.map((g) => g.entryName)).toEqual(["main", "serve"]);
    expect(groups[0].flows.map((f) => f.id)).toEqual([3, 1]);
    expect(groups[1].flows).toHaveLength(1);
  });
  it("handles empty input", () => {
    expect(groupFlowsByEntry([])).toEqual([]);
  });
});
