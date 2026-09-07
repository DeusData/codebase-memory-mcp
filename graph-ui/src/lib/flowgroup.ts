import type { FlowSummary } from "./atlas";

/* Merge flows that share an entry point: the list shows one row per entry,
 * richest flow first, with the rest behind a x-N expander. */
export interface FlowEntryGroup {
  entryName: string;
  flows: FlowSummary[];
}

export function groupFlowsByEntry(flows: FlowSummary[]): FlowEntryGroup[] {
  const order: string[] = [];
  const byEntry = new Map<string, FlowSummary[]>();
  for (const flow of flows) {
    const key = flow.entry.name;
    const bucket = byEntry.get(key);
    if (bucket) {
      bucket.push(flow);
    } else {
      byEntry.set(key, [flow]);
      order.push(key);
    }
  }
  return order.map((entryName) => {
    const group = byEntry.get(entryName)!;
    return {
      entryName,
      flows: [...group].sort((a, b) => b.steps - a.steps),
    };
  });
}
