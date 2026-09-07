/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { NodeDetailPanel, CONNECTIONS_PAGE_SIZE } from "./NodeDetailPanel";
import type { GraphEdge, GraphNode } from "../lib/types";

/* #1198: a node with more connections than one page must offer them all —
 * paged behind "Show more", never silently cut at 25. */
describe("NodeDetailPanel connections paging", () => {
  it("pages beyond the first slice instead of capping at 25", () => {
    const total = CONNECTIONS_PAGE_SIZE + 5;
    const center: GraphNode = {
      id: 0, x: 0, y: 0, z: 0, label: "Function", name: "hub", size: 1, color: "#fff",
    };
    const nodes: GraphNode[] = [center];
    const edges: GraphEdge[] = [];
    for (let i = 1; i <= total; i++) {
      nodes.push({
        id: i, x: 0, y: 0, z: 0, label: "Function", name: `callee_${i}`, size: 1, color: "#fff",
      });
      edges.push({ source: 0, target: i, type: "CALLS" });
    }

    render(
      <NodeDetailPanel
        node={center}
        allNodes={nodes}
        allEdges={edges}
        project="demo"
        repoInfo={null}
        onClose={vi.fn()}
        onNavigate={vi.fn()}
      />,
    );

    /* First page rendered, the rest offered. */
    expect(screen.getByText(`callee_${CONNECTIONS_PAGE_SIZE}`)).toBeInTheDocument();
    expect(screen.queryByText(`callee_${total}`)).not.toBeInTheDocument();
    const more = screen.getByRole("button", { name: /Show 5 more/ });
    fireEvent.click(more);
    expect(screen.getByText(`callee_${total}`)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Show .* more/ })).not.toBeInTheDocument();
  });
});
