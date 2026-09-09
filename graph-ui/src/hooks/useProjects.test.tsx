/* @vitest-environment jsdom */
import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { useProjects } from "./useProjects";

const callToolMock = vi.fn();
vi.mock("../api/rpc", () => ({
  callTool: (...args: unknown[]) => callToolMock(...args),
}));

describe("useProjects machine-readable pagination", () => {
  beforeEach(() => callToolMock.mockReset());

  it("requests JSON and merges every project and schema page", async () => {
    callToolMock.mockImplementation(async (name: string, args: Record<string, unknown>) => {
      if (name === "list_projects") {
        if (args.offset === 0) {
          return {
            projects: [{ name: "alpha", root_path: "/alpha", indexed_at: "now" }],
            has_more: true,
            next_offset: 1,
          };
        }
        return {
          projects: [{ name: "beta", root_path: "/beta", indexed_at: "now" }],
          has_more: false,
        };
      }
      if (name === "get_graph_schema" && args.project === "alpha") {
        if (args.offset === 0) {
          return {
            node_labels: [{ label: "Function", count: 3 }],
            edge_types: [],
            total_nodes: 3,
            total_edges: 2,
            has_more: true,
            next_offset: 1,
          };
        }
        return {
          node_labels: [],
          edge_types: [{ type: "CALLS", count: 2 }],
          total_nodes: 3,
          total_edges: 2,
          has_more: false,
        };
      }
      return {
        node_labels: [{ label: "Class", count: 1 }],
        edge_types: [],
        total_nodes: 1,
        total_edges: 0,
        has_more: false,
      };
    });

    const { result } = renderHook(() => useProjects());
    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(result.current.projects).toHaveLength(2);
    expect(result.current.projects[0].schema?.node_labels).toEqual([
      { label: "Function", count: 3 },
    ]);
    expect(result.current.projects[0].schema?.edge_types).toEqual([
      { type: "CALLS", count: 2 },
    ]);
    expect(callToolMock).toHaveBeenCalledWith("list_projects", {
      format: "json",
      detail: "stats",
      limit: 500,
      offset: 0,
    });
    expect(callToolMock).toHaveBeenCalledWith("list_projects", {
      format: "json",
      detail: "stats",
      limit: 500,
      offset: 1,
    });
    expect(callToolMock).toHaveBeenCalledWith("get_graph_schema", {
      project: "alpha",
      format: "json",
      limit: 500,
      offset: 1,
    });
  });
});
