/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useProjects } from "./useProjects";

const { callToolMock } = vi.hoisted(() => ({ callToolMock: vi.fn() }));

vi.mock("../api/rpc", () => ({ callTool: callToolMock }));

function ProjectsHarness() {
  const { projects } = useProjects();
  return <output data-testid="project-count">{projects.length}</output>;
}

describe("useProjects", () => {
  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it("loads every list_projects page", async () => {
    const firstPage = Array.from({ length: 50 }, (_, index) => ({
      name: `project-${index + 1}`,
      root_path: `/repo/project-${index + 1}`,
      indexed_at: "2026-01-01T00:00:00Z",
    }));
    const finalProject = {
      name: "project-51",
      root_path: "/repo/project-51",
      indexed_at: "2026-01-01T00:00:00Z",
    };

    callToolMock.mockImplementation(
      async (name: string, args: Record<string, unknown> = {}) => {
        if (name === "list_projects") {
          if (args.offset === 50) {
            return {
              projects: [finalProject],
              total: 51,
              offset: 50,
              limit: 50,
              returned: 1,
              has_more: false,
            };
          }
          return {
            projects: firstPage,
            total: 51,
            offset: 0,
            limit: 50,
            returned: 50,
            has_more: true,
          };
        }
        return {
          node_labels: [],
          edge_types: [],
          total_nodes: 0,
          total_edges: 0,
        };
      },
    );

    render(<ProjectsHarness />);

    await waitFor(() => {
      expect(screen.getByTestId("project-count")).toHaveTextContent("51");
    });
    expect(callToolMock).toHaveBeenCalledWith("list_projects", {
      offset: 50,
      limit: 50,
    });
  });

  it("renders projects before bounded schema hydration completes", async () => {
    const projects = Array.from({ length: 10 }, (_, index) => ({
      name: `project-${index + 1}`,
      root_path: `/repo/project-${index + 1}`,
      indexed_at: "2026-01-01T00:00:00Z",
    }));

    callToolMock.mockImplementation(async (name: string) => {
      if (name === "list_projects") {
        return {
          projects,
          total: projects.length,
          offset: 0,
          limit: 50,
          returned: projects.length,
          has_more: false,
        };
      }
      return new Promise(() => {});
    });

    render(<ProjectsHarness />);

    await waitFor(() => {
      expect(screen.getByTestId("project-count")).toHaveTextContent("10");
    });
    const schemaCalls = callToolMock.mock.calls.filter(([name]) => name === "get_graph_schema");
    expect(schemaCalls).toHaveLength(4);
  });
});
