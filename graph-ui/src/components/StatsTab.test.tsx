/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StatsTab } from "./StatsTab";

function mockProjectsFetch(extra?: (url: string, init?: RequestInit) => Response | undefined) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const overridden = extra?.(url, init);
    if (overridden) return overridden;
    if (url === "/rpc") {
      return new Response(JSON.stringify({
        result: { content: [{ text: JSON.stringify({ projects: [] }) }] },
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
    if (url.startsWith("/api/ui-config")) {
      return new Response(JSON.stringify({ lang: "en" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    if (url.startsWith("/api/browse")) {
      return new Response(JSON.stringify({
        path: "/home/dev",
        parent: "/home",
        dirs: ["alpha", "beta"],
        roots: ["/", "D:/"],
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
    if (url === "/api/index") {
      return new Response(JSON.stringify({ status: "indexing", slot: 0 }), {
        status: 202,
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("{}", { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("StatsTab index modal", () => {
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("submits a custom path and project name", async () => {
    let submitted: unknown = null;
    mockProjectsFetch((url, init) => {
      if (url === "/api/index") {
        submitted = JSON.parse(String(init?.body));
        return new Response(JSON.stringify({ status: "indexing", slot: 0 }), {
          status: 202,
          headers: { "Content-Type": "application/json" },
        });
      }
      return undefined;
    });

    render(<StatsTab onSelectProject={() => {}} />);
    fireEvent.click(await screen.findByRole("button", { name: "Index your first repository" }));

    fireEvent.change(await screen.findByLabelText("Repository path"), {
      target: { value: "D:\\work\\信租风控通后端" },
    });
    fireEvent.change(screen.getByLabelText("Project name"), {
      target: { value: "信租风控通后端" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Index This Folder" }));

    await waitFor(() => {
      expect(submitted).toEqual({
        root_path: "D:\\work\\信租风控通后端",
        project_name: "信租风控通后端",
      });
    });
  });

  it("filters picker rows and exposes quick row indexing", async () => {
    mockProjectsFetch();

    render(<StatsTab onSelectProject={() => {}} />);
    fireEvent.click(await screen.findByRole("button", { name: "Index your first repository" }));

    fireEvent.change(await screen.findByPlaceholderText("Filter folders"), {
      target: { value: "bet" },
    });

    expect(screen.queryByText("alpha")).not.toBeInTheDocument();
    expect(screen.getByText("beta")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Index beta" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Browse D:/" })).toBeInTheDocument();
  });

  it("navigates Windows breadcrumb segments to real drive paths", async () => {
    const fetchMock = mockProjectsFetch((url) => {
      if (url.startsWith("/api/browse")) {
        return new Response(JSON.stringify({
          path: "C:/Users/rap",
          parent: "C:/Users",
          dirs: ["Documents", "Downloads"],
          roots: ["C:/", "D:/"],
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      }
      return undefined;
    });

    render(<StatsTab onSelectProject={() => {}} />);
    fireEvent.click(await screen.findByRole("button", { name: "Index your first repository" }));

    /* No bogus unified "/" root crumb on a Windows drive path. */
    await screen.findByRole("button", { name: "C:" });
    expect(screen.queryByRole("button", { name: "/" })).not.toBeInTheDocument();

    /* Clicking the drive crumb browses to "C:/", not "/C:". */
    fireEvent.click(screen.getByRole("button", { name: "C:" }));
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith("/api/browse?path=C%3A%2F");
    });

    /* Clicking a nested crumb browses to "C:/Users", not "/C:/Users". */
    fireEvent.click(screen.getByRole("button", { name: "Users" }));
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith("/api/browse?path=C%3A%2FUsers");
    });
  });
});
