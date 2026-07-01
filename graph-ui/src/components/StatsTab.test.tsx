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

  it("submits a custom path", async () => {
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
    fireEvent.click(screen.getByRole("button", { name: "Index This Folder" }));

    await waitFor(() => {
      expect(submitted).toEqual({ root_path: "D:\\work\\信租风控通后端" });
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

  it("refreshes the folder list when a drive is typed into the path field", async () => {
    mockProjectsFetch((url) => {
      if (url.startsWith("/api/browse")) {
        const m = /[?&]path=([^&]*)/.exec(url);
        const path = m ? decodeURIComponent(m[1]) : "C:/Users/rap";
        const onD = path.replace(/\\/g, "/").toUpperCase().startsWith("D:");
        return new Response(JSON.stringify({
          path,
          parent: "C:/",
          dirs: onD ? ["projects", "games"] : ["Documents", "Downloads"],
          roots: ["C:/", "D:/"],
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      }
      return undefined;
    });

    render(<StatsTab onSelectProject={() => {}} />);
    fireEvent.click(await screen.findByRole("button", { name: "Index your first repository" }));

    /* Initial C: listing is shown. */
    expect(await screen.findByText("Documents")).toBeInTheDocument();

    /* Typing a different drive refreshes the listing to that drive (debounced). */
    fireEvent.change(await screen.findByLabelText("Repository path"), {
      target: { value: "D:/" },
    });

    expect(await screen.findByText("projects")).toBeInTheDocument();
    expect(screen.queryByText("Documents")).not.toBeInTheDocument();
  });

  it("replaces the meaningless '/' root with the drive on Windows", async () => {
    const fetchMock = mockProjectsFetch((url) => {
      if (url.startsWith("/api/browse")) {
        const m = /[?&]path=([^&]*)/.exec(url);
        const path = m ? decodeURIComponent(m[1]) : "C:/Users/rap";
        return new Response(JSON.stringify({
          path,
          parent: "C:/",
          dirs: ["Documents"],
          roots: ["/"], // older backend: no drive enumeration
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      }
      return undefined;
    });

    render(<StatsTab onSelectProject={() => {}} />);
    fireEvent.click(await screen.findByRole("button", { name: "Index your first repository" }));

    /* The bogus "/" quick-jump is gone; the current drive root is offered. */
    expect(await screen.findByRole("button", { name: "Browse C:/" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Browse /" })).not.toBeInTheDocument();

    /* Clicking it browses to the drive root, not "/". */
    fireEvent.click(screen.getByRole("button", { name: "Browse C:/" }));
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith("/api/browse?path=C%3A%2F");
    });
  });

  it("does not auto-refresh on POSIX when a path is typed", async () => {
    const fetchMock = mockProjectsFetch(); // browse returns POSIX path "/home/dev"

    render(<StatsTab onSelectProject={() => {}} />);
    fireEvent.click(await screen.findByRole("button", { name: "Index your first repository" }));
    await screen.findByText("alpha"); // initial POSIX listing

    const browseCalls = () =>
      fetchMock.mock.calls.filter((c) => String(c[0]).startsWith("/api/browse")).length;
    const before = browseCalls();

    fireEvent.change(screen.getByLabelText("Repository path"), {
      target: { value: "/usr/local" },
    });

    /* Wait past the debounce window; a POSIX path must NOT trigger a re-browse. */
    await new Promise((r) => setTimeout(r, 400));
    expect(browseCalls()).toBe(before);
  });
});
