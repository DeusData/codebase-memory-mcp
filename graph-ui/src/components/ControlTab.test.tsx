import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ControlTab } from "./ControlTab";


vi.mock("@/components/ui/scroll-area", () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));

describe("ControlTab", () => {
  const defaultFetch = async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.startsWith("/api/processes")) {
      return {
        json: async () => ({
          self_rss_mb: 128,
          self_user_cpu_s: 2,
          self_sys_cpu_s: 1,
          processes: [
            { pid: 101, cpu: 20, rss_mb: 128, elapsed: "10s", command: "cbm", is_self: true },
            { pid: 202, cpu: 90, rss_mb: 256, elapsed: "20s", command: "worker", is_self: false },
          ],
        }),
      } as Response;
    }
    if (url.startsWith("/api/logs")) {
      return {
        json: async () => ({ lines: ["level=info ready", "level=warn slow", "level=error failed"] }),
      } as Response;
    }
    return { json: async () => ({ ok: true }) } as Response;
  };
  const fetchMock = vi.fn(defaultFetch);

  const processCallCount = () =>
    fetchMock.mock.calls.filter(([input]) => String(input) === "/api/processes").length;

  beforeEach(() => {
    fetchMock.mockReset();
    fetchMock.mockImplementation(defaultFetch);
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("confirm", vi.fn(() => false));
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("renders live process metrics, selection, logs, and refresh behavior", async () => {
    render(<ControlTab />);

    await waitFor(() => expect(screen.getByText("PID 101")).toBeInTheDocument());
    expect(screen.getByText("PID 202")).toBeInTheDocument();
    expect(screen.getByText("THIS")).toBeInTheDocument();
    expect(screen.getByText("level=warn slow")).toHaveClass("text-yellow-400/60");
    expect(screen.getByText("level=error failed")).toHaveClass("text-red-400/70");

    fireEvent.click(screen.getByText("PID 202"));
    const callsBeforeRefresh = processCallCount();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(processCallCount()).toBe(callsBeforeRefresh + 1));

    fireEvent.click(screen.getByRole("button", { name: "Kill" }));
    expect(confirm).toHaveBeenCalledWith("Kill process 202?");
    expect(fetchMock).not.toHaveBeenCalledWith("/api/process-kill", expect.anything());
  });

  it("posts a confirmed process kill and schedules a process refresh", async () => {
    vi.stubGlobal("confirm", vi.fn(() => true));
    render(<ControlTab />);
    await waitFor(() => expect(screen.getByText("PID 202")).toBeInTheDocument());
    const timeout = vi.spyOn(globalThis, "setTimeout").mockImplementation((() => 0) as typeof setTimeout);

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Kill" }));
      await Promise.resolve();
    });

    expect(fetchMock).toHaveBeenCalledWith("/api/process-kill", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pid: 202 }),
    });
    expect(timeout).toHaveBeenCalledWith(expect.any(Function), 1000);
    timeout.mockRestore();
  });

  it("shows empty states when process and log responses contain no rows", async () => {
    fetchMock.mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      return {
        json: async () => (url.startsWith("/api/processes") ? { processes: [] } : { lines: [] }),
      } as Response;
    });

    render(<ControlTab />);
    await waitFor(() => expect(screen.getByText("No processes found")).toBeInTheDocument());
    expect(screen.getByText("No logs yet")).toBeInTheDocument();
  });
});
