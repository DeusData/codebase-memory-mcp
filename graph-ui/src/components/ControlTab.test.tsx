/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ControlTab, parseElapsedSeconds } from "./ControlTab";
import { messages } from "../lib/i18n";

vi.mock("../lib/i18n", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../lib/i18n")>();
  return { ...actual, useUiMessages: () => messages.en };
});

describe("parseElapsedSeconds", () => {
  it("parses Windows elapsed format (days-hh:mm:ss)", () => {
    expect(parseElapsedSeconds("0-03:55:04")).toBe(14104);
    expect(parseElapsedSeconds("1-02:03:04")).toBe(86400 + 2 * 3600 + 3 * 60 + 4);
  });

  it("parses POSIX elapsed formats (hh:mm:ss and mm:ss)", () => {
    expect(parseElapsedSeconds("01:30:00")).toBe(5400);
    expect(parseElapsedSeconds("05:20")).toBe(320);
  });

  it("handles empty or invalid strings gracefully", () => {
    expect(parseElapsedSeconds("")).toBe(0);
    expect(parseElapsedSeconds("invalid")).toBe(0);
  });
});

describe("ControlTab", () => {
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("normalizes cumulative CPU seconds to realistic percentage", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        if (url.includes("/api/processes")) {
          return new Response(
            JSON.stringify({
              self_pid: 16856,
              self_rss_mb: 14.2,
              self_user_cpu_s: 23.3,
              self_sys_cpu_s: 7.4,
              processes: [
                {
                  pid: 17500,
                  cpu: 118.1, // cumulative seconds
                  rss_mb: 8.5,
                  elapsed: "0-03:55:04",
                  command: "codebase-memory-mcp",
                  is_self: false,
                },
                {
                  pid: 16856,
                  cpu: 30.7, // cumulative seconds
                  rss_mb: 14.2,
                  elapsed: "0-03:54:58",
                  command: "codebase-memory-mcp",
                  is_self: true,
                },
              ],
            }),
            { status: 200, headers: { "Content-Type": "application/json" } }
          );
        }
        if (url.includes("/api/logs")) {
          return new Response(JSON.stringify({ lines: [], total: 0 }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        return new Response("{}", { status: 200 });
      })
    );

    render(<ControlTab />);

    // Wait for the processes to load
    await waitFor(() => {
      expect(screen.getByText("PID 17500")).toBeInTheDocument();
      expect(screen.getByText("PID 16856")).toBeInTheDocument();
    });

    // Verify CPU % is normalized to ~0.8% and ~0.2%, not 118.1% or 30.7%
    expect(screen.getByText("0.8%")).toBeInTheDocument();
    expect(screen.getByText("0.2%")).toBeInTheDocument();

    // Verify cumulative seconds are displayed as total
    expect(screen.getByText("118.1s total")).toBeInTheDocument();
    expect(screen.getByText("30.7s total")).toBeInTheDocument();
  });
});
