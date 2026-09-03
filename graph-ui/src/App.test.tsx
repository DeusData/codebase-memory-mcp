/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import { messages } from "./lib/i18n";

vi.mock("./components/GraphTab", () => ({ GraphTab: () => null }));
vi.mock("./components/StatsTab", () => ({ StatsTab: () => null }));
vi.mock("./components/ControlTab", () => ({ ControlTab: () => null }));
vi.mock("./lib/i18n", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./lib/i18n")>();
  return { ...actual, useUiMessages: () => messages.en };
});

describe("App", () => {
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
    window.history.replaceState(null, "", "/");
  });

  it("shows the serving binary version", async () => {
    vi.stubGlobal("fetch", vi.fn(async () =>
      new Response(JSON.stringify({ lang: "en", version: "0.10.8" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    ));

    render(<App />);

    expect(await screen.findByText("v0.10.8")).toBeVisible();
  });

  it("hides the version when the config has no string version", async () => {
    const fetchMock = vi.fn(async () =>
      new Response(JSON.stringify({ lang: "en", version: 108 }), { status: 200 }),
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/ui-config"));
    expect(screen.queryByTitle("Server version")).not.toBeInTheDocument();
  });

  it("hides the version when the config request fails", async () => {
    const fetchMock = vi.fn(async () => {
      throw new Error("offline");
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/ui-config"));
    expect(screen.queryByTitle("Server version")).not.toBeInTheDocument();
  });
});