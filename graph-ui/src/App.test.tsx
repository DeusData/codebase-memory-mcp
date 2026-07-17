import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";


vi.mock("./components/GraphTab", () => ({
  GraphTab: ({ project }: { project: string | null }) => <div>graph:{project}</div>,
}));

vi.mock("./components/StatsTab", () => ({
  StatsTab: ({ onSelectProject }: { onSelectProject: (project: string) => void }) => (
    <button onClick={() => onSelectProject("library-project")}>select library-project</button>
  ),
}));

vi.mock("./components/ControlTab", () => ({
  ControlTab: () => <div>control panel</div>,
}));

describe("App routing", () => {
  beforeEach(() => {
    window.history.replaceState(null, "", "/");
    localStorage.clear();
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("normalizes the default route and disables graph until a project is selected", () => {
    render(<App />);

    expect(screen.getByRole("button", { name: "Graph" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "select library-project" })).toBeInTheDocument();
    expect(window.location.search).toBe("?tab=stats");
  });

  it("selects a project, loads the graph route, and clears the selection", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "select library-project" }));

    await waitFor(() => expect(screen.getByText("graph:library-project")).toBeInTheDocument());
    expect(window.location.search).toBe("?tab=graph&project=library-project");
    expect(screen.getByText("library-project")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "×" }));
    expect(window.location.search).toBe("?tab=stats");
    expect(screen.getByRole("button", { name: "Graph" })).toBeDisabled();
  });

  it("navigates to control and restores a route on popstate", () => {
    window.history.replaceState(null, "", "/?tab=control&project=alpha");
    render(<App />);
    expect(screen.getByText("control panel")).toBeInTheDocument();

    act(() => {
      window.history.pushState(null, "", "/?tab=stats");
      window.dispatchEvent(new PopStateEvent("popstate"));
    });
    expect(screen.getByRole("button", { name: "select library-project" })).toBeInTheDocument();
  });

  it("does not push a duplicate route", () => {
    window.history.replaceState(null, "", "/?tab=control");
    const push = vi.spyOn(window.history, "pushState");
    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Control" }));
    expect(push).not.toHaveBeenCalled();
  });
});
