/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DiagramsTab, DIAGRAMS } from "./DiagramsTab";

describe("DiagramsTab", () => {
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("renders gallery view with all 6 verified diagrams", () => {
    render(<DiagramsTab />);

    expect(screen.getByText("Codebase Memory MCP Visualizations")).toBeVisible();
    expect(screen.getByText("Archify Architecture Suite")).toBeVisible();

    DIAGRAMS.forEach((diagram) => {
      expect(screen.getAllByText(diagram.title).length).toBeGreaterThanOrEqual(1);
    });

    expect(screen.getByText("6 / 6")).toBeInTheDocument();
    expect(screen.getByText("100%")).toBeInTheDocument();
  });

  it("navigates to diagram viewer when a card is clicked", async () => {
    const onSelectDiagram = vi.fn();
    render(<DiagramsTab onSelectDiagram={onSelectDiagram} />);

    const archCardTitle = screen.getByRole("heading", { name: "System Architecture" });
    fireEvent.click(archCardTitle);

    expect(onSelectDiagram).toHaveBeenCalledWith("architecture");
    expect(await screen.findByTitle("System Architecture")).toBeInTheDocument();
    expect(screen.getByText("Open Standalone")).toBeInTheDocument();
    expect(screen.getByText("Spec")).toBeInTheDocument();
  });

  it("navigates to export diagram sequence viewer when its card is clicked", async () => {
    const onSelectDiagram = vi.fn();
    render(<DiagramsTab onSelectDiagram={onSelectDiagram} />);

    const exportSeqTitle = screen.getByRole("heading", { name: "Export Diagram Sequence" });
    fireEvent.click(exportSeqTitle);

    expect(onSelectDiagram).toHaveBeenCalledWith("sequence_export");
    expect(await screen.findByTitle("Export Diagram Sequence")).toBeInTheDocument();
  });

  it("navigates back to overview when Overview button is clicked", async () => {
    render(<DiagramsTab initialDiagram="architecture" />);

    expect(await screen.findByTitle("System Architecture")).toBeInTheDocument();

    const overviewBtn = screen.getByRole("button", { name: /overview/i });
    fireEvent.click(overviewBtn);

    expect(screen.getByText("Codebase Memory MCP Visualizations")).toBeVisible();
  });

  it("allows switching between diagrams using top navbar buttons", async () => {
    render(<DiagramsTab initialDiagram="architecture" />);

    expect(await screen.findByTitle("System Architecture")).toBeInTheDocument();

    const workflowBtn = screen.getByRole("button", { name: /multi-pass indexing pipeline/i });
    fireEvent.click(workflowBtn);

    expect(await screen.findByTitle("Multi-Pass Indexing Pipeline")).toBeInTheDocument();
  });

  it("opens standalone window when Open Standalone is clicked", async () => {
    const openMock = vi.fn();
    vi.stubGlobal("open", openMock);

    render(<DiagramsTab initialDiagram="architecture" />);

    const standaloneBtn = await screen.findByRole("button", { name: /open standalone/i });
    fireEvent.click(standaloneBtn);

    await waitFor(() => {
      expect(openMock).toHaveBeenCalled();
    });
  });
});
