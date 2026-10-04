/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToolsTab, MCP_TOOLS } from "./ToolsTab";

describe("ToolsTab", () => {
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("renders all 18 MCP tools in the catalog", () => {
    render(<ToolsTab selectedProject="test-project" />);

    expect(screen.getByText("MCP Tools Directory")).toBeInTheDocument();
    expect(screen.getByText("18 Tools Registered")).toBeInTheDocument();

    // Verify key tools are displayed
    expect(screen.getByText("search_graph")).toBeInTheDocument();
    expect(screen.getByText("query_graph")).toBeInTheDocument();
    expect(screen.getByText("trace_path")).toBeInTheDocument();
    expect(screen.getByText("export_diagram")).toBeInTheDocument();
    expect(screen.getByText("index_repository")).toBeInTheDocument();
    expect(screen.getByText("get_code_snippet")).toBeInTheDocument();

    expect(MCP_TOOLS.length).toBe(18);
  });

  it("filters tools by search text", () => {
    render(<ToolsTab selectedProject="test-project" />);

    const searchInput = screen.getByPlaceholderText(/Search tools by name/i);
    fireEvent.change(searchInput, { target: { value: "export_diagram" } });

    expect(screen.getByText("export_diagram")).toBeInTheDocument();
    expect(screen.queryByText("search_graph")).not.toBeInTheDocument();
    expect(screen.queryByText("index_repository")).not.toBeInTheDocument();
  });

  it("filters tools by category pill", () => {
    render(<ToolsTab selectedProject="test-project" />);

    // Click on "Tracing & Architecture"
    const tracingBtn = screen.getByRole("button", { name: /Tracing & Architecture/i });
    fireEvent.click(tracingBtn);

    expect(screen.getByText("export_diagram")).toBeInTheDocument();
    expect(screen.getByText("trace_path")).toBeInTheDocument();
    expect(screen.getByText("get_architecture")).toBeInTheDocument();
    expect(screen.getByText("compare_graphs")).toBeInTheDocument();

    // Should not contain tools from other categories
    expect(screen.queryByText("search_graph")).not.toBeInTheDocument();
    expect(screen.queryByText("index_repository")).not.toBeInTheDocument();
  });

  it("filters tools by read-only toggle", () => {
    render(<ToolsTab selectedProject="test-project" />);

    const readOnlyToggle = screen.getByRole("button", { name: /Read-only only/i });
    fireEvent.click(readOnlyToggle);

    // Read-only tools should be visible
    expect(screen.getByText("search_graph")).toBeInTheDocument();
    expect(screen.getByText("export_diagram")).toBeInTheDocument();

    // Mutating tools should be filtered out
    expect(screen.queryByText("index_repository")).not.toBeInTheDocument();
    expect(screen.queryByText("delete_project")).not.toBeInTheDocument();
  });

  it("expands tool details and displays parameter table and copy buttons", () => {
    render(<ToolsTab selectedProject="sample-repo" />);

    // By default export_diagram is expanded
    expect(screen.getByText("Parameters (6 total, 2 required)")).toBeInTheDocument();
    expect(screen.getByText("Active Project: sample-repo")).toBeInTheDocument();

    // Check parameters
    expect(screen.getByText("architecture")).toBeInTheDocument();
    expect(screen.getByText("sequence")).toBeInTheDocument();
    expect(screen.getByText("Copy JSON")).toBeInTheDocument();
    expect(screen.getByText("Copy CLI")).toBeInTheDocument();
  });
});
