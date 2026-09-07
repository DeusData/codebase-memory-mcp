/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { WikiPanel } from "./WikiPanel";

describe("WikiPanel", () => {
  it("renders the entry with its sections, caps as warnings included", () => {
    render(<WikiPanel slug="hotspot" onClose={() => {}} onNavigate={() => {}} />);

    expect(screen.getByRole("heading", { name: "hotspot" })).toBeInTheDocument();
    expect(screen.getByText("first-class")).toBeInTheDocument();
    expect(screen.getByText(/where fixes cluster/)).toBeInTheDocument();
    expect(screen.getByText("Why this matters")).toBeInTheDocument();
    expect(screen.getByText("How it's computed")).toBeInTheDocument();
    expect(screen.getByText(/files under 100 lines are excluded/)).toBeInTheDocument();
    expect(screen.getByText("What this does not cover")).toBeInTheDocument();
    expect(screen.getByText("Where it appears")).toBeInTheDocument();
    expect(screen.getByText("M-HOTSPOT")).toBeInTheDocument();
  });

  it("hides absent sections — a refused entry has no notCovered", () => {
    render(
      <WikiPanel slug="halstead" onClose={() => {}} onNavigate={() => {}} />,
    );

    expect(screen.getByText("refused")).toBeInTheDocument();
    expect(screen.queryByText("What this does not cover")).not.toBeInTheDocument();
    expect(screen.queryByText("How it's computed")).not.toBeInTheDocument();
  });

  it("navigates via the pairedWith chip and closes on Escape", () => {
    const onNavigate = vi.fn();
    const onClose = vi.fn();
    render(<WikiPanel slug="hotspot" onClose={onClose} onNavigate={onNavigate} />);

    /* hotspot is paired with tested — the chip pushes that entry. */
    fireEvent.click(screen.getByRole("button", { name: "tested" }));
    expect(onNavigate).toHaveBeenCalledWith("tested");

    fireEvent.keyDown(window, { key: "Escape" });
    expect(onClose).toHaveBeenCalled();
  });

  it("pops the navigation stack with the back arrow", () => {
    const onNavigate = vi.fn();
    const { rerender } = render(
      <WikiPanel slug="hotspot" onClose={() => {}} onNavigate={onNavigate} />,
    );

    /* The host owns the slug; a chip click comes back as a prop change. */
    rerender(<WikiPanel slug="churn" onClose={() => {}} onNavigate={onNavigate} />);
    expect(screen.getByRole("heading", { name: "churn" })).toBeInTheDocument();

    fireEvent.click(screen.getByTitle("Back"));
    expect(onNavigate).toHaveBeenCalledWith("hotspot");
  });
});
