/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MetricChip } from "./MetricChip";
import { WikiPanel } from "./WikiPanel";
import {
  DEFAULT_DISPLAY_SETTINGS,
  loadDisplaySettings,
  saveDisplaySettings,
} from "../lib/density";

/* The hover card is exercised through MetricChip — the integration every
 * chip everywhere actually gets. Timing is deterministic via fake timers. */
describe("WikiHoverCard", () => {
  beforeEach(() => {
    localStorage.clear();
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  const hoverChip = () => {
    const chip = screen.getByRole("button", { name: "hotspot" });
    fireEvent.mouseEnter(chip);
    return chip;
  };

  it("shows the entry preview after the default dwell on hover", () => {
    render(<MetricChip slug="hotspot" onOpen={() => {}} />);
    hoverChip();

    act(() => vi.advanceTimersByTime(349));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();

    act(() => vi.advanceTimersByTime(1));
    const card = screen.getByRole("tooltip");
    expect(card).toBeInTheDocument();
    /* term + tier chip + the single-source sentence + the hint line */
    expect(card).toHaveTextContent("hotspot");
    expect(card).toHaveTextContent("first-class");
    expect(card).toHaveTextContent(/where fixes cluster/);
    expect(card).toHaveTextContent("click for more →");
  });

  it("never shows when the pointer leaves before the dwell elapses", () => {
    render(<MetricChip slug="hotspot" onOpen={() => {}} />);
    const chip = hoverChip();

    act(() => vi.advanceTimersByTime(200));
    fireEvent.mouseLeave(chip);
    act(() => vi.advanceTimersByTime(5000));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("shows on keyboard focus and closes after blur (symmetric with hover)", () => {
    render(<MetricChip slug="hotspot" onOpen={() => {}} />);
    const chip = screen.getByRole("button", { name: "hotspot" });

    fireEvent.focus(chip);
    act(() => vi.advanceTimersByTime(350));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();

    fireEvent.blur(chip);
    act(() => vi.advanceTimersByTime(150));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("stays open when the pointer moves into the card, with no auto-timeout", () => {
    render(<MetricChip slug="hotspot" onOpen={() => {}} />);
    const chip = hoverChip();
    act(() => vi.advanceTimersByTime(350));

    /* Leave the chip within the grace, land on the card — it must survive. */
    fireEvent.mouseLeave(chip);
    fireEvent.mouseEnter(screen.getByRole("tooltip"));
    act(() => vi.advanceTimersByTime(60_000)); /* persistent: no timeout */
    expect(screen.getByRole("tooltip")).toBeInTheDocument();

    /* Leaving the card closes after the grace. */
    fireEvent.mouseLeave(screen.getByRole("tooltip"));
    act(() => vi.advanceTimersByTime(150));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("Escape dismisses the card without closing the panel underneath", () => {
    const onClose = vi.fn();
    render(<WikiPanel slug="hotspot" onClose={onClose} onNavigate={() => {}} />);

    /* hotspot's footer pairs it with tested — hover that chip. */
    fireEvent.mouseEnter(screen.getByRole("button", { name: "tested" }));
    act(() => vi.advanceTimersByTime(350));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();

    fireEvent.keyDown(document, { key: "Escape" });
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(onClose).not.toHaveBeenCalled();
  });

  it("keeps the click behavior: opens the wiki and yields the preview", () => {
    const onOpen = vi.fn();
    render(<MetricChip slug="hotspot" onOpen={onOpen} />);
    const chip = hoverChip();
    act(() => vi.advanceTimersByTime(350));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();

    fireEvent.click(chip);
    expect(onOpen).toHaveBeenCalledWith("hotspot");
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("respects the delay configured in Display settings", () => {
    saveDisplaySettings({ ...DEFAULT_DISPLAY_SETTINGS, tooltipDelayMs: 600 });
    render(<MetricChip slug="hotspot" onOpen={() => {}} />);
    hoverChip();

    act(() => vi.advanceTimersByTime(599));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    act(() => vi.advanceTimersByTime(1));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
  });
});

describe("tooltip delay setting", () => {
  beforeEach(() => localStorage.clear());

  it("round-trips localStorage", () => {
    saveDisplaySettings({ ...DEFAULT_DISPLAY_SETTINGS, tooltipDelayMs: 500 });
    expect(loadDisplaySettings().tooltipDelayMs).toBe(500);
  });

  it("falls back to the default when an older payload lacks the key", () => {
    localStorage.setItem("cbm-display", JSON.stringify({ edgeBrightness: 2 }));
    const settings = loadDisplaySettings();
    expect(settings.edgeBrightness).toBe(2);
    expect(settings.tooltipDelayMs).toBe(DEFAULT_DISPLAY_SETTINGS.tooltipDelayMs);
  });
});
