import { describe, expect, it } from "vitest";
import { messages } from "./i18n";
import { allHopsObserved, newestObserved, observedTitle } from "./observed";

const obs = (label: string, last_seen: string, count = 1) => ({
  count,
  label,
  last_seen,
});

describe("allHopsObserved", () => {
  it("is false with no hops to judge — empty path or origin alone", () => {
    expect(allHopsObserved([])).toBe(false);
    expect(allHopsObserved([{}])).toBe(false);
    expect(allHopsObserved([{ observed: obs("pytest", "2026-08-27") }])).toBe(false);
  });

  it("is true when every non-root hop is observed — the origin never counts", () => {
    expect(
      allHopsObserved([
        {},
        { observed: obs("pytest 2026-08-27", "2026-08-27T10:00:00Z") },
        { observed: obs("pytest 2026-08-27", "2026-08-27T10:00:00Z") },
      ]),
    ).toBe(true);
  });

  it("is false as soon as one non-root hop was never recorded", () => {
    expect(
      allHopsObserved([
        {},
        { observed: obs("pytest 2026-08-27", "2026-08-27T10:00:00Z") },
        {},
      ]),
    ).toBe(false);
  });
});

describe("newestObserved", () => {
  it("is null when nothing was observed", () => {
    expect(newestObserved([])).toBeNull();
    expect(newestObserved([{}, {}])).toBeNull();
  });

  it("names the most recently seen run across steps", () => {
    const newest = newestObserved([
      { observed: obs("pytest 2026-08-25", "2026-08-25T09:00:00Z") },
      {},
      { observed: obs("pytest 2026-08-27", "2026-08-27T14:12:00Z") },
      { observed: obs("pytest 2026-08-26", "2026-08-26T09:00:00Z") },
    ]);
    expect(newest?.label).toBe("pytest 2026-08-27");
  });
});

describe("observedTitle", () => {
  it("carries the run label and the ISO date of the newest sighting", () => {
    expect(observedTitle(obs("pytest 2026-08-27", "2026-08-27T14:12:00Z", 4))).toBe(
      "ran in pytest 2026-08-27 · last 2026-08-27",
    );
  });

  it("speaks the zh catalog when its messages are passed", () => {
    expect(
      observedTitle(obs("pytest 2026-08-27", "2026-08-27T14:12:00Z", 4), messages.zh.observed),
    ).toBe("曾在 pytest 2026-08-27 中执行 · 最近 2026-08-27");
  });
});
