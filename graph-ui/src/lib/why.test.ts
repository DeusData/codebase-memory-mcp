import { describe, expect, it } from "vitest";
import { messages } from "./i18n";
import { formatGuard, formatGuardChain, allUnguarded, type WhyEntry } from "./why";

describe("formatGuard", () => {
  it("renders if/unless, case, loop and catch guards as words", () => {
    expect(formatGuard({ kind: "if", cond: "mode > 2" })).toBe("when mode > 2");
    expect(formatGuard({ kind: "if", cond: "strict", negated: true })).toBe(
      "unless strict",
    );
    expect(formatGuard({ kind: "case", cond: "CBM_LANG_C" })).toBe("case CBM_LANG_C");
    expect(formatGuard({ kind: "loop", cond: "i < n" })).toBe("looping while i < n");
    expect(formatGuard({ kind: "loop" })).toBe("in a loop");
    expect(formatGuard({ kind: "catch" })).toBe("on error handling");
  });
  it("chains outermost-first with arrows", () => {
    expect(
      formatGuardChain([
        { kind: "if", cond: "mode > 2" },
        { kind: "if", cond: "strict" },
      ]),
    ).toBe("when mode > 2 → when strict");
  });
  it("composes zh guards with native grammar, conditions verbatim", () => {
    const zh = messages.zh.why;
    expect(formatGuard({ kind: "if", cond: "mode > 2" }, zh)).toBe("当 mode > 2 时");
    expect(formatGuard({ kind: "if", cond: "strict", negated: true }, zh)).toBe(
      "除非 strict",
    );
    expect(formatGuard({ kind: "case", cond: "CBM_LANG_C" }, zh)).toBe(
      "匹配 CBM_LANG_C 分支",
    );
    expect(formatGuard({ kind: "loop", cond: "i < n" }, zh)).toBe("当 i < n 时循环");
    expect(formatGuard({ kind: "loop" }, zh)).toBe("在循环中");
    expect(formatGuard({ kind: "catch" }, zh)).toBe("在错误处理中");
  });
  it("chains a zh guard sentence outermost-first", () => {
    expect(
      formatGuardChain(
        [
          { kind: "if", cond: "mode > 2" },
          { kind: "if", cond: "strict", negated: true },
        ],
        messages.zh.why,
      ),
    ).toBe("当 mode > 2 时 → 除非 strict");
  });
});

describe("allUnguarded", () => {
  const entry = (guards: WhyEntry["guards"], loop?: boolean): WhyEntry => ({
    id: 1,
    name: "f",
    guards,
    loop,
    more: 0,
  });
  it("is true only when no entry carries a guard or loop", () => {
    expect(allUnguarded([entry([])])).toBe(true);
    expect(allUnguarded([entry([{ kind: "if", cond: "x" }])])).toBe(false);
    expect(allUnguarded([entry([], true)])).toBe(false);
  });
});
