import { describe, expect, it } from "vitest";
import { surprisingCouplings, suggestedQuestions } from "./firstread";
import { messages } from "./i18n";
import type { RegionsPayload } from "./types";

/* One coupling that trips all three reasons: heavy, cross-top-level, and
 * touching a low-cohesion region big enough to also earn the split
 * question. */
const payload: RegionsPayload = {
  level: "regions",
  method: "leiden+folders",
  total_nodes: 100,
  unmapped_nodes: 0,
  regions: [
    { id: 0, name: "src/core", files: 4, members: 60, cohesion: 0.8, top_nodes: [], x: 0, y: 0, z: 0, size: 1, color: "#111111" },
    { id: 1, name: "web/api", files: 3, members: 50, cohesion: 0.2, top_nodes: [], x: 0, y: 0, z: 0, size: 1, color: "#222222" },
  ],
  edges: [{ source: 0, target: 1, weight: 1200 }],
};

describe("surprisingCouplings", () => {
  it("explains the ranking in English by default", () => {
    const [coupling] = surprisingCouplings(payload);
    expect(coupling.reasons).toEqual([
      "1,200 edges cross the boundary",
      "links src/ to web/ — different top-level areas",
      "web/api holds together loosely (cohesion 0.20)",
    ]);
  });
  it("composes the reasons natively in zh from the same parameters", () => {
    const [coupling] = surprisingCouplings(payload, 5, messages.zh.firstread);
    expect(coupling.reasons).toEqual([
      "1,200 条边跨越这条边界",
      "连接 src/ 与 web/——分属不同的顶层目录",
      "web/api 内聚松散（cohesion 0.20）",
    ]);
  });
});

describe("suggestedQuestions", () => {
  it("asks in English by default, evidence joined with a semicolon", () => {
    const questions = suggestedQuestions(payload, { deadCount: 1234 });
    expect(questions[0].question).toBe("Why does src/core depend on web/api?");
    expect(questions[0].why).toContain("; ");
    expect(questions.map((q) => q.question)).toContain(
      "Are the 1,234 functions with no callers safe to delete?",
    );
  });
  it("asks the zh questions with zh evidence, seeds untouched", () => {
    const questions = suggestedQuestions(
      payload,
      { deadCount: 1234 },
      5,
      messages.zh.firstread,
    );
    expect(questions[0].question).toBe("为什么 src/core 依赖 web/api？");
    expect(questions[0].why).toContain("；");
    expect(questions.map((q) => q.question)).toContain("web/api 应该拆分成更小的模块吗？");
    const dead = questions.find((q) => q.about.includes("dead code"))!;
    expect(dead.question).toBe("这 1,234 个没有调用者的函数可以安全删除吗？");
    expect(dead.why).toBe("没有任何 CALLS 或 USAGE 到达它们——已排除入口点与测试");
  });
});
