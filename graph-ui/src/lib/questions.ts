/* The question index — Atlas's honest scorecard. Twenty years of
 * developer-information-needs research reduces to 18 question families;
 * this module says, per family, whether Atlas answers it, answers it
 * partially, or does not answer it yet. Honesty about the gaps is a
 * deliberate product feature — the wording is not softened, in either
 * language. Chinese content lives co-located per family (zh) with
 * per-field English fallback; UI chrome around it is in i18n.ts. */
import type { UiLanguage } from "./i18n";
import type { TabId } from "./types";

export type QuestionStatus = "answers" | "partial" | "lacks";

/* Only the project tabs a question row can navigate to. */
export type QuestionTab = Extract<
  TabId,
  "overview" | "modules" | "graph" | "flows" | "changes" | "dashboard"
>;

export interface QuestionFamily {
  id: string;
  question: string;
  status: QuestionStatus;
  tab?: QuestionTab;
  /* Where to look / how to get there when a tab alone isn't enough. */
  hint?: string;
  /* What's missing — required for partial and lacks. */
  gap?: string;
  /* Simplified-Chinese content; absent fields fall back to English. */
  zh?: { question?: string; hint?: string; gap?: string };
}

export const QUESTION_FAMILIES: QuestionFamily[] = [
  {
    id: "F1",
    question: "Where is the code that does X?",
    status: "answers",
    tab: "graph",
    hint: "Galaxy + Modules + ⌘K search; scent shows where a term concentrates",
    zh: {
      question: "做 X 的代码在哪里？",
      hint: "星系 + 模块 + ⌘K 搜索；scent 显示搜索词聚集在哪里",
    },
  },
  {
    id: "F2",
    question: "What calls this — and what does it call?",
    status: "answers",
    hint: "Open any symbol (⌘K) — callers and callees with resolution confidence",
    zh: {
      question: "谁调用它——它又调用谁？",
      hint: "打开任意符号（⌘K）——调用者与被调用者，附解析置信度",
    },
  },
  {
    id: "F3",
    question: "If I change this, what breaks — and what must I retest?",
    status: "answers",
    tab: "changes",
    hint: "Changes tab for the current diff; every symbol page answers it hypothetically under 'If you change this'",
    zh: {
      question: "改了这里，什么会坏——必须重测什么？",
      hint: "变更页看当前 diff；每个符号页在“如果你改动这里”下做假设性回答",
    },
  },
  {
    id: "F4",
    question: "Is this dead? Can I remove it safely?",
    status: "partial",
    tab: "dashboard",
    gap: "Dead candidates list what has no recorded callers; the caveats you must check first are in the wiki entry",
    zh: {
      question: "这段代码死了吗？能安全删掉吗？",
      gap: "疑似死代码列出没有已记录调用者的符号；删除前必须核对的注意事项在 wiki 词条里",
    },
  },
  {
    id: "F5",
    question: "What code caused this behavior?",
    status: "partial",
    tab: "flows",
    hint: "Flows tab — ▶ observed markers on trace hops and flow steps show what actually ran",
    gap: "Flows and traces now mark observed vs possible hops from ingested runtime traces; automatic trace capture and failure reproduction are not built",
    zh: {
      question: "这个行为是哪段代码引起的？",
      hint: "流程页——追踪跳步与流程步骤上的 ▶ 已观测标记显示真实运行过什么",
      gap: "流程与追踪现在能依据摄入的运行时追踪区分已观测与可能的跳步；自动抓取追踪与失败复现尚未构建",
    },
  },
  {
    id: "F6",
    question: "When does this actually run, and under what conditions?",
    status: "answers",
    hint: "Open any symbol — the trigger tree shows guards, dispatch, and what it triggers",
    zh: {
      question: "它实际何时运行，在什么条件下？",
      hint: "打开任意符号——触发树展示守卫、分发方式以及它会触发什么",
    },
  },
  {
    id: "F7",
    question: "Why was it built this way?",
    status: "partial",
    hint: "Symbol view: docs, ADR panel, file history, and 'Why is this here?' traces a symbol's own git history",
    gap: "Symbol-level history with linked commits and PR/issue refs is here; PR/issue titles and ADR-to-symbol links are not fetched or joined yet",
    zh: {
      question: "当初为什么这么设计？",
      hint: "符号页：文档、ADR 面板、文件历史；“为什么会有这段代码？”追溯符号自身的 git 历史",
      gap: "带提交与 PR/issue 引用链接的符号级历史已就绪；PR/issue 标题与 ADR 到符号的关联尚未抓取或连接",
    },
  },
  {
    id: "F8",
    question: "Did I follow this codebase's conventions?",
    status: "lacks",
    gap: "Conventions conformance is not derived yet — planned",
    zh: {
      question: "我遵守这个代码库的约定了吗？",
      gap: "约定符合度尚未推导——已列入计划",
    },
  },
  {
    id: "F9",
    question: "What tests cover this?",
    status: "partial",
    tab: "dashboard",
    hint: "Tested symbols card; per-symbol 'Tested by'",
    gap: "TESTS-edge reach is shown per symbol; change→test selection is planned",
    zh: {
      question: "哪些测试覆盖它？",
      hint: "已测符号卡片；每个符号页的“覆盖它的测试”",
      gap: "按符号展示 TESTS 边触达；变更→测试选择在计划中",
    },
  },
  {
    id: "F10",
    question: "Where is the risk — what should we fix first?",
    status: "answers",
    tab: "dashboard",
    hint: "Churn × complexity hero with the knee-based head and cost sentence",
    zh: {
      question: "风险在哪里——我们该先修什么？",
      hint: "变更频率 × 复杂度英雄区，带基于拐点的头部与成本句",
    },
  },
  {
    id: "F11",
    question: "What are the components — and are the boundaries respected?",
    status: "answers",
    tab: "overview",
    hint: "Regions, boundary spanners, unusually-coupled pairs",
    zh: {
      question: "有哪些组件——边界被遵守了吗？",
      hint: "区域、跨界符号、异常耦合的区域对",
    },
  },
  {
    id: "F12",
    question: "What changed here, and what changes together with it?",
    status: "partial",
    hint: "Symbol view: file history and co-change",
    gap: "File-granular only; symbol-level evolution is not tracked",
    zh: {
      question: "这里改过什么，什么会随它一起改？",
      hint: "符号页：文件历史与共同变更",
      gap: "只到文件粒度；符号级演化未被跟踪",
    },
  },
  {
    id: "F13",
    question: "Who knows this code — who should I ask?",
    status: "partial",
    hint: "Every symbol page: 'Who can help' — people with their evidence (commits here, breadth, last touched)",
    gap: "File-level only; a region-level rollup is not built — and never a leaderboard",
    zh: {
      question: "谁了解这段代码——我该问谁？",
      hint: "每个符号页的“可求助的人”——带证据的人（此文件提交数、广度、最近改动）",
      gap: "仅到文件粒度；区域级汇总未构建——且绝不做排行榜",
    },
  },
  {
    id: "F14",
    question: "Does this dependency upgrade reach my code — what breaks?",
    status: "lacks",
    gap: "External dependency reachability is not modelled yet — planned",
    zh: {
      question: "这次依赖升级会波及我的代码吗——什么会坏？",
      gap: "外部依赖可达性尚未建模——已列入计划",
    },
  },
  {
    id: "F15",
    question: "How do I use this API idiomatically?",
    status: "partial",
    hint: "Symbol view: snippets and near-clones",
    gap: "Deliberately not invested — LLMs answer idiom questions well; Atlas shows real call sites and near-clones instead",
    zh: {
      question: "这个 API 的惯用法是什么？",
      hint: "符号页：代码片段与近似克隆",
      gap: "有意不投入——LLM 很擅长回答惯用法问题；Atlas 转而展示真实调用点与近似克隆",
    },
  },
  {
    id: "F16",
    question: "Is this bug report legit — how hard is it to fix?",
    status: "partial",
    tab: "overview",
    hint: "Needs attention (Overview) + Changes risk",
    gap: "Needs-attention ranks code-side difficulty; there is no issue-tracker join",
    zh: {
      question: "这个 bug 报告靠谱吗——修起来有多难？",
      hint: "需要关注（总览）+ 变更页风险",
      gap: "“需要关注”只排代码侧的难度；没有 issue 跟踪器关联",
    },
  },
  {
    id: "F17",
    question: "What did the agent touch — and what did it not look at?",
    status: "lacks",
    gap: "Agent-diff provenance is not built yet — planned",
    zh: {
      question: "智能体动了什么——它没看什么？",
      gap: "智能体 diff 溯源尚未构建——已列入计划",
    },
  },
  {
    id: "F18",
    question: "What do I hand my agent so it doesn't guess?",
    status: "answers",
    hint: "The Prompt composer collects cited evidence; flows copy as mermaid; the handout is one click",
    zh: {
      question: "我该把什么交给智能体，让它不用猜？",
      hint: "Prompt 合成器收集带引用的证据；流程可复制为 mermaid；讲义一键生成",
    },
  },
];

/* Derived, never hard-coded — the scorecard header counts itself. */
export function questionStatusCounts(
  families: QuestionFamily[],
): Record<QuestionStatus, number> {
  const counts: Record<QuestionStatus, number> = { answers: 0, partial: 0, lacks: 0 };
  for (const family of families) counts[family.status] += 1;
  return counts;
}

/* One family as one locale sees it — zh fields when the locale is zh and
 * the field exists, English fallback field-by-field. */
export function localizeQuestion(
  family: QuestionFamily,
  lang: UiLanguage,
): Pick<QuestionFamily, "question" | "hint" | "gap"> {
  if (lang !== "zh" || !family.zh) return family;
  return {
    question: family.zh.question ?? family.question,
    hint: family.zh.hint ?? family.hint,
    gap: family.zh.gap ?? family.gap,
  };
}
