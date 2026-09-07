import { useEffect, useState } from "react";

export type UiLanguage = "en" | "zh";

export const messages = {
  en: {
    tabs: {
      overview: "Overview",
      modules: "Modules",
      graph: "Galaxy",
      flows: "Flows",
      changes: "Changes",
      dashboard: "Dashboard",
      projects: "Projects",
      control: "Control",
    },
    switcher: {
      select: "Select a project…",
      search: "Search projects…",
      manage: "manage & index",
    },
    common: {
      cancel: "Cancel",
      refresh: "Refresh",
      loading: "Loading...",
      save: "Save",
      saving: "Saving...",
      delete: "Delete",
      noMatches: "No matches",
      dismiss: "Dismiss",
    },
    graph: {
      selectedLabel: "Graph",
      search: "Search...",
      clearSelection: "Clear selection",
      folders: "Folders",
    },
    projects: {
      indexedProjects: "Indexed Projects",
      noIndexedProjects: "No indexed projects",
      indexFirstRepository: "Index your first repository",
      viewGraph: "View Graph",
      nodes: "nodes",
      edges: "edges",
      deleteTitle: "Delete index",
      deleteConfirm: (name: string) => `Delete index for "${name}"?`,
      healthHealthy: "Database healthy",
      healthMissing: "Database missing",
      healthCorrupt: "Database unhealthy",
      healthChecking: "Checking...",
      indexingInProgress: "Indexing in progress",
      indexingFailed: "Indexing failed",
    },
    index: {
      newIndex: "New Index",
      selectRepositoryFolder: "Select Repository Folder",
      instructions: "Navigate to the project root and click \"Index This Folder\".",
      repositoryPath: "Repository path",
      projectName: "Project ID (optional — permanent, cannot be renamed)",
      projectNamePlaceholder: "Derived from folder name if blank",
      projectNameHelp: "Becomes the database name and query prefix. Leave blank to derive it from the path.",
      filterFolders: "Filter folders",
      noSubdirectories: "No subdirectories",
      indexThisFolder: "Index This Folder",
      starting: "Starting...",
      browseRoot: (path: string) => `Browse ${path}`,
      indexDirectory: (name: string) => `Index ${name}`,
    },
    adr: {
      title: "Architecture Decision Record",
      lastUpdated: "Last updated",
    },
    wiki: {
      whyMatters: "Why this matters",
      howComputed: "How it's computed",
      notCovered: "What this does not cover",
      whereAppears: "Where it appears",
      pairedWith: "Paired with",
      back: "Back",
      close: "Close",
      clickForMore: "click for more →",
      noEntry: (slug: string) => `No wiki entry for “${slug}”.`,
    },
    questions: {
      title: "What can Atlas answer?",
      answers: "answers",
      partial: "partial",
      lacks: "not yet",
      missingPrefix: "missing:",
      summary: (answers: number, partial: number, lacks: number) =>
        `${answers} answered · ${partial} partial · ${lacks} not yet`,
    },
    impact: {
      heading: "If you change this",
      nothingCalls:
        "Nothing recorded calls this — changes here surface only where it is referenced dynamically.",
      sentence: (reachable: number, callableTotal: number, regionTotal: number) =>
        `${reachable.toLocaleString("en-US")} of ${callableTotal.toLocaleString("en-US")} callables can reach this — ${regionTotal.toLocaleString("en-US")} region${regionTotal === 1 ? "" : "s"} could notice.`,
      cappedSuffix: " (walk capped — the true count is higher)",
      runTheseFirst: "Run these first",
      noTestReaches:
        "⚠ No test reaches this symbol — nothing will catch a regression here automatically.",
      basisFootnote:
        "Static CALLS edges only — dynamic dispatch and reflection are not counted; treat the count as a floor.",
      unavailable: (reason: string) => `Impact unavailable: ${reason}`,
      directCallers: (n: number) =>
        `${n.toLocaleString("en-US")} direct caller${n === 1 ? "" : "s"}`,
      withinTwoHops: (n: number) => `${n.toLocaleString("en-US")} within 2 hops`,
      totalWithin: (n: number, depth: number) =>
        `${n.toLocaleString("en-US")} total within ${depth}`,
      testsReach: (count: number, shown: number) =>
        `${count.toLocaleString("en-US")} test${count === 1 ? " reaches" : "s reach"} this symbol${
          shown < count ? ` — showing ${shown} of ${count.toLocaleString("en-US")}` : ""
        }`,
    },
    history: {
      whyHere: "Why is this here?",
      docstringIntro:
        "The docstring above is the stated intent; below is the recorded history.",
      trace: "Trace this symbol's history",
      reading: "Reading git history…",
      unavailable: (reason: string) => `History unavailable: ${reason}`,
      noneReadable: "No git history is readable for this symbol's file.",
      noneForRange: "No commits recorded for this line range.",
      capped: (max: number) => `showing ${max} (capped)`,
      footnote:
        "History follows this symbol's line range through renames (git log -L). PR and issue links open the forge — titles are not fetched.",
    },
    who: {
      heading: "Who can help",
      loading: "Reading authorship…",
      unavailable: (reason: string) => `Authorship unavailable: ${reason}`,
      noHistory: "No git history is readable for this project.",
      noCommits: "No recorded commits touch this file in the window (1y).",
      commitsHere: (n: number) =>
        `${n.toLocaleString("en-US")} commit${n === 1 ? "" : "s"} to this file this year`,
      breadth: (n: number) =>
        `active in ${n.toLocaleString("en-US")} file${n === 1 ? "" : "s"} repo-wide`,
      lastTouched: (date: string) => `last touched ${date}`,
      footer: (total: number, shown: number) =>
        total > shown
          ? `${total.toLocaleString("en-US")} people have recorded history here — showing ${shown} of ${total.toLocaleString("en-US")}`
          : `${total.toLocaleString("en-US")} ${total === 1 ? "person has" : "people have"} recorded history here`,
    },
    observed: {
      word: "observed",
      allObserved: "This whole path was observed in recorded runs.",
      freshness: (label: string) =>
        `Runtime markers from recorded runs (${label}). Unmarked hops are possible, not dead — runs only cover what they exercised.`,
      title: (label: string, date: string) => `ran in ${label} · last ${date}`,
    },
    symbol: {
      loading: "Loading symbol…",
      calledBy: "Called by",
      calls: "Calls",
      none: "None.",
      showMore: (n: number) => `Show more (${n.toLocaleString("en-US")} hidden)`,
      hiddenByFile: "hidden, by file:",
      crossRegions: (cross: number, total: number) => `${cross} of ${total} cross regions`,
      crossRegionsTitle:
        "most of these live in other regions — the cross-seam dot is omitted because it would mark nearly every row",
      crossSeamDotTitle: "in another region — this edge crosses an architectural seam",
      entryPoint: "entry point",
      testFlag: "test",
      exported: "exported",
      regionLink: (name: string) => `region: ${name} →`,
      openInEditor: "Open in editor ↗",
      showCode: "Show code",
      hideCode: "Hide code",
      recentChanges: "Recent changes to this file",
      noGitHistory: "No git history readable for this project.",
      untouchedYear: "Untouched in the last year.",
      commitsLastYear: (n: number) =>
        `${n.toLocaleString("en-US")} commits in the last year`,
      mostlyBy: (author: string, share: number, authors: number) =>
        ` · mostly ${author} (${(share * 100).toFixed(0)}% of ${authors} author${authors > 1 ? "s" : ""})`,
      dataFlows: "Data flows",
      receivesFrom: "receives from",
      feedsInto: "feeds into",
      noDataFlows: "No DATA_FLOWS edges touch this symbol.",
      testedBy: "Tested by",
      noTests: "No TESTS edges reach this symbol.",
      changesTogether: "Changes together with",
      noCoChange: "No co-change history.",
      nearClones: "Near-clones",
      noNearClones: "No near-clones.",
    },
    flows: {
      loading: "Tracing the journeys…",
      traceHeading: "Trace A → B",
      fromPlaceholder: "from symbol…",
      toPlaceholder: "to symbol…",
      modeControl: "control",
      modeData: "data",
      modeTitle: "Follow control flow (CALLS) or data flow (DATA_FLOWS)",
      traceButton: "trace",
      traceFailed: "trace failed",
      reachableIn: (hops: number, viaData: boolean) =>
        `reachable in ${hops} hop${hops === 1 ? "" : "s"} via ${viaData ? "data flow" : "calls"}`,
      notReachable: (maxDepth: number, viaData: boolean, explored?: number) =>
        `not reachable within ${maxDepth} hops (${viaData ? "data flow" : "calls"}; ${explored?.toLocaleString("en-US") ?? ""} nodes explored)`,
      guardTitleHop: "syntactic guard at this hop's call site",
      guardTitleStep: "syntactic guard at this step's call site",
      listHeading: "Flows — entry → terminal",
      summary: (flows: number, callables: number, dropped: number) =>
        `${flows} journeys from ${callables.toLocaleString("en-US")} callables${
          dropped > 0 ? ` · ${dropped.toLocaleString("en-US")} candidates not walked` : ""
        }`,
      acrossRegions: "across regions",
      withinOneRegion: "within one region",
      depthCapTitle: "walk stopped at the depth cap, not at a sink",
      collapse: "− collapse",
      showAll: (n: number, entry: string) => `× ${n} journeys from ${entry} — show all`,
      noFlows: "No flows detected — the project may have no clear entry points.",
      pickJourney: "Pick a journey on the left",
      copied: "Copied ✓",
      copyMermaid: "Copy as mermaid",
      stepsCount: (n: number) => `${n} steps`,
      endsAtSink: "ends at a sink",
      stoppedAtCap: "stopped at the depth cap",
      crossesRegions: "crosses regions",
      branchesBeyondCap: (n: number) => `${n} branches beyond the cap not shown`,
      resolverConfidence: (pct: number) =>
        `resolver confidence ${pct}% — this hop may be misresolved`,
    },
    overview: {
      loading: "Reading the map…",
      handout: "Handout ↗",
      handoutTitle:
        "A self-contained, shareable document explaining this codebase — generated from the graph, print-friendly",
      needsAttention: "Needs attention",
      filesCount: (n: number) => `${n} file${n === 1 ? "" : "s"}`,
      churnUnavailable: "churn unavailable without git history",
      concentrateRisk: "concentrate the churn × complexity risk",
      direction: "Direction",
      firstIndex: "first index",
      sincePrevIndex: "since the previous index",
      trendsAfterReindex: "trends appear after the next reindex",
      riskiestArea: "Riskiest area",
      riskiestSub: (commits: string | number, file: string) =>
        `${commits} commits this year × high complexity — ${file}`,
      noComplexChurn: "no complex churning files",
      trust: "Trust",
      filesMissed: (n: number) => `${n} files missed`,
      trustSub: (date: string, unmapped: number) =>
        `index ${date} · ${unmapped} unmapped symbols`,
      findings: "Findings",
      dismissed: (n: number) => `${n} dismissed`,
      couplingAnd: " and ",
      couplingSuffix: " are unusually coupled",
      inspect: "inspect",
      intended: "intended",
      intendedTitle:
        "Dismiss as intended — re-alerts if the coupling grows an order of magnitude",
      questionsHeading: "Questions this graph can answer — ask your agent",
      regionsHeading: (method: string) => `Regions — the de-facto modules (${method})`,
      hideTestCode: "hide test code",
      testRegionsHidden: (n: number) =>
        `${n} test region${n > 1 ? "s" : ""} hidden — show`,
      regionMeta: (members: number, cohesion: number) =>
        `${members.toLocaleString("en-US")} symbols · cohesion ${cohesion.toFixed(2)}`,
      cohesionAria: (own: number, count: number, median: number) =>
        `cohesion ${own.toFixed(2)} among ${count} regions, median ${median.toFixed(2)}`,
      allRegions: (n: number) => `all ${n} regions in Modules →`,
      bridgesHeading: "Boundary spanners — code that reaches into many regions",
      bridgeMeta: (regions: number, calls: number) =>
        `${regions} regions · ${calls.toLocaleString("en-US")} cross calls`,
      bridgesFootnote:
        "Changes here ripple across regions — mention these names when a task spans areas.",
      referenceSummary: "Reference — hubs, entry points, boundaries",
      hubsHeading: "Hubs — highest fan-in",
      noHotspots: "No hotspot data.",
      entryPoints: "Entry points",
      followFlows: "follow them in Flows →",
      boundariesHeading: "Boundaries — cross-package calls",
      noBoundaries: "No cross-package calls recorded.",
      inventory: (symbols: string, edges: string, regions: string) =>
        `${symbols} symbols · ${edges} edges · ${regions} regions`,
    },
    dashboard: {
      /* Both thresholds arrive from CPLX_BINS via complexityTakeaway — the
       * binding test in complexity.test.ts parses each language's claims
       * back against the bin labels, so a bin edit fails both or neither. */
      takeaway: (
        simplePct: string,
        simpleMax: number,
        tail: number,
        tailMin: number,
        led: string[],
      ) =>
        `${simplePct}% of functions are simple (≤${simpleMax}); ${tail.toLocaleString("en-US")} exceed ${tailMin}${led.length > 0 ? ` — led by ${led.join(", ")}` : ""}.`,
      costSentence: (files: number, filesShare: string, commitShare: string) =>
        `${files} file${files > 1 ? "s" : ""} — ${filesShare}% of the codebase, ${commitShare}% of all commits this year`,
      costConcentrate: " — change here is where defects concentrate.",
    },
    why: {
      whenRuns: "When does this run?",
      whatTriggers: "What does this trigger?",
      toWhatTriggers: "→ what it triggers",
      toWhenRuns: "← when it runs",
      dispatchChip: (n: number) => `◇ 1 of ${n} targets`,
      dispatchTitle:
        "dynamic dispatch: the resolver saw several possible targets for this call site — this is one of them",
      guardsUnavailable: "guards unavailable (source not readable)",
      unguarded: "unguarded — always on this path",
      loopChip: "⟳ loop",
      loopChipTitle: "the call site sits inside a loop",
      guardChipTitle: (kind: string) =>
        `syntactic ${kind} guard around the call site — not a proven path condition`,
      alwaysRunsPrefix: "Always runs when ",
      alwaysRunsSuffix: " runs — no conditions at the call sites.",
      alwaysTriggersPrefix: "Always triggers ",
      alwaysTriggersSuffix: " — unconditionally.",
      alwaysJoiner: " and ",
      honestyFooter:
        "Guards are the syntactic conditions around each call site — not proven path conditions. Dispatch, events and reflection are invisible here.",
      /* Guard fragments — the condition expression stays verbatim code. */
      guardWhen: (cond: string) => `when ${cond}`,
      guardUnless: (cond: string) => `unless ${cond}`,
      guardConditionally: "conditionally",
      guardElseArm: "in an else arm",
      guardCase: (cond: string) => `case ${cond}`,
      guardSwitchCase: "in a switch case",
      guardSwitchOn: (cond: string) => `switch on ${cond}`,
      guardSwitch: "in a switch",
      guardLoopWhile: (cond: string) => `looping while ${cond}`,
      guardLoop: "in a loop",
      guardCatch: "on error handling",
      chainJoiner: " → ",
    },
    firstread: {
      edgesCross: (weight: number) =>
        `${weight.toLocaleString("en-US")} edges cross the boundary`,
      linksAreas: (a: string, b: string) =>
        `links ${a}/ to ${b}/ — different top-level areas`,
      holdsLoosely: (name: string, cohesion: number) =>
        `${name} holds together loosely (cohesion ${cohesion.toFixed(2)})`,
      reasonSeparator: "; ",
      questionWhyDepend: (a: string, b: string) => `Why does ${a} depend on ${b}?`,
      questionSplit: (name: string) => `Should ${name} be split into smaller modules?`,
      whySplit: (pct: string, members: number) =>
        `only ${pct}% of its edges stay inside the region (${members.toLocaleString("en-US")} symbols)`,
      questionDeadSafe: (n: number) =>
        `Are the ${n.toLocaleString("en-US")} functions with no callers safe to delete?`,
      whyDeadSafe: "zero CALLS and zero USAGE reach them, excluding entry points and tests",
      questionUnresolved: "Which call sites does the graph fail to resolve, and why?",
      whyUnresolved: (pct: string) =>
        `${pct}% of call-ish edges are USAGE (no proven single target)`,
    },
    regions: {
      /* The en messages reproduce layout_regions.c's templates byte-for-byte
       * — localizeRegionWhy round-trips English through them unchanged. */
      whyCallCommunity: (files: number, pct: number, dir: string) =>
        `call community: ${files} files, ${pct}% under ${dir}`,
      whyFolderGroup: "folder group (not explained by a kept call community)",
      whyMisc: "files outside every kept community and folder group",
    },
    control: {
      panel: "Control Panel",
      totalCpu: "Total CPU",
      totalRam: "Total RAM",
      processes: "Processes",
      selfRam: "Self RAM",
      activeProcesses: "Active Processes",
      processLogs: "Process Logs",
      noProcesses: "No processes found",
      noLogs: "No logs yet",
      thisProcess: "THIS",
      uptime: "Uptime",
    },
  },
  zh: {
    tabs: {
      overview: "总览",
      modules: "模块",
      graph: "星系",
      flows: "流程",
      changes: "变更",
      dashboard: "仪表盘",
      projects: "项目",
      control: "控制",
    },
    switcher: {
      select: "选择项目…",
      search: "搜索项目…",
      manage: "管理与索引",
    },
    common: {
      cancel: "取消",
      refresh: "刷新",
      loading: "加载中...",
      save: "保存",
      saving: "保存中...",
      delete: "删除",
      noMatches: "无匹配结果",
      dismiss: "关闭",
    },
    graph: {
      selectedLabel: "图谱",
      search: "搜索...",
      clearSelection: "清除选择",
      folders: "目录",
    },
    projects: {
      indexedProjects: "已索引项目",
      noIndexedProjects: "暂无已索引项目",
      indexFirstRepository: "索引第一个仓库",
      viewGraph: "查看图谱",
      nodes: "节点",
      edges: "边",
      deleteTitle: "删除索引",
      deleteConfirm: (name: string) => `删除 "${name}" 的索引？`,
      healthHealthy: "数据库正常",
      healthMissing: "数据库缺失",
      healthCorrupt: "数据库异常",
      healthChecking: "检查中...",
      indexingInProgress: "正在索引",
      indexingFailed: "索引失败",
    },
    index: {
      newIndex: "新建索引",
      selectRepositoryFolder: "选择仓库目录",
      instructions: "导航到项目根目录，然后点击“索引此目录”。",
      repositoryPath: "仓库路径",
      projectName: "项目 ID（可选，永久且不可重命名）",
      projectNamePlaceholder: "留空则从路径派生",
      projectNameHelp: "将作为数据库名称与查询前缀；留空则从路径派生。",
      filterFolders: "筛选目录",
      noSubdirectories: "没有子目录",
      indexThisFolder: "索引此目录",
      starting: "启动中...",
      browseRoot: (path: string) => `浏览 ${path}`,
      indexDirectory: (name: string) => `索引 ${name}`,
    },
    adr: {
      title: "架构决策记录",
      lastUpdated: "最后更新",
    },
    wiki: {
      whyMatters: "为什么重要",
      howComputed: "如何计算",
      notCovered: "不涵盖的内容",
      whereAppears: "出现位置",
      pairedWith: "配对指标",
      back: "返回",
      close: "关闭",
      clickForMore: "点击查看更多 →",
      noEntry: (slug: string) => `没有 “${slug}” 的 wiki 词条。`,
    },
    questions: {
      title: "Atlas 能回答什么？",
      answers: "已回答",
      partial: "部分",
      lacks: "暂缺",
      missingPrefix: "缺少:",
      summary: (answers: number, partial: number, lacks: number) =>
        `${answers} 已回答 · ${partial} 部分 · ${lacks} 暂缺`,
    },
    impact: {
      heading: "如果你改动这里",
      nothingCalls:
        "没有任何已记录的调用指向这里——改动只会在动态引用它的地方显现。",
      sentence: (reachable: number, callableTotal: number, regionTotal: number) =>
        `全仓库 ${callableTotal.toLocaleString("en-US")} 个可调用符号中，有 ${reachable.toLocaleString("en-US")} 个能到达这里——${regionTotal.toLocaleString("en-US")} 个区域可能察觉。`,
      cappedSuffix: "（遍历已达上限——真实数量更高）",
      runTheseFirst: "先跑这些测试",
      noTestReaches:
        "⚠ 没有任何测试到达这个符号——出了回归不会被自动发现。",
      basisFootnote:
        "仅统计静态 CALLS 边——动态分发与反射不计入；请把数字当作下限。",
      unavailable: (reason: string) => `影响不可用：${reason}`,
      directCallers: (n: number) => `${n.toLocaleString("en-US")} 个直接调用者`,
      withinTwoHops: (n: number) => `2 跳内 ${n.toLocaleString("en-US")} 个`,
      totalWithin: (n: number, depth: number) =>
        `${depth} 跳内共 ${n.toLocaleString("en-US")} 个`,
      testsReach: (count: number, shown: number) =>
        `${count.toLocaleString("en-US")} 个测试到达这个符号${
          shown < count ? `——显示其中 ${shown} 个` : ""
        }`,
    },
    history: {
      whyHere: "为什么会有这段代码？",
      docstringIntro: "上方的文档注释是声明的意图；下面是实际记录的历史。",
      trace: "追溯这个符号的历史",
      reading: "正在读取 git 历史…",
      unavailable: (reason: string) => `历史不可用：${reason}`,
      noneReadable: "这个符号所在的文件没有可读的 git 历史。",
      noneForRange: "这段行范围没有记录到任何提交。",
      capped: (max: number) => `仅显示 ${max} 条（已达上限）`,
      footnote:
        "历史沿这个符号自身的行范围追踪，可跨重命名（git log -L）。PR 与 issue 链接指向代码托管平台——标题不会被抓取。",
    },
    who: {
      heading: "可求助的人",
      loading: "正在读取作者记录…",
      unavailable: (reason: string) => `作者记录不可用：${reason}`,
      noHistory: "这个项目没有可读的 git 历史。",
      noCommits: "窗口期（1 年）内没有提交记录触及这个文件。",
      commitsHere: (n: number) => `今年对此文件提交 ${n.toLocaleString("en-US")} 次`,
      breadth: (n: number) => `活跃于全仓 ${n.toLocaleString("en-US")} 个文件`,
      lastTouched: (date: string) => `最近改动 ${date}`,
      footer: (total: number, shown: number) =>
        total > shown
          ? `共 ${total.toLocaleString("en-US")} 人在这里留有提交记录——显示其中 ${shown} 位`
          : `共 ${total.toLocaleString("en-US")} 人在这里留有提交记录`,
    },
    observed: {
      word: "已观测",
      allObserved: "这条路径整体都在录制的运行中被观测到。",
      freshness: (label: string) =>
        `运行时标记来自录制的运行（${label}）。未标记的跳步只是可能执行，不代表死代码——一次运行只覆盖它实际执行到的部分。`,
      title: (label: string, date: string) => `曾在 ${label} 中执行 · 最近 ${date}`,
    },
    symbol: {
      loading: "正在加载符号…",
      calledBy: "调用者",
      calls: "被调用者",
      none: "无。",
      showMore: (n: number) => `显示更多（还有 ${n.toLocaleString("en-US")} 条）`,
      hiddenByFile: "未显示的，按文件：",
      crossRegions: (cross: number, total: number) => `${cross}/${total} 跨区域`,
      crossRegionsTitle:
        "其中大多数位于其他区域——逐行标注跨界点会标记几乎每一行，故省略",
      crossSeamDotTitle: "位于另一个区域——这条边跨越了架构边界",
      entryPoint: "入口点",
      testFlag: "测试",
      exported: "已导出",
      regionLink: (name: string) => `区域：${name} →`,
      openInEditor: "在编辑器中打开 ↗",
      showCode: "显示代码",
      hideCode: "隐藏代码",
      recentChanges: "此文件的近期变更",
      noGitHistory: "这个项目没有可读的 git 历史。",
      untouchedYear: "最近一年未改动。",
      commitsLastYear: (n: number) => `最近一年提交 ${n.toLocaleString("en-US")} 次`,
      mostlyBy: (author: string, share: number, authors: number) =>
        ` · 主要来自 ${author}（占 ${authors} 位作者提交的 ${(share * 100).toFixed(0)}%）`,
      dataFlows: "数据流",
      receivesFrom: "接收自",
      feedsInto: "输出到",
      noDataFlows: "没有 DATA_FLOWS 边触及这个符号。",
      testedBy: "覆盖它的测试",
      noTests: "没有 TESTS 边到达这个符号。",
      changesTogether: "共同变更",
      noCoChange: "没有共同变更历史。",
      nearClones: "近似克隆",
      noNearClones: "没有近似克隆。",
    },
    flows: {
      loading: "正在追踪调用链路…",
      traceHeading: "追踪 A → B",
      fromPlaceholder: "起点符号…",
      toPlaceholder: "终点符号…",
      modeControl: "控制流",
      modeData: "数据流",
      modeTitle: "沿控制流（CALLS）或数据流（DATA_FLOWS）追踪",
      traceButton: "追踪",
      traceFailed: "追踪失败",
      reachableIn: (hops: number, viaData: boolean) =>
        `经${viaData ? "数据流" : "调用"} ${hops} 跳可达`,
      notReachable: (maxDepth: number, viaData: boolean, explored?: number) =>
        `${maxDepth} 跳内不可达（${viaData ? "数据流" : "调用"}；已探索 ${explored?.toLocaleString("en-US") ?? ""} 个节点）`,
      guardTitleHop: "这一跳调用点上的语法守卫",
      guardTitleStep: "这一步调用点上的语法守卫",
      listHeading: "流程——入口 → 终点",
      summary: (flows: number, callables: number, dropped: number) =>
        `${callables.toLocaleString("en-US")} 个可调用符号中走出 ${flows} 条调用链路${
          dropped > 0 ? ` · ${dropped.toLocaleString("en-US")} 个候选未遍历` : ""
        }`,
      acrossRegions: "跨区域",
      withinOneRegion: "单一区域内",
      depthCapTitle: "遍历止步于深度上限，而非汇点",
      collapse: "− 收起",
      showAll: (n: number, entry: string) =>
        `× 来自 ${entry} 的 ${n} 条调用链路——全部显示`,
      noFlows: "未检测到流程——项目可能没有明确的入口点。",
      pickJourney: "在左侧选择一条调用链路",
      copied: "已复制 ✓",
      copyMermaid: "复制为 mermaid",
      stepsCount: (n: number) => `${n} 步`,
      endsAtSink: "终止于汇点",
      stoppedAtCap: "止步于深度上限",
      crossesRegions: "跨区域",
      branchesBeyondCap: (n: number) => `超出上限的 ${n} 个分支未显示`,
      resolverConfidence: (pct: number) =>
        `解析置信度 ${pct}%——这一跳可能解析有误`,
    },
    overview: {
      loading: "正在读取地图…",
      handout: "讲义 ↗",
      handoutTitle:
        "一份自包含、可分享的代码库讲解文档——由图谱生成，适合打印",
      needsAttention: "需要关注",
      filesCount: (n: number) => `${n} 个文件`,
      churnUnavailable: "没有 git 历史，无法计算变更频率",
      concentrateRisk: "集中了变更频率 × 复杂度的风险",
      direction: "趋势",
      firstIndex: "首次索引",
      sincePrevIndex: "相对上一次索引",
      trendsAfterReindex: "下次重建索引后显示趋势",
      riskiestArea: "风险最高处",
      riskiestSub: (commits: string | number, file: string) =>
        `今年 ${commits} 次提交 × 高复杂度——${file}`,
      noComplexChurn: "没有高复杂度且频繁变更的文件",
      trust: "可信度",
      filesMissed: (n: number) => `漏掉 ${n} 个文件`,
      trustSub: (date: string, unmapped: number) =>
        `索引于 ${date} · ${unmapped} 个未映射符号`,
      findings: "发现",
      dismissed: (n: number) => `已忽略 ${n} 条`,
      couplingAnd: " 与 ",
      couplingSuffix: " 异常耦合",
      inspect: "查看",
      intended: "符合预期",
      intendedTitle: "标记为符合预期并忽略——若耦合增长一个数量级会重新提醒",
      questionsHeading: "这张图能回答的问题——拿去问你的智能体",
      regionsHeading: (method: string) => `区域——事实上的模块（${method}）`,
      hideTestCode: "隐藏测试代码",
      testRegionsHidden: (n: number) => `已隐藏 ${n} 个测试区域——显示`,
      regionMeta: (members: number, cohesion: number) =>
        `${members.toLocaleString("en-US")} 个符号 · cohesion ${cohesion.toFixed(2)}`,
      cohesionAria: (own: number, count: number, median: number) =>
        `cohesion ${own.toFixed(2)}，共 ${count} 个区域，中位数 ${median.toFixed(2)}`,
      allRegions: (n: number) => `全部 ${n} 个区域，见模块页 →`,
      bridgesHeading: "跨界符号——伸入多个区域的代码",
      bridgeMeta: (regions: number, calls: number) =>
        `${regions} 个区域 · ${calls.toLocaleString("en-US")} 次跨区调用`,
      bridgesFootnote:
        "这里的改动会波及多个区域——当任务横跨多个区域时，请提到这些名字。",
      referenceSummary: "参考——枢纽、入口点、边界",
      hubsHeading: "枢纽——最高 fan-in",
      noHotspots: "暂无热点数据。",
      entryPoints: "入口点",
      followFlows: "在流程页跟踪它们 →",
      boundariesHeading: "边界——跨包调用",
      noBoundaries: "未记录跨包调用。",
      inventory: (symbols: string, edges: string, regions: string) =>
        `${symbols} 个符号 · ${edges} 条边 · ${regions} 个区域`,
    },
    dashboard: {
      takeaway: (
        simplePct: string,
        simpleMax: number,
        tail: number,
        tailMin: number,
        led: string[],
      ) =>
        `${simplePct}% 的函数是简单的（≤${simpleMax}）；${tail.toLocaleString("en-US")} 个超过 ${tailMin}${led.length > 0 ? `——以 ${led.join("、")} 为首` : ""}。`,
      costSentence: (files: number, filesShare: string, commitShare: string) =>
        `${files} 个文件——占代码库的 ${filesShare}%，占今年全部提交的 ${commitShare}%`,
      costConcentrate: "——缺陷就集中在这里的变更中。",
    },
    why: {
      whenRuns: "它何时运行？",
      whatTriggers: "它会触发什么？",
      toWhatTriggers: "→ 它触发什么",
      toWhenRuns: "← 它何时运行",
      dispatchChip: (n: number) => `◇ ${n} 个候选目标之一`,
      dispatchTitle:
        "动态分发：解析器在这个调用点看到多个可能的目标——这是其中之一",
      guardsUnavailable: "守卫不可用（源码不可读）",
      unguarded: "无守卫——这条路径上必然执行",
      loopChip: "⟳ 循环",
      loopChipTitle: "调用点位于循环内部",
      guardChipTitle: (kind: string) =>
        `调用点周围的语法 ${kind} 守卫——不是经过证明的路径条件`,
      alwaysRunsPrefix: "只要 ",
      alwaysRunsSuffix: " 运行，它就一定运行——调用点上没有任何条件。",
      alwaysTriggersPrefix: "总是触发 ",
      alwaysTriggersSuffix: "——无条件。",
      alwaysJoiner: " 与 ",
      honestyFooter:
        "守卫只是各调用点周围的语法条件——不是经过证明的路径条件。分发、事件与反射在这里不可见。",
      /* 守卫片段——条件表达式保留代码原文。 */
      guardWhen: (cond: string) => `当 ${cond} 时`,
      guardUnless: (cond: string) => `除非 ${cond}`,
      guardConditionally: "视条件而定",
      guardElseArm: "在 else 分支中",
      guardCase: (cond: string) => `匹配 ${cond} 分支`,
      guardSwitchCase: "在 switch 分支中",
      guardSwitchOn: (cond: string) => `按 ${cond} 分支`,
      guardSwitch: "在 switch 中",
      guardLoopWhile: (cond: string) => `当 ${cond} 时循环`,
      guardLoop: "在循环中",
      guardCatch: "在错误处理中",
      chainJoiner: " → ",
    },
    firstread: {
      edgesCross: (weight: number) =>
        `${weight.toLocaleString("en-US")} 条边跨越这条边界`,
      linksAreas: (a: string, b: string) =>
        `连接 ${a}/ 与 ${b}/——分属不同的顶层目录`,
      holdsLoosely: (name: string, cohesion: number) =>
        `${name} 内聚松散（cohesion ${cohesion.toFixed(2)}）`,
      reasonSeparator: "；",
      questionWhyDepend: (a: string, b: string) => `为什么 ${a} 依赖 ${b}？`,
      questionSplit: (name: string) => `${name} 应该拆分成更小的模块吗？`,
      whySplit: (pct: string, members: number) =>
        `只有 ${pct}% 的边留在区域内部（${members.toLocaleString("en-US")} 个符号）`,
      questionDeadSafe: (n: number) =>
        `这 ${n.toLocaleString("en-US")} 个没有调用者的函数可以安全删除吗？`,
      whyDeadSafe: "没有任何 CALLS 或 USAGE 到达它们——已排除入口点与测试",
      questionUnresolved: "图谱未能解析哪些调用点？为什么？",
      whyUnresolved: (pct: string) =>
        `${pct}% 的调用类边是 USAGE（没有可证明的唯一目标）`,
    },
    regions: {
      whyCallCommunity: (files: number, pct: number, dir: string) =>
        `调用社区：${files} 个文件，${pct}% 位于 ${dir} 之下`,
      whyFolderGroup: "目录分组（未被任何保留的调用社区解释）",
      whyMisc: "游离于所有保留社区与目录分组之外的文件",
    },
    control: {
      panel: "控制面板",
      totalCpu: "总 CPU",
      totalRam: "总内存",
      processes: "进程",
      selfRam: "自身内存",
      activeProcesses: "活动进程",
      processLogs: "进程日志",
      noProcesses: "未找到进程",
      noLogs: "暂无日志",
      thisProcess: "本进程",
      uptime: "运行时间",
    },
  },
} as const;

export type UiMessages = (typeof messages)[UiLanguage];

export function detectLanguage(acceptLanguage?: string | null, override?: string | null): UiLanguage {
  if (override === "zh" || override === "en") return override;
  if (!acceptLanguage) return "en";

  // Ranked by q, not by whether "zh" appears anywhere. A substring test served
  // Chinese for "en-US,en;q=0.9,zh;q=0.5", where English is clearly preferred,
  // and for "zh;q=0, en", where q=0 means Chinese is unacceptable.
  const best = acceptLanguage
    .split(",")
    .map((part) => {
      const [tag, ...params] = part.trim().split(";");
      const q = params.map((p) => /^\s*q\s*=\s*([\d.]+)\s*$/i.exec(p)).find(Boolean);
      return { tag: tag.trim().toLowerCase(), q: q ? Number(q[1]) : 1 };
    })
    .filter(({ tag, q }) => tag && Number.isFinite(q) && q > 0)
    .sort((a, b) => b.q - a.q)
    .find(({ tag }) => tag.split("-")[0] === "zh" || tag.split("-")[0] === "en");

  return best?.tag.startsWith("zh") ? "zh" : "en";
}

let cachedLanguage: UiLanguage = "en";
let languageLoaded = false;
let languageRequest: Promise<UiLanguage> | null = null;
const languageListeners = new Set<(lang: UiLanguage) => void>();

function loadUiLanguage(): Promise<UiLanguage> {
  if (languageLoaded) return Promise.resolve(cachedLanguage);
  if (languageRequest) return languageRequest;

  languageRequest = fetch("/api/ui-config")
    .then((r) => r.json())
    .then((data) => detectLanguage(null, data?.lang))
    .catch(() => detectLanguage(navigator.language))
    .then((lang) => {
      cachedLanguage = lang;
      languageLoaded = true;
      for (const listener of languageListeners) listener(lang);
      return lang;
    })
    .finally(() => {
      languageRequest = null;
    });

  return languageRequest;
}

/* The active language itself — for content that localizes outside the
 * message catalog (wiki entries, question families carry co-located zh
 * fields with per-field English fallback). */
export function useUiLanguage(): UiLanguage {
  const [lang, setLang] = useState<UiLanguage>(cachedLanguage);

  useEffect(() => {
    let cancelled = false;
    languageListeners.add(setLang);
    void loadUiLanguage().then((nextLang) => {
      if (!cancelled) setLang(nextLang);
    });
    return () => {
      cancelled = true;
      languageListeners.delete(setLang);
    };
  }, []);

  return lang;
}

export function useUiMessages(): UiMessages {
  return messages[useUiLanguage()];
}
