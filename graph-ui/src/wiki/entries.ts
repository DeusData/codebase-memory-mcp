/* The metric wiki — every number Atlas shows, explained in one place: what
 * it means (one tooltip-sized sentence), why it earns its spot, how it is
 * computed, the named caps, and what it deliberately does not cover. The
 * "refused" tier documents metrics Atlas will NOT show, and why. This file
 * is the single source of truth; panels and chips render from it. Chinese
 * content lives co-located per entry (zh) so translations cannot drift. */
import type { UiLanguage } from "../lib/i18n";

export type WikiTier = "first-class" | "caveated" | "refused";

export interface WikiEntry {
  id: string;
  slug: string;
  term: string;
  tier: WikiTier;
  /* ≤30 words — this IS the tooltip text. */
  sentence: string;
  why?: string;
  computedParts?: string[];
  /* Named caps, each "capped: … because …" style — rendered as warnings. */
  caps?: string[];
  notCovered?: string;
  appearsIn: string[];
  pairedWith?: string;
  seeAlso?: string[];
  /* Simplified-Chinese content; absent fields fall back to English. The
   * term itself is never translated — the zh panel keeps teaching the
   * English-canonical token and glosses it in the header. */
  zh?: {
    /* Short gloss shown beside the term in the zh panel header. */
    gloss?: string;
    /* ≤45 characters — zh is denser, same tooltip budget. */
    sentence?: string;
    why?: string;
    caps?: string[];
    notCovered?: string;
    appearsIn?: string[];
  };
}

export const WIKI_ENTRIES: WikiEntry[] = [
  {
    id: "M-CHURN",
    slug: "churn",
    term: "churn",
    tier: "first-class",
    sentence:
      "How often this file changed in the last year — the strongest single predictor of where work concentrates.",
    why: "Where code changed last year is where it will change next — and every change is a chance to break something. Process history predicts defects better than any measure of the code itself.",
    caps: ["vendored and generated paths are excluded"],
    notCovered:
      "Raw commit counts, not size-adjusted; relative churn (per size) is the variant with peer-reviewed defect-prediction backing and is planned as the default axis.",
    appearsIn: ["Dashboard", "Symbol file history", "Handout"],
    pairedWith: "complexity",
    zh: {
      gloss: "变更频率",
      sentence:
        "这个文件过去一年改动的频率——预测工作将集中在哪里的最强单一信号。",
      why: "去年改动过的地方就是接下来还会改的地方——而每次改动都是一次弄坏东西的机会。过程历史对缺陷的预测力，胜过任何针对代码本身的度量。",
      caps: ["vendored 与生成的路径已被排除"],
      notCovered:
        "只是原始提交次数，未按规模调整；相对变更频率（按规模）才是有同行评审缺陷预测证据的变体，计划作为默认轴。",
      appearsIn: ["仪表盘", "符号页文件历史", "Handout 讲义"],
    },
  },
  {
    id: "M-COCHANGE",
    slug: "co-change",
    term: "co-change",
    tier: "first-class",
    sentence:
      "Files that tend to change in the same commits as this one — coupling as history actually happened, not as imports suggest.",
    why: "If two files always change together, they are one module the directory tree forgot. Editing one without its partner is a classic source of half-done changes.",
    notCovered:
      "Pairs below the minimum co-commit support are not shown; the observation window is stated on the panel.",
    appearsIn: ["Symbol"],
    pairedWith: "coupling",
    zh: {
      gloss: "共同变更",
      sentence:
        "总和它出现在同一批提交里的文件——真实发生过的耦合，而非 import 暗示的。",
      why: "如果两个文件总是一起改，它们就是目录树忘了承认的同一个模块。只改其一、落下搭档，是改一半改不完的经典来源。",
      notCovered: "低于最小共同提交支持度的文件对不显示；观测窗口在面板上注明。",
      appearsIn: ["符号页"],
    },
  },
  {
    id: "M-FANIN",
    slug: "fan",
    term: "fan",
    tier: "first-class",
    sentence:
      "How many symbols call this one (fan-in), and how many it calls (fan-out) — the raw material of blast radius.",
    why: "High fan-in means many places notice when this breaks; high fan-out means this breaks when many places change. Both ends deserve tests and careful review.",
    notCovered:
      "Edges carry resolution confidence; unresolved dynamic calls are not counted.",
    appearsIn: ["Symbol", "Galaxy"],
    seeAlso: ["confidence", "blast-radius"],
    zh: {
      gloss: "扇入/扇出",
      sentence:
        "多少符号调用它（扇入），它又调用多少（扇出）——影响范围的原材料。",
      why: "扇入高，意味着它一坏很多地方都会察觉；扇出高，意味着很多地方一变它就会坏。两端都值得测试与仔细评审。",
      notCovered: "边带有解析置信度；未解析的动态调用不计入。",
      appearsIn: ["符号页", "星系"],
    },
  },
  {
    id: "M-HOTSPOT",
    slug: "hotspot",
    term: "hotspot",
    tier: "first-class",
    sentence:
      "Files that change often and are hard to follow — where fixes cluster and edits cost the most.",
    why: "A small set of hotspots typically carries a large share of defect-fixing work. Relative churn predicts defect density; raw line counts don't. Effort spent here beats effort spread evenly.",
    computedParts: [
      "churn percentile of this repo",
      "complexity percentile of this repo",
    ],
    caps: [
      "capped: files under 100 lines are excluded — too small to rank",
      "capped: test files are excluded",
    ],
    notCovered:
      "Bug density is not measured directly — there is no issue-tracker join yet. Generated or vendored files can inflate churn. A stable-but-tangled file with no recent commits won't appear here; see complexity alone for those.",
    appearsIn: ["Dashboard hero", "Changes risk", "Handout"],
    pairedWith: "tested",
    seeAlso: ["churn", "complexity"],
    zh: {
      gloss: "热点",
      sentence: "改动频繁又难以读懂的文件——修复在这里扎堆，改动在这里最贵。",
      why: "一小撮热点通常承担着缺陷修复工作的一大部分。相对变更频率能预测缺陷密度，原始行数不能。把精力花在这里，胜过平均摊开。",
      caps: [
        "上限：不足 100 行的文件被排除——太小无法排名",
        "上限：测试文件被排除",
      ],
      notCovered:
        "缺陷密度没有被直接测量——尚无 issue 跟踪器关联。生成或 vendored 的文件会推高变更频率。稳定但纠缠、近期没有提交的文件不会出现在这里；那类文件请单看复杂度。",
      appearsIn: ["仪表盘英雄区", "变更页风险", "Handout 讲义"],
    },
  },
  {
    id: "M-CPLX",
    slug: "complexity",
    term: "complexity",
    tier: "caveated",
    sentence:
      "The number of independent paths through a function — a decent proxy for hard-to-test, a weak one for hard-to-read.",
    why: "Every independent path is a case a test must cover and a reader must simulate. High-percentile functions in this repository are where reviews slow down and tests multiply.",
    notCovered:
      "Cyclomatic complexity correlates heavily with size; the percentile shown is of this repository, never an absolute good/bad threshold.",
    appearsIn: ["Dashboard", "Symbol"],
    pairedWith: "churn",
    seeAlso: ["cognitive-complexity"],
    zh: {
      gloss: "圈复杂度",
      sentence:
        "函数内独立路径的数量——衡量“难测试”尚可，衡量“难读懂”偏弱。",
      why: "每条独立路径都是测试必须覆盖、读者必须在脑中推演的一种情况。本仓库里高百分位的函数，正是评审变慢、测试成倍增多的地方。",
      notCovered:
        "圈复杂度与规模高度相关；显示的百分位只相对本仓库，从来不是绝对的好坏阈值。",
      appearsIn: ["仪表盘", "符号页"],
    },
  },
  {
    id: "M-COG",
    slug: "cognitive-complexity",
    term: "cognitive complexity",
    tier: "caveated",
    sentence:
      "How much nesting and control-flow interruption a reader must hold in their head to follow this function.",
    why: "Deep nesting forces a reader to keep every enclosing condition in working memory. This measure was designed to match how hard code feels to read, and tracks that better than path counting.",
    notCovered:
      "Better matched to perceived readability than cyclomatic, but still size-confounded.",
    appearsIn: ["Dashboard", "Symbol"],
    seeAlso: ["complexity"],
    zh: {
      gloss: "认知复杂度",
      sentence:
        "读者跟随这个函数时，必须在脑中同时保持多少嵌套与控制流中断。",
      why: "深层嵌套迫使读者把每一层外围条件都留在工作记忆里。这个度量为贴合“代码读起来有多费劲”而设计，比数路径更接近这种感受。",
      notCovered: "比圈复杂度更贴近主观可读性，但仍受规模干扰。",
      appearsIn: ["仪表盘", "符号页"],
    },
  },
  {
    id: "M-COH",
    slug: "cohesion",
    term: "cohesion",
    tier: "caveated",
    sentence:
      "How much this region's files actually work together, measured by internal connections.",
    why: "A region whose files barely talk to each other is a folder, not a module — changes ripple outward instead of staying local. Read it beside coupling: shredding a module improves one at the cost of the other.",
    notCovered:
      "Structural cohesion measures failed empirical validation as quality predictors; a history-based co-change cohesion is the planned replacement. Always read beside coupling.",
    appearsIn: ["Overview regions", "Modules"],
    pairedWith: "coupling",
    zh: {
      gloss: "内聚",
      sentence: "这个区域的文件在多大程度上真正协作，按内部连接计算。",
      why: "文件之间几乎不来往的区域只是一个文件夹，不是一个模块——改动会向外扩散而不是留在本地。要和耦合放在一起读：把模块打散，改善其中一个的代价是恶化另一个。",
      notCovered:
        "结构性内聚度量作为质量预测指标未通过实证检验；基于历史共同变更的内聚是计划中的替代。永远与耦合并排读。",
      appearsIn: ["总览区域", "模块"],
    },
  },
  {
    id: "M-COUP",
    slug: "coupling",
    term: "coupling",
    tier: "first-class",
    sentence:
      "How strongly two regions depend on each other, counted in cross-region calls.",
    why: "The more two regions call each other, the harder they are to change, test, or understand apart. Heavy coupling between supposedly separate modules is the map's most actionable warning.",
    notCovered: "Only statically resolved calls are counted; confidence applies.",
    appearsIn: ["Overview", "Modules"],
    pairedWith: "cohesion",
    zh: {
      gloss: "耦合",
      sentence: "两个区域相互依赖的强度，按跨区域调用数计算。",
      why: "两个区域互相调用越多，就越难把它们分开修改、测试与理解。名义上相互独立的模块之间的重耦合，是这张地图上最可付诸行动的警告。",
      notCovered: "只统计静态解析成功的调用；置信度同样适用。",
      appearsIn: ["总览", "模块"],
    },
  },
  {
    id: "M-BRIDGE",
    slug: "boundary-spanner",
    term: "boundary spanner",
    tier: "first-class",
    sentence:
      "A symbol that calls into several foreign regions — the joints of the architecture, and the first places a refactor hurts.",
    why: "These symbols hold the architecture together — and any boundary refactor lands on them first. They are where an interface change fans out into foreign regions.",
    caps: [
      "capped: outgoing calls only",
      "capped: at least 2 foreign regions required",
      "capped: test files are excluded",
    ],
    notCovered:
      "Incoming cross-region popularity is a different signal (see fan-in).",
    appearsIn: ["Overview"],
    seeAlso: ["fan", "coupling"],
    zh: {
      gloss: "跨界符号",
      sentence: "调用多个外部区域的符号——架构的关节，重构最先疼的地方。",
      why: "这些符号把架构缝在一起——任何边界重构都会最先落在它们身上。接口一变，正是从这里扩散进外部区域。",
      caps: [
        "上限：只统计出向调用",
        "上限：至少要跨 2 个外部区域",
        "上限：测试文件被排除",
      ],
      notCovered: "入向的跨区域受欢迎度是另一个信号（见 fan-in）。",
      appearsIn: ["总览"],
    },
  },
  {
    id: "M-CONF",
    slug: "confidence",
    term: "confidence",
    tier: "first-class",
    sentence:
      "How likely this call edge is real, given how the resolver found it.",
    why: "Low-confidence hops can turn a trace into a plausible story that isn't true. The word and the number always appear together: certain ≥95, likely 75–94, uncertain 50–74, speculative <50.",
    caps: [
      "capped: cross-language boundary → at most likely",
      "reduced: target overload set larger than 1",
      "speculative: dynamic dispatch unresolved",
    ],
    notCovered:
      "Reflection, code generation, and runtime dispatch are not modelled. Coverage is best-effort.",
    appearsIn: ["Flows", "Trace", "Symbol connections"],
    zh: {
      gloss: "置信度",
      sentence: "按解析器找到这条调用边的方式，估计它有多可能真实存在。",
      why: "低置信度的跳步能把一条追踪变成貌似可信却不真实的故事。词和数字永远一起出现：certain ≥95、likely 75–94、uncertain 50–74、speculative <50。",
      caps: [
        "上限：跨语言边界 → 至多 likely",
        "降级：目标重载集大于 1",
        "speculative：动态分发未解析",
      ],
      notCovered: "反射、代码生成与运行时分发未被建模。覆盖为尽力而为。",
      appearsIn: ["流程", "追踪", "符号页连接"],
    },
  },
  {
    id: "M-OBSERVED",
    slug: "observed",
    term: "observed",
    tier: "first-class",
    sentence:
      "A call that actually fired in a recorded run — observed, not merely possible from static analysis.",
    why: "Static edges say what could happen; observation says what did. An observed marker on a speculative edge is runtime evidence the resolver got it right.",
    computedParts: [
      "ingest_traces call paths, stored by qualified name per run label",
    ],
    caps: [
      "capped: 200k observed pairs per project — whole runs age out together, oldest first",
    ],
    notCovered:
      "Absence of observation never means dead or unused — a run covers only what it exercised. Counts survive reindexing (name-keyed), but renamed symbols start fresh.",
    appearsIn: ["Flows", "Trace"],
    seeAlso: ["confidence", "dead-candidate"],
    zh: {
      gloss: "已观测",
      sentence:
        "在录制的运行中真实触发过的调用——已观测，而不只是静态分析认为可能。",
      why: "静态边说的是可能发生什么；观测说的是确实发生过什么。speculative 边上的已观测标记，是解析器判断正确的运行时证据。",
      caps: [
        "上限：每个项目 20 万条已观测调用对——整次运行一起老化，最旧的先淘汰",
      ],
      notCovered:
        "缺少观测绝不代表死代码或未使用——一次运行只覆盖它实际执行到的部分。计数按限定名保存、重建索引后仍在，但改名的符号从零开始。",
      appearsIn: ["流程", "追踪"],
    },
  },
  {
    id: "M-DEAD",
    slug: "dead-candidate",
    term: "dead candidate",
    tier: "caveated",
    sentence:
      "Symbols with no recorded callers — candidates for removal, with caveats you must check first.",
    why: "The blocker to deleting dead code is fear of unintended breakage, not detection. This list is evidence, not a verdict — verify with a repo-wide search and tests before deleting.",
    notCovered:
      "Dynamic dispatch, reflection, exported API consumed outside this repository, and config-referenced entry points all look dead here and are not.",
    appearsIn: ["Dashboard"],
    seeAlso: ["fan", "confidence"],
    zh: {
      gloss: "疑似死代码",
      sentence:
        "没有任何已记录调用者的符号——可考虑删除，但必须先核对注意事项。",
      why: "删除死代码的阻碍是怕误伤，不是找不到。这份列表是证据，不是判决——删除前先做全仓库搜索并跑测试确认。",
      notCovered:
        "动态分发、反射、被本仓库之外消费的导出 API、以及被配置引用的入口点，在这里都长得像死代码，但都不是。",
      appearsIn: ["仪表盘"],
    },
  },
  {
    id: "M-TESTED",
    slug: "tested",
    term: "tested",
    tier: "caveated",
    sentence:
      "A TESTS edge reaches this symbol — some test exercises it, directly or through calls.",
    why: "A symbol no test reaches fails only in production. Reach is the floor of safety; hotspots without reach are the riskiest code in the repository.",
    notCovered:
      "Reached is not asserted: a test can execute this code without checking its result. Atlas deliberately shows reach, not a coverage percentage.",
    appearsIn: ["Dashboard", "Symbol"],
    pairedWith: "hotspot",
    seeAlso: ["coverage-gates"],
    zh: {
      gloss: "测试触达",
      sentence: "有 TESTS 边到达这个符号——某个测试直接或经调用链运行到它。",
      why: "没有测试到达的符号只会在生产环境里失败。触达是安全的下限；没有触达的热点是仓库里风险最高的代码。",
      notCovered:
        "触达不等于断言：测试可以执行这段代码却不检查它的结果。Atlas 有意展示触达，而不是覆盖率百分比。",
      appearsIn: ["仪表盘", "符号页"],
    },
  },
  {
    id: "M-DOC",
    slug: "documented",
    term: "documented",
    tier: "caveated",
    sentence:
      "Exported symbols that carry a doc comment — a floor for explainability, not a measure of doc quality.",
    why: "An undocumented export forces every caller to read the implementation. A doc comment is a cheap floor that pays off at every call site — and in every agent prompt.",
    notCovered:
      "Comment presence only; accuracy and usefulness are not assessed.",
    appearsIn: ["Dashboard"],
    zh: {
      gloss: "有文档",
      sentence: "带文档注释的导出符号——可解释性的下限，不衡量文档质量。",
      why: "没有文档的导出迫使每个调用者去读实现。一条文档注释是廉价的下限，在每个调用点——以及每条智能体提示里——都有回报。",
      notCovered: "只看注释是否存在；准确性与有用性不做评估。",
      appearsIn: ["仪表盘"],
    },
  },
  {
    id: "M-DIVERG",
    slug: "divergence",
    term: "divergence",
    tier: "caveated",
    sentence:
      "Copies of this code that have started to drift apart — the risk is the drift, not the duplication.",
    why: "When copies drift, a bug fixed in one lives on in the other. Divergent near-clones are the copies that have already started disagreeing.",
    notCovered:
      "Duplication alone is a weak signal; identical, stable copies are mostly harmless. Divergence under change is the hazard this view ranks.",
    appearsIn: ["Symbol near-clones"],
    zh: {
      gloss: "副本漂移",
      sentence: "已经开始各自漂移的代码副本——风险在漂移，不在重复本身。",
      why: "副本一旦漂移，在一处修掉的 bug 会在另一处活下去。漂移中的近似克隆，就是已经开始互相矛盾的那些副本。",
      notCovered:
        "单纯的重复是弱信号；完全相同且稳定的副本大多无害。这个视图排的是变更之下的漂移这种危险。",
      appearsIn: ["符号页近似克隆"],
    },
  },
  {
    id: "M-HELP",
    slug: "who-can-help",
    term: "who can help",
    tier: "caveated",
    sentence:
      "People with recent, substantial history in this code — evidence for who to ask, never a performance measure.",
    why: "The most-sought information in twenty years of developer studies is what coworkers know. When the graph runs out of answers, the right person is the fastest path — this shows who, with the evidence.",
    notCovered:
      "Atlas never ranks people against each other and never shows per-person productivity. Names appear only with their evidence: commits, recency, breadth.",
    appearsIn: ["Symbol — Who can help", "Symbol file history"],
    zh: {
      gloss: "可求助的人",
      sentence:
        "近期在这段代码上有实质历史的人——该问谁的证据，绝不是绩效度量。",
      why: "二十年的开发者研究里，被寻求最多的信息是同事知道什么。当图谱给不出答案时，找对人是最快的路——这里展示是谁，并附上证据。",
      notCovered:
        "Atlas 从不把人相互排名，也从不展示个人生产力。名字只与证据一同出现：提交、新近度、广度。",
      appearsIn: ["符号页“可求助的人”", "符号页文件历史"],
    },
  },
  {
    id: "M-REGION",
    slug: "region",
    term: "region",
    tier: "first-class",
    sentence:
      "A cluster of files that call each other more than they call anything else — Atlas's inferred modules.",
    why: "Modules inferred from actual call structure show the architecture as it is, not as the directory tree claims. Where the two disagree is usually worth a look.",
    notCovered:
      "Inferred from call structure, not from directory names; a region panel must answer why is this here — members, internal density, top external ties.",
    appearsIn: ["Galaxy", "Overview", "Modules"],
    seeAlso: ["cohesion", "coupling"],
    zh: {
      gloss: "区域",
      sentence: "相互调用多于对外调用的一簇文件——Atlas 推断出的模块。",
      why: "从真实调用结构推断出的模块，展示的是架构的实际样子，而不是目录树宣称的样子。两者不一致的地方通常值得一看。",
      notCovered:
        "从调用结构推断，与目录名无关；区域面板必须回答“为什么在这里”——成员、内部密度、最强的外部联系。",
      appearsIn: ["星系", "总览", "模块"],
    },
  },
  {
    id: "M-HUB",
    slug: "hub",
    term: "hub",
    tier: "first-class",
    sentence:
      "The most-connected symbols in the graph — the ones most other code depends on, directly or transitively.",
    why: "A change to a hub is felt everywhere; an outage in one is an outage in many. Hubs deserve the strongest tests and the most careful review in the repository.",
    caps: [
      "capped: top 12 by connectivity are haloed; the cutoff is a display choice, not a cliff",
    ],
    notCovered: "Connectivity is static; a rarely-executed hub still ranks.",
    appearsIn: ["Galaxy halos", "Overview"],
    seeAlso: ["fan"],
    zh: {
      gloss: "枢纽",
      sentence: "图中连接最多的符号——其余代码直接或间接最依赖的那些。",
      why: "枢纽一变，处处有感；一个枢纽出事，就是多处一起出事。枢纽配得上仓库里最强的测试与最仔细的评审。",
      caps: ["上限：按连通度取前 12 个加光环；截断是显示上的选择，不是断崖"],
      notCovered: "连通度是静态的；很少被执行的枢纽同样上榜。",
      appearsIn: ["星系光环", "总览"],
    },
  },
  {
    id: "M-BLAST",
    slug: "blast-radius",
    term: "blast radius",
    tier: "first-class",
    sentence:
      "Everything that can reach the code you changed — what could notice, grouped by region.",
    why: "Most regressions ship because a change was tested where it was made, not where it is felt. The blast list is what to retest before pushing.",
    notCovered:
      "Static CALLS edges only — dynamic dispatch, reflection and code generation are invisible here, so every count is a floor. The walk is capped in depth and volume; caps are always stated.",
    appearsIn: ["Changes", "Symbol — If you change this"],
    seeAlso: ["fan", "confidence"],
    zh: {
      gloss: "影响范围",
      sentence: "所有能到达你所改代码的东西——哪些地方可能察觉，按区域分组。",
      why: "多数回归之所以上线，是因为改动只在发生它的地方被测试，而不是在感受到它的地方。这份影响列表就是推送前要重测的内容。",
      notCovered:
        "只有静态 CALLS 边——动态分发、反射与代码生成在这里不可见，因此每个数字都是下限。遍历有深度与数量上限；上限永远注明。",
      appearsIn: ["变更", "符号页——如果你改动这里"],
    },
  },
  {
    id: "M-SCENT",
    slug: "scent",
    term: "scent",
    tier: "first-class",
    sentence:
      "Where your search term concentrates across the map — follow the smell, not the guess.",
    why: "Search results drawn on the map show where a concept lives and how concentrated it is — one bright region means a clean home; scattered hits mean a crosscutting concern.",
    notCovered:
      "Name and text matches only; File and Folder nodes are excluded from counts.",
    appearsIn: ["Galaxy minimap", "Search"],
    zh: {
      gloss: "气味",
      sentence: "搜索词在地图上的聚集位置——跟着气味走，别靠猜。",
      why: "画在地图上的搜索结果显示一个概念住在哪里、有多集中——一片亮区意味着干净的归属；散落的命中意味着横切关注点。",
      notCovered: "只匹配名称与文本；File 与 Folder 节点不计入计数。",
      appearsIn: ["星系小地图", "搜索"],
    },
  },
  {
    id: "M-NEG-MI",
    slug: "maintainability-index",
    term: "maintainability index",
    tier: "refused",
    sentence:
      "Atlas does not show a Maintainability Index: its weights are folklore, it double-counts size, and it failed empirical validation.",
    why: "The MI's constants were fit to a handful of 1990s Hewlett-Packard systems and never revalidated; size dominates every term. What Atlas shows instead: churn, complexity and size separately, each with its own percentile.",
    appearsIn: ["Wiki only"],
    zh: {
      gloss: "可维护性指数",
      sentence:
        "Atlas 不展示可维护性指数：权重是民间传说，重复计算规模，且未通过实证检验。",
      why: "MI 的常数拟合自 1990 年代惠普的少数几个系统，此后从未重新验证；规模主导每一项。Atlas 转而分别展示变更频率、复杂度与规模，各带自己的百分位。",
      appearsIn: ["仅在 Wiki"],
    },
  },
  {
    id: "M-NEG-HAL",
    slug: "halstead",
    term: "Halstead",
    tier: "refused",
    sentence:
      "Atlas does not compute Halstead metrics: token-counting science from 1977 that never replicated; size does its job better.",
    why: "Operator-and-operand counting promised effort and bug prediction it never delivered under replication; every useful signal it carries, plain size carries better.",
    appearsIn: ["Wiki only"],
    zh: {
      gloss: "Halstead 度量",
      sentence:
        "Atlas 不计算 Halstead：1977 年的记号计数从未复现；规模干得更好。",
      why: "算符与操作数计数许诺的工作量与缺陷预测，在复现研究中从未兑现；它携带的每一个有用信号，朴素的规模都携带得更好。",
      appearsIn: ["仅在 Wiki"],
    },
  },
  {
    id: "M-NEG-COV",
    slug: "coverage-gates",
    term: "coverage gates",
    tier: "refused",
    sentence:
      "Atlas shows which tests reach a symbol, never a coverage-percentage target: rigid targets produce assertion-free tests.",
    why: "The correlation between coverage and post-release defects is weak and disappears above roughly 70–80%. Atlas shows test reach per symbol so you can judge the gap that matters.",
    appearsIn: ["Wiki only"],
    seeAlso: ["tested"],
    zh: {
      gloss: "覆盖率门槛",
      sentence:
        "Atlas 展示哪些测试到达符号，从不设覆盖率百分比目标：僵硬的指标催生无断言的测试。",
      why: "覆盖率与发布后缺陷的相关性很弱，超过大约 70–80% 后就消失了。Atlas 按符号展示测试触达，让你自己判断真正要紧的缺口。",
      appearsIn: ["仅在 Wiki"],
    },
  },
  {
    id: "M-NEG-GRADE",
    slug: "quality-grades",
    term: "quality grades",
    tier: "refused",
    sentence:
      "No A–F, no 0–100: a percentile in this repository carries the same information with no invented scale to misread.",
    why: "Published score curves still get read as linear; grades measurably undermine interest and performance relative to explanations; single scores with consequences invite gaming. Atlas ships percentiles, distributions and named caps instead.",
    appearsIn: ["Wiki only"],
    zh: {
      gloss: "质量评级",
      sentence:
        "不给 A–F 也不给 0–100：仓库内百分位承载同样信息，且没有会被误读的人造刻度。",
      why: "公开的评分曲线仍会被当成线性来读；相比解释，评级会切实削弱兴趣与表现；带后果的单一分数招致刷分。Atlas 提供的是百分位、分布与点名的上限。",
      appearsIn: ["仅在 Wiki"],
    },
  },
];

const bySlug = new Map(WIKI_ENTRIES.map((entry) => [entry.slug, entry]));
const byId = new Map(WIKI_ENTRIES.map((entry) => [entry.id, entry]));

export function wikiEntry(slug: string): WikiEntry | undefined {
  return bySlug.get(slug);
}

export function wikiEntryById(id: string): WikiEntry | undefined {
  return byId.get(id);
}

/* One entry as one locale sees it: zh fields when the locale is zh and the
 * field exists, English fallback field-by-field. The term itself stays
 * English-canonical — chips, headings and breadcrumbs keep teaching the
 * token readers meet in code, docs and issues; the gloss rides beside it. */
export interface WikiEntryView extends Omit<WikiEntry, "zh"> {
  gloss?: string;
}

export function localizeEntry(entry: WikiEntry, lang: UiLanguage): WikiEntryView {
  const { zh, ...en } = entry;
  if (lang !== "zh" || !zh) return en;
  return {
    ...en,
    gloss: zh.gloss,
    sentence: zh.sentence ?? en.sentence,
    why: zh.why ?? en.why,
    caps: zh.caps ?? en.caps,
    notCovered: zh.notCovered ?? en.notCovered,
    appearsIn: zh.appearsIn ?? en.appearsIn,
  };
}
