export const meta = {
  name: 'cbm-vs-graphify-qa',
  description: 'Layer 2: blind D1-D5 Q&A effectiveness — cbm vs graphify-AST vs Explorer(grep), judged against oracle ground truth',
  phases: [
    { title: 'Answer', detail: '3 tool-restricted conditions answer each question' },
    { title: 'Judge', detail: 'blind 2-pass scoring vs ground truth' },
  ],
}

const EVAL = '/private/tmp/claude-502/-Users-yf-jin-Desktop-03-Projects-codebase-memory-mcp-pro/fd757ee7-44e7-439c-ba81-f9b8dc43c4b4/scratchpad/eval'
const cfg = {
  corpus: `${EVAL}/eval-corpus`,
  evalDir: EVAL,
  proj: 'private-tmp-claude-502-Users-yf-jin-Desktop-03-Projects-codebase-memory-mcp-pro-fd757ee7-44e7-439c-ba81-f9b8dc43c4b4-scratchpad-eval-eval-corpus',
  cbmCache: `${EVAL}/cbm-cache-smoke`,
  cbmBin: '/Users/yf.jin/.local/bin/codebase-memory-mcp',
  gfyBin: '/Users/yf.jin/.local/bin/graphify',
  graph: `${EVAL}/eval-corpus/graphify-out/graph.json`,
  gfyOut: `${EVAL}/eval-corpus/graphify-out`,
}
const Qs = [
  { id: 'D1.1', dim: 'D1', question: 'List the names of all functions DEFINED in the file src/simhash/minhash.c. Return just the function names.' },
  { id: 'D1.2', dim: 'D1', question: 'List the names of all functions DEFINED in the file internal/cbm/ac.c. Return just the function names.' },
  { id: 'D1.3', dim: 'D1', question: 'List the names of all functions DEFINED in the file src/foundation/str_util.c. Return just the function names.' },
  { id: 'D1.4', dim: 'D1', question: 'List the names of all functions DEFINED in the file src/git/git_context.c. Return just the function names.' },
  { id: 'D1.5', dim: 'D1', question: 'List the names of all functions DEFINED in the file src/discover/discover.c. Return just the function names.' },
  { id: 'D2.1', dim: 'D2', question: 'Which functions/files call the function `cbm_arena_alloc`? List the caller locations (files).' },
  { id: 'D2.2', dim: 'D2', question: 'Which functions/files call the function `cbm_minhash_compute`? List the caller locations (files).' },
  { id: 'D2.3', dim: 'D2', question: 'Which functions/files call the function `cbm_split_camel_case`? List the caller locations (files).' },
  { id: 'D2.4', dim: 'D2', question: 'Which functions/files call the function `cbm_sem_tokenize`? List the caller locations (files).' },
  { id: 'D2.5', dim: 'D2', question: 'Which functions/files call the function `ac_build_trie`? List the caller locations (files).' },
  { id: 'D3.1', dim: 'D3', question: 'In which file and at what line is the function `ac_build_trie` defined? Show its full signature.' },
  { id: 'D3.2', dim: 'D3', question: 'In which file and at what line is the function `cbm_minhash_jaccard` defined? Show its full signature.' },
  { id: 'D3.3', dim: 'D3', question: 'In which file and at what line is the function `cbm_split_camel_case` defined? Show its full signature.' },
  { id: 'D3.4', dim: 'D3', question: 'In which file and at what line is the function `cbm_arena_alloc` defined? Show its full signature.' },
  { id: 'D3.5', dim: 'D3', question: 'In which file and at what line is the function `camel_should_split` defined? Show its full signature.' },
  { id: 'D4.1', dim: 'D4', question: 'What are the main subsystems / top-level modules of this codebase? Describe the overall architecture.' },
  { id: 'D4.2', dim: 'D4', question: 'Which source file is the largest / most central hub by number of functions, and roughly how many functions does it have?' },
  { id: 'D4.3', dim: 'D4', question: 'What is the program entry point, and which file contains main()?' },
  { id: 'D4.4', dim: 'D4', question: 'Which directory contains the per-language LSP type resolvers, and which languages have a hybrid LSP module?' },
  { id: 'D4.5', dim: 'D4', question: 'How is the MCP server layer organized — which file implements it and what does it expose?' },
  { id: 'D5.1', dim: 'D5', question: 'Find the functions that implement near-duplicate / clone detection (minhash/simhash).' },
  { id: 'D5.2', dim: 'D5', question: 'Where is camelCase / identifier tokenization (splitting identifiers into words) implemented?' },
  { id: 'D5.3', dim: 'D5', question: 'Find the arena / bump allocator implementation (allocation from a memory arena).' },
  { id: 'D5.4', dim: 'D5', question: 'Which code implements Aho-Corasick multi-pattern string matching?' },
  { id: 'D5.5', dim: 'D5', question: 'Where is the Cypher query language parsed and executed?' },
]
const CONDS = ['cbm', 'graphify', 'explorer']

const ANSWER_SCHEMA = {
  type: 'object',
  properties: {
    answer: { type: 'string', description: 'the direct factual answer, list-form where applicable' },
    tool_calls: { type: 'integer', description: 'how many tool/CLI invocations you made' },
  },
  required: ['answer', 'tool_calls'],
  additionalProperties: false,
}
const JUDGE_SCHEMA = {
  type: 'object',
  properties: {
    scores: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          slot: { type: 'string' },
          correctness: { type: 'number', description: '0-4 vs ground truth' },
          justification: { type: 'string' },
        },
        required: ['slot', 'correctness', 'justification'],
        additionalProperties: false,
      },
    },
  },
  required: ['scores'],
  additionalProperties: false,
}

function cbmPrompt(q) {
  return `You answer ONE question about a C codebase using ONLY the codebase-memory (cbm) graph tools via its CLI. STRICT RULE: do NOT read, cat, grep, or open source files. Use ONLY cbm CLI.

Invoke: CBM_CACHE_DIR='${cfg.cbmCache}' '${cfg.cbmBin}' cli --json <tool> '<jsonargs>'
The output is wrapped as {"content":[{"text":"<INNER JSON>"}]} — parse the inner text field.
Project name: ${cfg.proj}
Tools (jsonargs always include "project"):
- query_graph {"project","query"}  -> openCypher (e.g. MATCH (f:File {name:'minhash.c'})-[:DEFINES]->(fn:Function) RETURN fn.name ; or MATCH (n:Function {name:'X'}) RETURN n.file_path,n.line)
- search_graph {"project","query"|"name_pattern"|"semantic_query":["kw"]}
- trace_path {"project","function_name","direction":"inbound|outbound|both","depth":2}
- get_code_snippet {"project","qualified_name"}
- get_architecture {"project","aspects":["clusters","hotspots"]}
- search_code {"project","pattern"}

QUESTION (${q.id}): ${q.question}

Use the cbm tools to find the answer. Give the DIRECT answer (names / file:line / list). Do not narrate which tool you used. Report tool_calls = number of CLI invocations you ran.`
}

function gfyPrompt(q) {
  return `You answer ONE question about a C codebase using ONLY the graphify graph tools. STRICT RULE: do NOT read, cat, grep, or open the .c/.h source files. Use ONLY graphify commands and (optionally) the graphify-out report.

Commands:
- '${cfg.gfyBin}' query "<natural question>" --graph '${cfg.graph}' --budget 1500
- '${cfg.gfyBin}' query "<q>" --dfs --graph '${cfg.graph}'   (depth-first for paths)
- '${cfg.gfyBin}' path "A" "B" --graph '${cfg.graph}'
- '${cfg.gfyBin}' explain "X" --graph '${cfg.graph}'
- '${cfg.gfyBin}' affected "X" --graph '${cfg.graph}'
You may also read '${cfg.gfyOut}/GRAPH_REPORT.md' for architecture questions.
The graph node labels look like 'foo()' for functions; source_file/source_location give file and line.

QUESTION (${q.id}): ${q.question}

Use graphify to find the answer. Give the DIRECT answer (names / file:line / list). Do not narrate which tool you used. Report tool_calls = number of graphify invocations you ran.`
}

function expPrompt(q) {
  return `You answer ONE question about a C codebase using ONLY plain text exploration: Grep, Glob, and Read on the source tree at ${cfg.corpus}. STRICT RULE: do NOT use any graph/database tool.

QUESTION (${q.id}): ${q.question}

Find the answer by searching/reading the source. Give the DIRECT answer (names / file:line / list). Report tool_calls = number of Grep/Glob/Read calls you made.`
}

function judgePrompt(q, slotted, pass) {
  const block = slotted.map(s => `--- Answer ${s.slot} ---\n${s.answer}`).join('\n\n')
  return `You are a STRICT, blind grader. Score each candidate answer 0-4 against the GROUND TRUTH for a codebase question. You do not know which tool produced which answer.

First, load the ground truth by running:
python3 -c "import json; [print(json.dumps(j['ground_truth'],indent=0)) for j in (json.loads(l) for l in open('${cfg.evalDir}/qbank.jsonl')) if j['id']=='${q.id}']"
You MAY also verify against the real source at ${cfg.corpus} (you are the judge — reading source is allowed for grading).

QUESTION (${q.id}, ${q.dim}): ${q.question}

Rubric (0-4):
- 4 = fully correct AND complete vs ground truth (for set questions: ~all items, no fabrication; for location: correct file+line+signature)
- 3 = mostly correct, minor omissions or one small error
- 2 = partially correct (about half right, or right answer buried in noise/extras)
- 1 = mostly wrong / largely incomplete
- 0 = wrong, irrelevant, fabricated, or "no answer"
For D2 caller questions the ground truth is a grep upper-bound; credit answers that identify the real caller files even if counts differ. Penalize fabricated function/file names not in the source.

Candidate answers:
${block}

Score every slot (A, B, C). Pass #${pass}. Be calibrated and consistent.`
}

phase('Answer')
const results = await pipeline(
  Qs,
  async (q) => {
    const ans = await parallel([
      () => agent(cbmPrompt(q), { label: `ans:${q.id}:cbm`, phase: 'Answer', schema: ANSWER_SCHEMA }),
      () => agent(gfyPrompt(q), { label: `ans:${q.id}:gfy`, phase: 'Answer', schema: ANSWER_SCHEMA }),
      () => agent(expPrompt(q), { label: `ans:${q.id}:exp`, phase: 'Answer', schema: ANSWER_SCHEMA }),
    ])
    return { q, answers: CONDS.map((c, i) => ({ cond: c, answer: (ans[i] && ans[i].answer) || '(no answer)', tool_calls: (ans[i] && ans[i].tool_calls) || 0 })) }
  },
  async (r) => {
    const { q, answers } = r
    const rot = parseInt(q.id.replace(/\D/g, '') || '0', 10) % 3
    const slotNames = ['A', 'B', 'C']
    const order = [0, 1, 2].map((i) => (i + rot) % 3)
    const slotted = order.map((condIdx, k) => ({ slot: slotNames[k], cond: answers[condIdx].cond, answer: answers[condIdx].answer }))
    const judged = await parallel([0, 1].map((p) => () =>
      agent(judgePrompt(q, slotted, p), { label: `judge:${q.id}:p${p}`, phase: 'Judge', schema: JUDGE_SCHEMA })
    ))
    // map slot->cond
    const slotToCond = {}
    slotted.forEach((s) => { slotToCond[s.slot] = s.cond })
    return { id: q.id, dim: q.dim, question: q.question, answers, slotToCond, judged: judged.filter(Boolean) }
  }
)

// aggregate by dimension and condition
const agg = {}
for (const r of results) {
  if (!r) continue
  const condScore = {}
  for (const pass of r.judged) {
    for (const s of (pass.scores || [])) {
      const cond = r.slotToCond[s.slot]
      if (!cond) continue
      condScore[cond] = condScore[cond] || []
      condScore[cond].push(s.correctness)
    }
  }
  r.condMeanScore = {}
  for (const c of CONDS) {
    const arr = condScore[c] || []
    r.condMeanScore[c] = arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null
  }
  for (const c of CONDS) {
    const key = `${r.dim}|${c}`
    agg[key] = agg[key] || []
    if (r.condMeanScore[c] != null) agg[key].push(r.condMeanScore[c])
  }
}
const summary = {}
for (const dim of ['D1', 'D2', 'D3', 'D4', 'D5']) {
  summary[dim] = {}
  for (const c of CONDS) {
    const arr = agg[`${dim}|${c}`] || []
    summary[dim][c] = arr.length ? +(arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(2) : null
  }
}
// overall mean per condition
summary.OVERALL = {}
for (const c of CONDS) {
  const all = []
  for (const dim of ['D1', 'D2', 'D3', 'D4', 'D5']) if (summary[dim][c] != null) all.push(summary[dim][c])
  summary.OVERALL[c] = all.length ? +(all.reduce((a, b) => a + b, 0) / all.length).toFixed(2) : null
}
log(`SUMMARY (mean 0-4): cbm=${summary.OVERALL.cbm} graphify=${summary.OVERALL.graphify} explorer=${summary.OVERALL.explorer}`)
return { summary, results }