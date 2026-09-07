/* CBM Atlas data layer: the /api services and the read-only /rpc tools the
 * Atlas screens consume. */
import { callTool } from "../api/rpc";

/* ── /api/tree ──────────────────────────────────────────────── */

export interface TreeChild {
  name: string;
  path: string;
  kind: "dir" | "file";
  symbols: number;
  files: number;
  missed?: number;
  region?: number;
}

export interface TreePayload {
  path: string;
  files: number;
  symbols: number;
  children_dropped?: number;
  children: TreeChild[];
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error ?? `HTTP ${res.status}`);
  }
  return res.json();
}

export function fetchTree(project: string, path: string): Promise<TreePayload> {
  const params = new URLSearchParams({ project, path });
  return getJson(`/api/tree?${params}`);
}

/* ── /api/symbol ────────────────────────────────────────────── */

export interface SymbolRef {
  region?: number;
  id: number;
  name: string;
  label?: string;
  file_path?: string;
  start_line?: number;
  qualified_name?: string;
  type?: string;
}

export interface ConnectionPage {
  total: number;
  by_type: Record<string, number>;
  limit: number;
  offset: number;
  items: SymbolRef[];
  /* The shape of what the page cut: hidden remainder grouped by file. */
  overflow_by_file?: { file: string; count: number }[];
}

export interface DataFlowRef {
  id: number;
  label?: string;
  name: string;
  file_path?: string;
  detail?: Record<string, unknown>;
}

export interface FileHistory {
  available: boolean;
  /* Normalized https forge base for commit/#ref links; absent when the
   * repo has no usable remote. */
  remote_url?: string;
  commits_1y?: number;
  recent?: { hash: string; time: number; author: string; subject: string }[];
  top_author?: string;
  top_author_share?: number;
  authors?: number;
}

export interface SymbolBundle {
  node: {
    id: number;
    label: string;
    name: string;
    qualified_name?: string;
    file_path?: string;
    start_line?: number;
    end_line?: number;
    docstring?: string;
    is_entry?: boolean;
    is_test?: boolean;
    is_exported?: boolean;
  };
  region?: { id: number; name?: string };
  callers: ConnectionPage;
  callees: ConnectionPage;
  tests?: SymbolRef[];
  co_change?: { file_path: string; score?: number }[];
  similar?: { id: number; name: string; file_path?: string; score?: number }[];
  data_in?: DataFlowRef[];
  data_out?: DataFlowRef[];
  file_history?: FileHistory;
  /* False = this project has no DATA_FLOWS edges at all, so the panel
   * would only ever say "none" and should not render. */
  project_has_data_flows?: boolean;
}

export function fetchSymbol(
  project: string,
  ref: { id?: number; qn?: string },
  limit = 50,
  offset = 0,
): Promise<SymbolBundle> {
  const params = new URLSearchParams({ project, limit: String(limit), offset: String(offset) });
  if (ref.id !== undefined) params.set("id", String(ref.id));
  else if (ref.qn) params.set("qn", ref.qn);
  return getJson(`/api/symbol?${params}`);
}

/* ── /api/flows ─────────────────────────────────────────────── */

export interface FlowSummary {
  id: number;
  label: string;
  entry: SymbolRef;
  terminal: SymbolRef;
  steps: number;
  sink_terminated: boolean;
  cross_region: boolean;
  steps_capped?: number;
}

export interface FlowsPayload {
  callable_total: number;
  candidates_dropped: number;
  flows: FlowSummary[];
}

/* Runtime evidence: this exact caller→callee pair fired in a recorded run.
 * Absent means "never recorded" — NOT dead; a run only covers what it
 * exercised. label/last_seen name the newest run, count spans all runs. */
export interface ObservedCall {
  count: number;
  label: string;
  last_seen: string;
}

export interface FlowStep {
  id: number;
  name: string;
  file_path?: string;
  depth: number;
  parent: number;
  guards?: { kind: string; cond?: string; negated?: boolean }[];
  /* Resolver confidence of the edge from the parent step (0..1). */
  confidence?: number;
  observed?: ObservedCall;
}

export interface FlowDetail {
  id: number;
  entry: SymbolRef;
  terminal: SymbolRef;
  sink_terminated: boolean;
  cross_region: boolean;
  steps_capped?: number;
  steps: FlowStep[];
}

export function fetchFlows(project: string): Promise<FlowsPayload> {
  return getJson(`/api/flows?${new URLSearchParams({ project })}`);
}

export function fetchFlow(project: string, id: number): Promise<FlowDetail> {
  /* guards=1: per-step syntactic guard chips, computed on demand. */
  return getJson(`/api/flow?${new URLSearchParams({ project, id: String(id), guards: "1" })}`);
}

/* ── get_architecture (format:"json" → {cols, rows} sections) ── */

export interface ArchSection {
  cols: string[];
  rows: (string | number | null)[][];
}

export type ArchitectureJson = Record<string, ArchSection | string | number>;

/* Decode one {cols, rows} section into row objects. Absent → []. */
export function archRows<T = Record<string, string | number | null>>(
  arch: ArchitectureJson | null,
  section: string,
): T[] {
  const s = arch?.[section];
  if (!s || typeof s !== "object" || !("cols" in s)) return [];
  const { cols, rows } = s as ArchSection;
  return rows.map((row) => {
    const obj: Record<string, string | number | null> = {};
    cols.forEach((col, i) => {
      obj[col] = row[i] ?? null;
    });
    return obj as T;
  });
}

export function fetchArchitecture(project: string): Promise<ArchitectureJson> {
  return callTool<ArchitectureJson>("get_architecture", {
    project,
    format: "json",
    aspects: ["overview"],
  });
}

/* ── detect_changes (format:"json") ─────────────────────────── */

export function fetchChanges(project: string): Promise<ArchitectureJson> {
  return callTool<ArchitectureJson>("detect_changes", {
    project,
    format: "json",
    risk_labels: true,
  });
}

/* ── search_graph (format:"json") ───────────────────────────────
 * Shape: {total, count, cols, groups:[{qn_prefix, file, rows}], has_more}.
 * The first row column is the qualified-name SUFFIX under qn_prefix. */

export interface SearchRow {
  qualified_name: string;
  name: string;
  label: string;
  lines?: string;
  fan_in: number;
  fan_out: number;
  file?: string;
}

interface SearchGroupsJson {
  total?: number;
  count?: number;
  groups?: {
    qn_prefix?: string;
    file?: string;
    rows: (string | number | null)[][];
  }[];
  has_more?: boolean;
}

export function decodeSearchGroups(payload: SearchGroupsJson): SearchRow[] {
  const out: SearchRow[] = [];
  for (const group of payload.groups ?? []) {
    for (const row of group.rows ?? []) {
      const suffix = String(row[0] ?? "");
      const qualified = group.qn_prefix ? `${group.qn_prefix}.${suffix}` : suffix;
      out.push({
        qualified_name: qualified,
        name: suffix.split(".").pop() ?? suffix,
        label: String(row[1] ?? ""),
        lines: row[2] ? String(row[2]) : undefined,
        fan_in: Number(row[3] ?? 0),
        fan_out: Number(row[4] ?? 0),
        file: group.file,
      });
    }
  }
  return out;
}

export async function searchGraph(
  project: string,
  args: Record<string, unknown>,
): Promise<{ rows: SearchRow[]; total: number }> {
  const payload = await callTool<SearchGroupsJson>("search_graph", {
    project,
    format: "json",
    ...args,
  });
  return { rows: decodeSearchGroups(payload), total: payload.total ?? 0 };
}

/* ── /api/trace — A→B reachability over calls or data flows ──── */

export interface TracePayload {
  mode: "calls" | "data";
  max_depth: number;
  reachable: boolean;
  explored?: number;
  error?: string;
  hops?: number;
  path?: {
    id: number;
    name: string;
    file_path?: string;
    guards?: { kind: string; cond?: string; negated?: boolean }[];
    observed?: ObservedCall;
  }[];
}

export function fetchTrace(
  project: string,
  from: string,
  to: string,
  mode: "calls" | "data",
): Promise<TracePayload> {
  const params = new URLSearchParams({ project, from, to, mode, guards: "1" });
  return getJson(`/api/trace?${params}`);
}

/* ── /api/scent — per-region search hit counts ─────────────────── */

export interface ScentPayload {
  query: string;
  total: number;
  unmapped: number;
  capped: boolean;
  regions: { region: number; count: number }[];
}

export function fetchScent(project: string, q: string): Promise<ScentPayload> {
  const params = new URLSearchParams({ project, q });
  return getJson(`/api/scent?${params}`);
}

/* ── /api/bridges — boundary spanners by distinct-region reach ── */

export interface BridgeRow {
  id: number;
  name: string;
  qualified_name?: string;
  file_path?: string;
  regions: number;
  cross_calls: number;
}

export function fetchBridges(project: string): Promise<{ bridges: BridgeRow[] }> {
  return getJson(`/api/bridges?${new URLSearchParams({ project })}`);
}

/* ── /api/blast — bucket a file list by region ─────────────────── */

export interface BlastPayload {
  files: number;
  unmapped: number;
  truncated: boolean;
  regions: { region: number; name?: string; count: number }[];
}

export function fetchBlast(project: string, files: string[]): Promise<BlastPayload> {
  const params = new URLSearchParams({ project, files: files.join(",") });
  return getJson(`/api/blast?${params}`);
}
