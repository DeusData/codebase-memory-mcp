import { useCallback, useEffect, useRef, useState } from "react";
import { callTool } from "../api/rpc";
import type { Project, SchemaInfo } from "../lib/types";

interface ProjectInfo {
  project: Project;
  schema: SchemaInfo | null;
}

interface ProjectPage {
  projects?: Project[];
  has_more?: boolean;
  next_offset?: number;
}

interface SchemaPage extends SchemaInfo {
  has_more?: boolean;
  next_offset?: number;
}

const PAGE_LIMIT = 500;

function nextPageOffset(page: { has_more?: boolean; next_offset?: number }, offset: number) {
  if (typeof page.has_more !== "boolean") {
    throw new Error("Invalid pagination response");
  }
  if (!page.has_more) return null;
  if (!Number.isInteger(page.next_offset) || page.next_offset! <= offset) {
    throw new Error("Invalid pagination response");
  }
  return page.next_offset!;
}

async function fetchAllProjects(): Promise<Project[]> {
  const projects: Project[] = [];
  let offset = 0;
  for (;;) {
    const page = await callTool<ProjectPage>("list_projects", {
      format: "json",
      detail: "stats",
      limit: PAGE_LIMIT,
      offset,
    });
    projects.push(...(page.projects ?? []));
    const next = nextPageOffset(page, offset);
    if (next === null) return projects;
    offset = next;
  }
}

async function fetchFullSchema(project: string): Promise<SchemaInfo> {
  const nodeLabels: SchemaInfo["node_labels"] = [];
  const edgeTypes: SchemaInfo["edge_types"] = [];
  let firstPage: SchemaPage | null = null;
  let offset = 0;
  for (;;) {
    const page = await callTool<SchemaPage>("get_graph_schema", {
      project,
      format: "json",
      limit: PAGE_LIMIT,
      offset,
    });
    firstPage ??= page;
    nodeLabels.push(...(page.node_labels ?? []));
    edgeTypes.push(...(page.edge_types ?? []));
    const next = nextPageOffset(page, offset);
    if (next === null) {
      return { ...firstPage, node_labels: nodeLabels, edge_types: edgeTypes };
    }
    offset = next;
  }
}

interface UseProjectsResult {
  projects: ProjectInfo[];
  loading: boolean;
  error: string | null;
  refresh: () => void;
}

/* Schemas above this size are never auto-fetched: get_graph_schema scans
 * every node and edge, which on a multi-million-node index takes ~30 s of
 * server time. The card still shows totals from list_projects; only the
 * per-label chips are absent. */
const SCHEMA_LAZY_MAX_NODES = 200_000;
/* And never enrich more than this many cards per visit. */
const SCHEMA_LAZY_MAX_PROJECTS = 20;

/* Session-scoped schema cache: revisiting the Projects tab must not rescan
 * stores the server already walked for us once. */
const schemaCache = new Map<string, SchemaInfo>();

export function useProjects(): UseProjectsResult {
  const [projects, setProjects] = useState<ProjectInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  /* Bumped on unmount/refresh so a stale enrichment loop stops issuing
   * requests and stops patching state. */
  const generationRef = useRef(0);

  const fetchProjects = useCallback(async () => {
    const generation = ++generationRef.current;
    setLoading(true);
    setError(null);
    try {
      /* Paginated, format json, detail stats: node/edge totals and indexed_at
       * come with the list, so every card renders before any schema scan. */
      const list = await fetchAllProjects();
      if (generation !== generationRef.current) return;

      /* Render immediately from list_projects' own totals… */
      setProjects(
        list.map((p) => ({ project: p, schema: schemaCache.get(p.name) ?? null })),
      );
      setLoading(false);

      /* …then enrich small projects with per-label schemas, one at a time,
       * patching each card as its schema lands. Sequential on purpose — N
       * parallel scans would serialize behind the store lock anyway — and
       * abandoned as soon as the tab unmounts or refreshes. */
      const candidates = list
        .filter(
          (p) => !schemaCache.has(p.name) && (p.nodes ?? 0) <= SCHEMA_LAZY_MAX_NODES,
        )
        .slice(0, SCHEMA_LAZY_MAX_PROJECTS);
      for (const p of candidates) {
        if (generation !== generationRef.current) return;
        try {
          const schema = await fetchFullSchema(p.name);
          schemaCache.set(p.name, schema);
          if (generation !== generationRef.current) return;
          setProjects((prev) =>
            prev.map((info) =>
              info.project.name === p.name ? { ...info, schema } : info,
            ),
          );
        } catch {
          /* card keeps its totals; chips stay absent */
        }
      }
    } catch (e) {
      if (generation !== generationRef.current) return;
      setError(e instanceof Error ? e.message : "Failed to fetch projects");
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchProjects();
    return () => {
      /* Stop any in-flight enrichment loop when the tab goes away. */
      generationRef.current++;
    };
  }, [fetchProjects]);

  return { projects, loading, error, refresh: fetchProjects };
}
