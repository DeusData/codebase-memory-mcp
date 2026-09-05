import { useCallback, useEffect, useState } from "react";
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

export function useProjects(): UseProjectsResult {
  const [projects, setProjects] = useState<ProjectInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchProjects = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await fetchAllProjects();

      /* Fetch schema for each project */
      const infos: ProjectInfo[] = await Promise.all(
        list.map(async (p) => {
          try {
            const schema = await fetchFullSchema(p.name);
            return { project: p, schema };
          } catch {
            return { project: p, schema: null };
          }
        }),
      );

      setProjects(infos);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to fetch projects");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchProjects();
  }, [fetchProjects]);

  return { projects, loading, error, refresh: fetchProjects };
}
