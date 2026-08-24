import { useCallback, useEffect, useState } from "react";
import { callTool } from "../api/rpc";
import type { Project, SchemaInfo } from "../lib/types";

interface ProjectInfo {
  project: Project;
  schema: SchemaInfo | null;
}

interface UseProjectsResult {
  projects: ProjectInfo[];
  loading: boolean;
  error: string | null;
  refresh: () => void;
}

interface ProjectPage {
  projects?: Project[];
  offset?: number;
  returned?: number;
  has_more?: boolean;
}

const PROJECT_PAGE_SIZE = 50;
const SCHEMA_CONCURRENCY = 4;

async function fetchAllProjects(): Promise<Project[]> {
  const projects: Project[] = [];
  let offset = 0;

  while (true) {
    const page = await callTool<ProjectPage>("list_projects", {
      offset,
      limit: PROJECT_PAGE_SIZE,
    });
    const pageProjects = page.projects ?? [];
    projects.push(...pageProjects);

    if (!page.has_more) return projects;

    const pageOffset = Number.isSafeInteger(page.offset) ? page.offset! : offset;
    const returned = Number.isSafeInteger(page.returned) ? page.returned! : pageProjects.length;
    const nextOffset = pageOffset + returned;
    if (returned <= 0 || nextOffset <= offset) {
      throw new Error("list_projects pagination did not advance");
    }
    offset = nextOffset;
  }
}

async function hydrateSchemas(infos: ProjectInfo[]): Promise<void> {
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < infos.length) {
      const index = nextIndex++;
      const project = infos[index].project;
      try {
        infos[index] = {
          project,
          schema: await callTool<SchemaInfo>("get_graph_schema", {
            project: project.name,
          }),
        };
      } catch {
        infos[index] = { project, schema: null };
      }
    }
  }

  const workerCount = Math.min(SCHEMA_CONCURRENCY, infos.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
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
      const infos: ProjectInfo[] = list.map((project) => ({ project, schema: null }));
      setProjects(infos);

      await hydrateSchemas(infos);
      setProjects([...infos]);
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
