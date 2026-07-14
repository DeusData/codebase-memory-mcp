/* Graph data types matching the C layout3d.c JSON output */

export interface GraphNode {
  id: number;
  x: number;
  y: number;
  z: number;
  label: string;
  name: string;
  file_path?: string;
  qualified_name?: string;
  start_line?: number;
  end_line?: number;
  size: number;
  color: string;
  /* Dead-code classification from the backend layout (layout3d.c). */
  status?: NodeStatus;
  in_calls?: number;
}

export type NodeStatus =
  | "dead"
  | "single"
  | "entry"
  | "test"
  | "exported"
  | "normal"
  | "structural";

/* Git remote metadata for building GitHub deep-links (/api/repo-info). */
export interface RepoInfo {
  root_path: string;
  branch: string;
  remote_url: string;
  web_base: string; /* e.g. github.com/<org>/<repo> */
  blob_base: string; /* e.g. github.com/<org>/<repo>/blob/<branch> */
}

export interface GraphEdge {
  source: number;
  target: number;
  type: string;
}

export interface LinkedProject {
  project: string;
  nodes: GraphNode[];
  edges: GraphEdge[];
  offset: { x: number; y: number; z: number };
  cross_edges: GraphEdge[];
}

/* Missed-graph skeleton (#963): the file structure of files the indexer
 * could not fully cover, laid out as a satellite cluster beside the code
 * galaxy (server-computed offset, same shape as LinkedProject's). */
export interface MissedGraph {
  nodes: GraphNode[];
  edges: GraphEdge[];
  offset: { x: number; y: number; z: number };
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
  total_nodes: number;
  linked_projects?: LinkedProject[];
  missed_graph?: MissedGraph;
}

export interface Project {
  name: string;
  root_path: string;
  indexed_at: string;
}

export interface SchemaInfo {
  node_labels: { label: string; count: number }[];
  edge_types: { type: string; count: number }[];
  total_nodes: number;
  total_edges: number;
}

export type TabId = "graph" | "stats" | "control";

export interface ProcessInfo {
  pid: number;
  cpu: number;
  rss_mb: number;
  elapsed: string;
  command: string;
  is_self: boolean;
}
