/* Graph data types matching the C layout3d.c JSON output */

export interface GraphNode {
  id: number;
  x: number;
  y: number;
  z: number;
  label: string;
  name: string;
  file_path?: string;
  size: number;
  color: string;
}

export interface GraphEdge {
  source: number;
  target: number;
  type: string;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
  total_nodes: number;
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

/* ── Service Graph types (match Go service-graph/graph/types.go) ── */

export interface ServiceNode {
  id: string;
  repoPath: string;
  description?: string;
}

export interface TopicNode {
  id: string;
  options?: Record<string, string>;
}

export interface GraphQLEndpointNode {
  id: string;
  schemaFiles: string[];
  gatewayUrl?: string;
}

export interface DatabaseTableNode {
  id: string;
  orm?: string;
}

export interface ServiceEdge {
  source: string;
  target: string;
  type: string;
  file: string;
  line: number;
  metadata?: Record<string, string>;
}

export interface ServiceGraph {
  services: ServiceNode[];
  topics: TopicNode[];
  graphqlEndpoints: GraphQLEndpointNode[];
  tables: DatabaseTableNode[];
  edges: ServiceEdge[];
  scannedAt: string;
}
