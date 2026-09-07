import type { Project, SchemaInfo } from "../lib/types";
import { colorForLabel } from "../lib/colors";

interface ProjectCardProps {
  project: Project;
  schema: SchemaInfo | null;
  onSelect: (project: string) => void;
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

export function ProjectCard({ project, schema, onSelect }: ProjectCardProps) {
  const totalNodes = schema?.node_labels?.reduce((s, l) => s + l.count, 0) ?? 0;
  const totalEdges = schema?.edge_types?.reduce((s, t) => s + t.count, 0) ?? 0;

  return (
    <div className="border border-white/10 rounded-md p-4 hover:border-white/20 transition-colors">
      <div className="flex items-start justify-between mb-3">
        <div>
          <h3 className="text-white font-medium">{project.name}</h3>
          <p className="text-foreground/40 text-xs font-mono mt-0.5 truncate max-w-[300px]">
            {project.root_path}
          </p>
        </div>
        <button
          onClick={() => onSelect(project.name)}
          className="px-3 py-1 bg-cyan-500/20 hover:bg-cyan-500/30 text-cyan-300 rounded text-xs font-medium transition-colors"
        >
          View Graph
        </button>
      </div>

      {schema && (
        <>
          <div className="flex gap-4 text-xs text-foreground/60 mb-3">
            <span>{formatNumber(totalNodes)} nodes</span>
            <span>{formatNumber(totalEdges)} edges</span>
          </div>

          {schema.node_labels && schema.node_labels.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {schema.node_labels.map((l) => (
                <span
                  key={l.label}
                  className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs"
                  style={{ backgroundColor: colorForLabel(l.label) + "20" }}
                >
                  <span
                    className="w-1.5 h-1.5 rounded-full"
                    style={{ backgroundColor: colorForLabel(l.label) }}
                  />
                  <span style={{ color: colorForLabel(l.label) }}>
                    {l.label}
                  </span>
                  <span className="text-foreground/40">{formatNumber(l.count)}</span>
                </span>
              ))}
            </div>
          )}
        </>
      )}

      {!schema && (
        <p className="text-foreground/45 text-xs italic">Loading schema...</p>
      )}
    </div>
  );
}
