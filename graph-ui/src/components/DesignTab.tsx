import { useEffect, useMemo, useState } from "react";
import { callTool } from "../api/rpc";
import { useUiMessages } from "../lib/i18n";

interface DesignNode {
  id: number;
  name: string;
  qualified_name: string;
  file_path: string;
  line: number;
  properties?: {
    scope?: string;
    token_path?: string;
    token_type?: string;
    value?: string;
    description?: string;
    provenance?: "authoritative" | "generated" | "observed" | "derived";
    source_format?: string;
    modifier?: string;
    context?: string;
    default?: boolean;
    sources?: string[];
  };
}

interface DesignRelation {
  type: string;
  source: string;
  source_label: string;
  target: string;
  target_label: string;
  properties?: Record<string, unknown>;
}

interface DesignContextResponse {
  project: string;
  status: "ready" | "no_design_context";
  hint?: string;
  boundary: string;
  total: { systems: number; tokens: number; components: number; modes: number };
  systems: DesignNode[];
  tokens: DesignNode[];
  components: DesignNode[];
  modes: DesignNode[];
  relations: DesignRelation[];
  returned_relations: number;
  has_more: boolean;
  has_more_relations?: boolean;
  filtered_total?: { systems: number; tokens: number; components: number; modes: number };
  has_more_by_type?: { systems: boolean; tokens: boolean; components: boolean; modes: boolean };
}

const DESIGN_PAGE_LIMIT = 1000;

function mergeByKey<T>(current: T[], incoming: T[], key: (item: T) => string): T[] {
  const merged = new Map(current.map((item) => [key(item), item]));
  incoming.forEach((item) => merged.set(key(item), item));
  return [...merged.values()];
}

async function loadDesignContext(project: string): Promise<DesignContextResponse> {
  let offset = 0;
  let relationOffset = 0;
  let result: DesignContextResponse | null = null;
  let hasMoreNodes = true;
  let hasMoreRelations = true;

  while (hasMoreNodes || hasMoreRelations) {
    const page = await callTool<DesignContextResponse>("get_design_context", {
      project,
      limit: DESIGN_PAGE_LIMIT,
      offset,
      relation_offset: relationOffset,
    });
    if (!result) {
      result = { ...page, systems: [], tokens: [], components: [], modes: [], relations: [] };
    }
    result.systems = mergeByKey(result.systems, page.systems, (node) => node.qualified_name);
    result.tokens = mergeByKey(result.tokens, page.tokens, (node) => node.qualified_name);
    result.components = mergeByKey(result.components, page.components, (node) => node.qualified_name);
    result.modes = mergeByKey(result.modes, page.modes, (node) => node.qualified_name);
    result.relations = mergeByKey(
      result.relations,
      page.relations,
      (relation) => `${relation.type}:${relation.source}:${relation.target}:${JSON.stringify(relation.properties ?? {})}`,
    );
    hasMoreNodes = page.has_more;
    hasMoreRelations = page.has_more_relations ?? false;
    if (hasMoreNodes) offset += DESIGN_PAGE_LIMIT;
    if (hasMoreRelations) relationOffset += page.returned_relations || DESIGN_PAGE_LIMIT * 4;
  }
  if (!result) throw new Error("Design context returned no pages");
  result.returned_relations = result.relations.length;
  result.has_more = false;
  result.has_more_relations = false;
  return result;
}

function tokenColor(node: DesignNode): string | null {
  const value = node.properties?.value;
  if (node.properties?.token_type !== "color" || !value) return null;
  if (/^(#[\da-f]{3,8}|rgba?\(|hsla?\(|oklch\(|oklab\(|lab\(|lch\(|transparent$)/i.test(value)) {
    return value;
  }
  return null;
}

function shortQualifiedName(value: string): string {
  const marker = ".design.";
  const index = value.indexOf(marker);
  return index >= 0 ? value.slice(index + marker.length) : value;
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-xl border border-white/[0.06] bg-white/[0.025] px-4 py-3">
      <p className="text-[10px] uppercase tracking-[0.16em] text-foreground/30">{label}</p>
      <p className="mt-1 text-xl font-semibold tabular-nums text-foreground/90">{value.toLocaleString()}</p>
    </div>
  );
}

function ProvenanceBadge({ value }: { value?: string }) {
  const style =
    value === "authoritative"
      ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-300"
      : value === "generated"
        ? "border-amber-400/20 bg-amber-400/10 text-amber-300"
        : "border-slate-400/15 bg-slate-400/10 text-slate-300";
  return (
    <span className={`rounded-full border px-2 py-0.5 text-[9px] uppercase tracking-wider ${style}`}>
      {value ?? "observed"}
    </span>
  );
}

export function DesignTab({ project }: { project: string | null }) {
  const t = useUiMessages();
  const [data, setData] = useState<DesignContextResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [scope, setScope] = useState("");
  const [selectedQn, setSelectedQn] = useState<string | null>(null);

  useEffect(() => {
    if (!project) {
      setData(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    loadDesignContext(project)
      .then((response) => {
        if (!cancelled) setData(response);
      })
      .catch((reason: unknown) => {
        if (!cancelled) setError(reason instanceof Error ? reason.message : String(reason));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [project]);

  const filteredTokens = useMemo(() => {
    const needle = query.trim().toLowerCase();
    const scopeNeedle = scope.trim().toLowerCase();
    return (data?.tokens ?? []).filter((token) => {
      const haystack = `${token.name} ${token.qualified_name} ${token.properties?.token_path ?? ""}`.toLowerCase();
      const tokenScope = (token.properties?.scope ?? "root").toLowerCase();
      return (!needle || haystack.includes(needle)) && (!scopeNeedle || tokenScope === scopeNeedle);
    });
  }, [data, query, scope]);

  const filteredComponents = useMemo(
    () => (data?.components ?? []).filter((node) => !scope || (node.properties?.scope ?? "root") === scope),
    [data, scope],
  );
  const filteredModes = useMemo(
    () => (data?.modes ?? []).filter((node) => !scope || (node.properties?.scope ?? "root") === scope),
    [data, scope],
  );

  const selected = filteredTokens.find((token) => token.qualified_name === selectedQn) ?? filteredTokens[0];
  const selectedRelations = selected
    ? (data?.relations ?? []).filter(
        (relation) => relation.source === selected.qualified_name || relation.target === selected.qualified_name,
      )
    : [];

  if (!project) {
    return <div className="p-8 text-sm text-foreground/40">{t.design.selectProject}</div>;
  }

  return (
    <div className="h-full overflow-y-auto bg-[radial-gradient(circle_at_70%_0%,rgba(45,212,191,0.07),transparent_35%)]">
      <div className="mx-auto max-w-[1500px] p-6 lg:p-8">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="text-[10px] uppercase tracking-[0.22em] text-primary/70">{t.design.eyebrow}</p>
            <h1 className="mt-1 text-2xl font-semibold tracking-tight text-foreground/95">{t.design.title}</h1>
            <p className="mt-2 max-w-2xl text-[12px] leading-relaxed text-foreground/40">
              {t.design.description}
            </p>
          </div>
          <div className="rounded-lg border border-white/[0.05] bg-black/10 px-3 py-2 text-[10px] text-foreground/30">
            {t.design.localBoundary}
          </div>
        </div>

        {loading && <div className="mt-12 text-sm text-foreground/40">{t.common.loading}</div>}
        {error && <div className="mt-8 rounded-xl border border-red-400/20 bg-red-400/10 p-4 text-sm text-red-300">{error}</div>}

        {data && (
          <>
            <div className="mt-6 grid grid-cols-2 gap-3 md:grid-cols-5">
              <Metric label={t.design.systems} value={data.total.systems} />
              <Metric label={t.design.tokens} value={data.total.tokens} />
              <Metric label={t.design.components} value={data.total.components} />
              <Metric label={t.design.modes} value={data.total.modes} />
              <Metric label={t.design.relations} value={data.returned_relations} />
            </div>

            {data.status === "no_design_context" ? (
              <div className="mt-6 rounded-2xl border border-dashed border-primary/20 bg-primary/[0.04] p-8">
                <h2 className="text-base font-medium text-foreground/80">{t.design.emptyTitle}</h2>
                <p className="mt-2 text-[12px] text-foreground/40">{data.hint}</p>
                <p className="mt-4 font-mono text-[11px] text-primary/70">DESIGN.md · design/tokens/*.tokens.json</p>
              </div>
            ) : (
              <div className="mt-6 grid min-h-[560px] gap-4 lg:grid-cols-[270px_minmax(0,1fr)_340px]">
                <aside className="rounded-2xl border border-white/[0.06] bg-[#0b1920]/65 p-4">
                  <p className="text-[10px] uppercase tracking-[0.16em] text-foreground/30">{t.design.scopes}</p>
                  <button
                    onClick={() => setScope("")}
                    className={`mt-3 w-full rounded-lg px-3 py-2 text-left text-[11px] ${scope === "" ? "bg-primary/10 text-primary" : "text-foreground/45 hover:bg-white/[0.03]"}`}
                  >
                    {t.design.allScopes}
                  </button>
                  {data.systems.map((system) => {
                    const systemScope = system.properties?.scope ?? "root";
                    return (
                      <button
                        key={system.qualified_name}
                        onClick={() => setScope(systemScope)}
                        className={`mt-1 w-full rounded-lg px-3 py-2 text-left ${scope === systemScope ? "bg-primary/10" : "hover:bg-white/[0.03]"}`}
                      >
                        <span className="block text-[11px] text-foreground/70">{system.name}</span>
                        <span className="mt-0.5 block truncate font-mono text-[9px] text-foreground/25">{systemScope}</span>
                      </button>
                    );
                  })}

                  <p className="mt-6 text-[10px] uppercase tracking-[0.16em] text-foreground/30">{t.design.components}</p>
                  <div className="mt-2 space-y-1">
                    {filteredComponents.map((component) => (
                      <div key={component.qualified_name} className="rounded-lg bg-white/[0.025] px-3 py-2">
                        <p className="text-[11px] text-foreground/65">{component.name}</p>
                        <p className="mt-0.5 truncate font-mono text-[9px] text-foreground/25">{component.file_path}</p>
                      </div>
                    ))}
                  </div>

                  {filteredModes.length > 0 && (
                    <>
                      <p className="mt-6 text-[10px] uppercase tracking-[0.16em] text-foreground/30">{t.design.modes}</p>
                      <div className="mt-2 space-y-1">
                        {filteredModes.map((mode) => (
                          <div key={mode.qualified_name} className="rounded-lg border border-primary/10 bg-primary/[0.035] px-3 py-2">
                            <div className="flex items-center justify-between gap-2">
                              <p className="truncate text-[11px] text-foreground/65">{mode.name}</p>
                              {mode.properties?.default && <span className="rounded bg-primary/10 px-1.5 py-0.5 text-[8px] uppercase text-primary/70">default</span>}
                            </div>
                            {mode.properties?.sources && mode.properties.sources.length > 0 && (
                              <p className="mt-1 truncate font-mono text-[9px] text-foreground/25" title={mode.properties.sources.join(", ")}>
                                {mode.properties.sources.join(" · ")}
                              </p>
                            )}
                          </div>
                        ))}
                      </div>
                    </>
                  )}
                </aside>

                <section className="rounded-2xl border border-white/[0.06] bg-[#0b1920]/55 p-4">
                  <div className="flex gap-2">
                    <input
                      value={query}
                      onChange={(event) => setQuery(event.target.value)}
                      placeholder={t.design.searchTokens}
                      className="w-full rounded-lg border border-white/[0.06] bg-black/15 px-3 py-2 text-[12px] text-foreground/80 outline-none placeholder:text-foreground/20 focus:border-primary/30"
                    />
                    <span className="flex items-center rounded-lg bg-white/[0.03] px-3 text-[10px] tabular-nums text-foreground/35">
                      {filteredTokens.length}
                    </span>
                  </div>
                  <div className="mt-4 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
                    {filteredTokens.map((token) => {
                      const color = tokenColor(token);
                      const active = selected?.qualified_name === token.qualified_name;
                      return (
                        <button
                          key={token.qualified_name}
                          onClick={() => setSelectedQn(token.qualified_name)}
                          className={`min-w-0 rounded-xl border p-3 text-left transition-colors ${active ? "border-primary/35 bg-primary/[0.07]" : "border-white/[0.05] bg-white/[0.02] hover:border-white/[0.12]"}`}
                        >
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0">
                              <p className="truncate font-mono text-[11px] text-foreground/75">{token.properties?.token_path ?? token.name}</p>
                              <p className="mt-1 truncate text-[10px] text-foreground/30">{token.properties?.value ?? "—"}</p>
                            </div>
                            {color && <span className="h-7 w-7 shrink-0 rounded-lg border border-white/10" style={{ background: color }} />}
                          </div>
                          <div className="mt-3 flex items-center justify-between gap-2">
                            <span className="text-[9px] uppercase tracking-wider text-foreground/25">{token.properties?.token_type ?? "token"}</span>
                            <ProvenanceBadge value={token.properties?.provenance} />
                          </div>
                        </button>
                      );
                    })}
                  </div>
                </section>

                <aside className="rounded-2xl border border-white/[0.06] bg-[#0b1920]/65 p-4">
                  {selected ? (
                    <>
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="text-[10px] uppercase tracking-[0.16em] text-foreground/30">{t.design.tokenDetail}</p>
                          <h2 className="mt-2 break-words font-mono text-[13px] text-foreground/85">{selected.properties?.token_path ?? selected.name}</h2>
                        </div>
                        {tokenColor(selected) && <span className="h-10 w-10 shrink-0 rounded-xl border border-white/10" style={{ background: tokenColor(selected) ?? undefined }} />}
                      </div>
                      <dl className="mt-5 space-y-3 text-[11px]">
                        <div><dt className="text-foreground/25">{t.design.value}</dt><dd className="mt-1 break-all font-mono text-foreground/65">{selected.properties?.value ?? "—"}</dd></div>
                        <div><dt className="text-foreground/25">{t.design.source}</dt><dd className="mt-1 break-all font-mono text-foreground/55">{selected.file_path}:{selected.line}</dd></div>
                        <div><dt className="text-foreground/25">Qualified name</dt><dd className="mt-1 break-all font-mono text-[9px] text-foreground/35">{selected.qualified_name}</dd></div>
                        {selected.properties?.description && <div><dt className="text-foreground/25">{t.design.descriptionLabel}</dt><dd className="mt-1 leading-relaxed text-foreground/55">{selected.properties.description}</dd></div>}
                      </dl>
                      <p className="mt-6 text-[10px] uppercase tracking-[0.16em] text-foreground/30">{t.design.connected}</p>
                      <div className="mt-2 space-y-2">
                        {selectedRelations.length === 0 && <p className="text-[10px] text-foreground/25">{t.design.noRelations}</p>}
                        {selectedRelations.map((relation, index) => {
                          const outgoing = relation.source === selected.qualified_name;
                          const peer = outgoing ? relation.target : relation.source;
                          return (
                            <div key={`${relation.type}-${index}`} className="rounded-lg border border-white/[0.04] bg-white/[0.02] px-3 py-2">
                              <p className="text-[9px] uppercase tracking-wider text-primary/60">{outgoing ? "→" : "←"} {relation.type.replace(/_/g, " ")}</p>
                              <p className="mt-1 truncate font-mono text-[9px] text-foreground/35" title={peer}>{shortQualifiedName(peer)}</p>
                              {typeof relation.properties?.value === "string" && (
                                <p className="mt-1 break-all font-mono text-[9px] text-foreground/50">{relation.properties.value}</p>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </>
                  ) : (
                    <p className="text-[11px] text-foreground/30">{t.design.noTokens}</p>
                  )}
                </aside>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
