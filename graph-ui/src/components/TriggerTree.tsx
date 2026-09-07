import { useEffect, useState } from "react";
import { useUiMessages } from "../lib/i18n";
import {
  fetchWhy,
  formatGuard,
  allUnguarded,
  type WhyEntry,
  type WhyPayload,
} from "../lib/why";

/* The Why view: "when does this run?" — callers on the spine, each edge
 * carrying its syntactic guard chain as chips, expandable upward in place.
 * Thin by default; elision declared; guards honest about being syntactic. */

function DispatchChip({ entry }: { entry: WhyEntry }) {
  const t = useUiMessages();
  if (!entry.candidates || entry.candidates <= 1) return null;
  return (
    <span
      className="text-[11px] px-1.5 rounded-full border border-border/60 text-foreground/55"
      title={t.why.dispatchTitle}
    >
      {t.why.dispatchChip(entry.candidates)}
    </span>
  );
}

function GuardChips({ entry }: { entry: WhyEntry }) {
  const t = useUiMessages();
  if (entry.guards_unavailable)
    return <span className="text-[12px] text-foreground/35">{t.why.guardsUnavailable}</span>;
  if (entry.guards.length === 0 && !entry.loop)
    return <span className="text-[12px] text-foreground/35">{t.why.unguarded}</span>;
  return (
    <span className="inline-flex flex-wrap gap-1 items-center">
      {entry.loop && (
        <span className="text-[11px] px-1.5 rounded-full border border-border/60 text-foreground/50" title={t.why.loopChipTitle}>
          {t.why.loopChip}
        </span>
      )}
      {entry.guards.map((guard, index) => (
        <span
          key={index}
          className={`text-[11px] px-1.5 rounded-full border font-mono ${
            guard.negated
              ? "border-warn/50 text-warn/90 bg-warn/10"
              : "border-primary/40 text-primary/90 bg-primary/10"
          }`}
          title={t.why.guardChipTitle(guard.kind)}
        >
          ? {formatGuard(guard, t.why)}
        </span>
      ))}
    </span>
  );
}

function WhyLevel({
  project,
  refId,
  direction,
  depth,
  onOpenSymbol,
}: {
  project: string;
  refId: number;
  direction: "up" | "down";
  depth: number;
  onOpenSymbol: (ref: { id?: number; qn?: string }) => void;
}) {
  const [payload, setPayload] = useState<WhyPayload | null>(null);
  const [error, setError] = useState(false);
  const [expanded, setExpanded] = useState<Set<number>>(new Set());

  useEffect(() => {
    let cancelled = false;
    setPayload(null);
    setExpanded(new Set());
    fetchWhy(project, { id: refId }, direction)
      .then((p) => {
        if (!cancelled) setPayload(p);
      })
      .catch(() => {
        if (!cancelled) setError(true);
      });
    return () => {
      cancelled = true;
    };
  }, [project, refId, direction]);

  if (error)
    return <p className="text-[12px] text-foreground/35">could not load this level</p>;
  if (!payload)
    return <p className="text-[12px] text-foreground/35">tracing…</p>;
  if (payload.entries.length === 0)
    return (
      <p className="text-[13px] text-foreground/40">
        {direction === "up"
          ? "No resolved callers — an entry point, or reached another way (dispatch, event, reflection)."
          : "No resolved callees."}
      </p>
    );

  return (
    <div className={depth > 0 ? "border-l border-border/40 pl-3 ml-1" : ""}>
      {payload.entries.map((entry) => (
        <div key={entry.id} className="py-[3px]">
          <div className="flex items-baseline gap-2 flex-wrap">
            <span className="text-foreground/35 text-[12px] shrink-0">
              {direction === "up" ? "←" : "→"}
            </span>
            <button
              onClick={() => onOpenSymbol({ id: entry.id })}
              className="text-[13px] font-mono text-foreground/75 hover:text-primary transition-colors"
              title={entry.file_path && entry.line ? `${entry.file_path}:${entry.line}` : entry.file_path}
            >
              {entry.name}
            </button>
            <GuardChips entry={entry} />
            <DispatchChip entry={entry} />
            {direction === "up" && entry.more > 0 && (
              <button
                onClick={() =>
                  setExpanded((prev) => {
                    const next = new Set(prev);
                    if (next.has(entry.id)) next.delete(entry.id);
                    else next.add(entry.id);
                    return next;
                  })
                }
                className="text-[12px] text-foreground/40 hover:text-primary transition-colors"
              >
                {expanded.has(entry.id)
                  ? "− collapse"
                  : `⌄ ${entry.more} caller${entry.more === 1 ? "" : "s"} above`}
              </button>
            )}
          </div>
          {expanded.has(entry.id) && depth < 6 && (
            <WhyLevel
              project={project}
              refId={entry.id}
              direction={direction}
              depth={depth + 1}
              onOpenSymbol={onOpenSymbol}
            />
          )}
        </div>
      ))}
      {payload.total > payload.entries.length && (
        <p className="text-[12px] text-foreground/35 mt-1">
          {payload.entries.length} of {payload.total} shown — the rest via the caller list
          above.
        </p>
      )}
    </div>
  );
}

/* The decision-table pivot (Huysmans 2011: tables win for
 * condition-combination lookup): rows = callers, columns = the distinct
 * conditions seen across their call sites, cells = when/unless/—. Offered
 * only when several guarded callers exist. */
function GuardTable({
  entries,
  onOpenSymbol,
}: {
  entries: WhyEntry[];
  onOpenSymbol: (ref: { id?: number; qn?: string }) => void;
}) {
  const conds: string[] = [];
  for (const entry of entries)
    for (const guard of entry.guards) {
      const key = guard.cond ?? guard.kind;
      if (!conds.includes(key)) conds.push(key);
    }
  const shown = conds.slice(0, 6);
  return (
    <div className="overflow-x-auto">
      <table className="text-[12px] border-collapse">
        <thead>
          <tr>
            <th className="text-left pr-3 pb-1 text-foreground/40 font-normal uppercase tracking-wider text-[11px]">
              caller
            </th>
            {shown.map((cond) => (
              <th
                key={cond}
                className="text-left px-2 pb-1 text-foreground/50 font-mono font-normal max-w-[180px] truncate"
                title={cond}
              >
                {cond}
              </th>
            ))}
            {conds.length > shown.length && (
              <th className="text-left px-2 pb-1 text-foreground/35 font-normal">
                +{conds.length - shown.length} more
              </th>
            )}
          </tr>
        </thead>
        <tbody>
          {entries.map((entry) => (
            <tr key={entry.id} className="border-t border-border/30">
              <td className="pr-3 py-1">
                <button
                  onClick={() => onOpenSymbol({ id: entry.id })}
                  className="font-mono text-foreground/70 hover:text-primary transition-colors"
                >
                  {entry.name}
                </button>
              </td>
              {shown.map((cond) => {
                const guard = entry.guards.find((g) => (g.cond ?? g.kind) === cond);
                return (
                  <td key={cond} className="px-2 py-1">
                    {guard ? (
                      <span
                        className={guard.negated ? "text-warn/90" : "text-good"}
                        title={guard.negated ? "call sits in the else arm" : "call requires this"}
                      >
                        {guard.negated ? "✗ unless" : "✓ when"}
                      </span>
                    ) : (
                      <span className="text-foreground/25">—</span>
                    )}
                  </td>
                );
              })}
              {conds.length > shown.length && <td className="px-2 py-1" />}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function TriggerTree({
  project,
  symbolId,
  onOpenSymbol,
}: {
  project: string;
  symbolId: number;
  onOpenSymbol: (ref: { id?: number; qn?: string }) => void;
}) {
  const t = useUiMessages();
  const [direction, setDirection] = useState<"up" | "down">("up");
  const [root, setRoot] = useState<WhyPayload | null>(null);
  const [asTable, setAsTable] = useState(false);
  const guardedCount = (root?.entries ?? []).filter((e) => e.guards.length > 0).length;

  useEffect(() => {
    let cancelled = false;
    setRoot(null);
    fetchWhy(project, { id: symbolId }, direction)
      .then((p) => {
        if (!cancelled) setRoot(p);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [project, symbolId, direction]);

  return (
    <div className="bg-card border border-border/50 rounded-md p-4">
      <div className="flex items-baseline gap-2 mb-2">
        <p className="text-[12px] uppercase tracking-widest text-foreground/40">
          {direction === "up" ? t.why.whenRuns : t.why.whatTriggers}
        </p>
        <div className="ml-auto flex items-center gap-3">
          {guardedCount >= 3 && (
            <button
              onClick={() => setAsTable((v) => !v)}
              className="text-[12px] text-foreground/45 hover:text-primary transition-colors"
              title="Pivot the guarded callers into a condition table"
            >
              {asTable ? "tree" : "table"}
            </button>
          )}
          <button
            onClick={() => setDirection((d) => (d === "up" ? "down" : "up"))}
            className="text-[12px] text-foreground/45 hover:text-primary transition-colors"
          >
            {direction === "up" ? t.why.toWhatTriggers : t.why.toWhenRuns}
          </button>
        </div>
      </div>
      {root && allUnguarded(root.entries) && root.entries.length > 0 && root.entries.length <= 2 ? (
        <p className="text-[13px] text-foreground/60">
          {direction === "up" ? t.why.alwaysRunsPrefix : t.why.alwaysTriggersPrefix}
          {root.entries.map((entry, index) => (
            <span key={entry.id}>
              {index > 0 && t.why.alwaysJoiner}
              <button
                onClick={() => onOpenSymbol({ id: entry.id })}
                className="font-mono text-foreground/80 hover:text-primary transition-colors"
              >
                {entry.name}
              </button>
            </span>
          ))}
          {direction === "up" ? t.why.alwaysRunsSuffix : t.why.alwaysTriggersSuffix}
        </p>
      ) : asTable && root ? (
        <GuardTable
          entries={root.entries.filter((e) => e.guards.length > 0)}
          onOpenSymbol={onOpenSymbol}
        />
      ) : (
        <WhyLevel
          project={project}
          refId={symbolId}
          direction={direction}
          depth={0}
          onOpenSymbol={onOpenSymbol}
        />
      )}
      <p className="text-[11px] text-foreground/30 mt-2">{t.why.honestyFooter}</p>
    </div>
  );
}
