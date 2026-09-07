/* Dashboard — one hero and its context. The hero is the churn × complexity
 * list (the evidence-backed "where the bugs live") with a knee-point head,
 * a cost sentence, and mute-as-intended dispositions. Quality signals get
 * tones and trends; inventory facts live in the footer; histograms exist
 * only with a computed takeaway. No composite score — the cards are the
 * score. Everything derives from the graph and one bounded git-log pass. */
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { kneeCount, costSentence, findingKey, isDismissed, dismiss } from "../lib/findings";
import { useUiMessages } from "../lib/i18n";
import {
  CPLX_BINS,
  CPLX_TAIL_MIN,
  CPLX_TAIL_START,
  complexityTakeaway,
} from "../lib/complexity";
import { MetricChip } from "./MetricChip";

interface MetricEntry {
  qn?: string;
  name?: string;
  file?: string;
  value: number;
  commits?: number;
}

interface MetricsPayload {
  generated_from: string;
  totals: {
    callables: number;
    dead: number;
    tested_symbols: number;
    similar_edges: number;
    missed_files: number;
    exported: number;
    documented_exported: number;
    avg_complexity: number;
    files: number;
  };
  certainty: { calls: number; call_reference: number; usage: number };
  complexity_hist: number[];
  lines_hist: number[];
  top_complex: MetricEntry[];
  top_cognitive: MetricEntry[];
  top_long: MetricEntry[];
  top_churn: MetricEntry[];
  top_churn_complex: MetricEntry[];
  churn_available: boolean;
  churn_total_commits: number;
  churn_total_files: number;
  history: {
    indexed_at: string;
    callables: number;
    dead: number;
    avg_complexity: number;
    tested: number;
    usage_share: number;
  }[];
}

interface DashboardTabProps {
  project: string;
  onOpenSymbol: (ref: { qn: string }) => void;
  onOpenModulesPath: (path: string) => void;
  onOpenWiki: (slug: string) => void;
}

interface TrendPoint {
  /* Epoch ms of the snapshot; NaN when the timestamp failed to parse. */
  t: number;
  v: number;
}

/* Shape-only sparkline on a real time axis — reindex snapshots are
 * irregular, so equal spacing would lie about the pace of change. Each
 * snapshot is a dot; lines only join runs of ≥4 snapshots, and never span
 * a gap wider than 3× the median gap (a segment across a dark period
 * invents data). The printed delta beside it carries the magnitude. */
function Sparkline({ points }: { points: TrendPoint[] }) {
  if (points.length < 2) return null;
  const values = points.map((point) => point.v);
  const max = Math.max(...values);
  const min = Math.min(...values);
  const span = max - min || 1;
  const t0 = points[0].t;
  const tN = points[points.length - 1].t;
  const timed = points.every((point) => Number.isFinite(point.t)) && tN > t0;
  const xs = points.map((point, index) =>
    timed ? ((point.t - t0) / (tN - t0)) * 100 : (index / (points.length - 1)) * 100,
  );
  const ys = values.map((value) => 28 - ((value - min) / span) * 24);
  const gaps = xs.slice(1).map((x, index) => x - xs[index]);
  const medianGap = [...gaps].sort((a, b) => a - b)[Math.floor(gaps.length / 2)] || 1;
  const runs: string[][] = [[`${xs[0]},${ys[0]}`]];
  for (let i = 1; i < xs.length; i++) {
    if (gaps[i - 1] > 3 * medianGap) runs.push([]);
    runs[runs.length - 1].push(`${xs[i]},${ys[i]}`);
  }
  return (
    <svg viewBox="0 0 100 30" className="w-full h-[28px] mt-1" preserveAspectRatio="none">
      {points.length >= 4 &&
        runs
          .filter((run) => run.length > 1)
          .map((run, index) => (
            <polyline
              key={index}
              points={run.join(" ")}
              fill="none"
              stroke="currentColor"
              strokeWidth="1.5"
              className="text-primary/50"
              vectorEffect="non-scaling-stroke"
            />
          ))}
      {/* Dots as zero-length round-capped strokes — with preserveAspectRatio
       * "none", circles would stretch into ellipses; non-scaling strokes
       * don't. */}
      <path
        d={xs.map((x, index) => `M${x},${ys[index]}h0.01`).join("")}
        fill="none"
        stroke="currentColor"
        strokeWidth="3"
        strokeLinecap="round"
        className="text-primary/60"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
}

function QualityCard({
  label,
  value,
  sub,
  tone,
  trend,
  trendFormat,
}: {
  label: ReactNode;
  value: string;
  sub?: string;
  tone?: "warn" | "crit";
  trend?: TrendPoint[];
  trendFormat?: (v: number) => string;
}) {
  const fmt = trendFormat ?? ((v: number) => v.toLocaleString("en-US"));
  const first = trend?.[0]?.v;
  const last = trend?.[trend.length - 1]?.v;
  const delta = first !== undefined && last !== undefined ? last - first : 0;
  return (
    <div className="bg-card border border-border/50 rounded-md p-4 min-w-0">
      <p className="text-[12px] uppercase tracking-widest text-foreground/40">{label}</p>
      <p
        className={`text-[22px] font-semibold tabular-nums mt-1 ${
          tone === "crit" ? "text-crit" : tone === "warn" ? "text-warn" : "text-foreground/90"
        }`}
      >
        {value}
      </p>
      {sub && <p className="text-[12px] text-foreground/40 mt-0.5">{sub}</p>}
      {trend && trend.length > 1 && (
        <>
          <Sparkline points={trend} />
          {/* The min–max band makes any wobble fill it — this pair is the
           * honest magnitude. */}
          <p className="text-[12px] tabular-nums text-foreground/40 mt-0.5">
            {fmt(first!)} → {fmt(last!)} ({delta > 0 ? "+" : ""}
            {fmt(delta)})
          </p>
        </>
      )}
    </div>
  );
}

function TopList({
  title,
  entries,
  unit,
  total,
  onOpenSymbol,
}: {
  title: ReactNode;
  entries: MetricEntry[];
  unit: string;
  /* Size of the population the list was ranked over — only when the payload
   * carries a real total; never estimated. Absent → the cap reads "top N". */
  total?: number;
  onOpenSymbol?: (ref: { qn: string }) => void;
}) {
  return (
    <div className="bg-card border border-border/50 rounded-md p-4">
      <p className="text-[12px] uppercase tracking-widest text-foreground/40 mb-2 flex items-baseline justify-between gap-2">
        <span className="truncate">{title}</span>
        {entries.length > 0 && (
          <span className="text-foreground/30 tabular-nums shrink-0">
            top {entries.length.toLocaleString("en-US")}
            {total !== undefined && ` of ${total.toLocaleString("en-US")}`}
          </span>
        )}
      </p>
      <div className="space-y-px">
        {entries.map((entry, index) => {
          const label = entry.name ?? entry.file ?? "?";
          const clickable = entry.qn && onOpenSymbol;
          const Row = clickable ? "button" : "div";
          return (
            <Row
              key={`${label}-${index}`}
              onClick={clickable ? () => onOpenSymbol!({ qn: entry.qn! }) : undefined}
              className={`flex items-center gap-2 w-full text-left py-[3px] ${
                clickable ? "group cursor-pointer" : ""
              }`}
            >
              <span
                className={`text-[13px] font-mono truncate flex-1 ${
                  clickable
                    ? "text-foreground/60 group-hover:text-primary transition-colors"
                    : "text-foreground/55"
                }`}
                title={entry.qn ?? entry.file}
              >
                {label}
              </span>
              <span className="text-[12px] tabular-nums text-foreground/35 shrink-0">
                {entry.value.toLocaleString("en-US")} {unit}
              </span>
            </Row>
          );
        })}
        {entries.length === 0 && <p className="text-[13px] text-foreground/40">None.</p>}
      </div>
    </div>
  );
}

export function DashboardTab({
  project,
  onOpenSymbol,
  onOpenModulesPath,
  onOpenWiki,
}: DashboardTabProps) {
  const t = useUiMessages();
  const [metrics, setMetrics] = useState<MetricsPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showMuted, setShowMuted] = useState(false);
  const [mutedTick, setMutedTick] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setMetrics(null);
    setError(null);
    fetch(`/api/metrics?${new URLSearchParams({ project })}`)
      .then(async (res) => {
        if (!res.ok)
          throw new Error((await res.json().catch(() => null))?.error ?? `HTTP ${res.status}`);
        return res.json();
      })
      .then((payload) => {
        if (!cancelled) setMetrics(payload);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "failed");
      });
    return () => {
      cancelled = true;
    };
  }, [project]);

  /* Hero rows: knee-point head + dispositions. */
  const hero = useMemo(() => {
    void mutedTick;
    const entries = metrics?.top_churn_complex ?? [];
    const head = kneeCount(entries.map((entry) => entry.value));
    const rows = entries.map((entry, index) => ({
      entry,
      inHead: index < head,
      key: findingKey("hotspot", entry.file ?? String(index), entry.value),
    }));
    return {
      visible: rows.filter((row) => showMuted || !isDismissed(row.key)),
      mutedCount: rows.filter((row) => isDismissed(row.key)).length,
      head,
      headCommits: entries
        .slice(0, head)
        .reduce((sum, entry) => sum + (entry.commits ?? 0), 0),
    };
  }, [metrics, showMuted, mutedTick]);

  const takeaway = useMemo(() => {
    if (!metrics) return null;
    return complexityTakeaway(
      metrics.complexity_hist,
      metrics.top_complex.slice(0, 3).map((entry) => entry.name ?? ""),
      t.dashboard,
    );
  }, [metrics, t]);

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-crit/90 text-sm">{error}</p>
      </div>
    );
  }
  if (!metrics) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-foreground/45 text-sm">Measuring…</p>
      </div>
    );
  }

  const { totals, certainty } = metrics;
  const callish = certainty.calls + certainty.call_reference + certainty.usage;
  const resolvedShare = callish > 0 ? (certainty.calls + certainty.call_reference) / callish : 0;
  const docShare = totals.exported > 0 ? totals.documented_exported / totals.exported : 0;
  const testedShare = totals.callables > 0 ? totals.tested_symbols / totals.callables : 0;
  const history = metrics.history ?? [];
  const cost = costSentence(
    hero.head,
    hero.headCommits,
    totals.files,
    metrics.churn_total_commits,
    t.dashboard,
  );

  return (
    <ScrollArea className="h-full">
      <div className="max-w-[1200px] mx-auto px-6 py-6">
        {/* ── Hero: where the bugs live ─────────────────────────── */}
        <div className="bg-card border border-border/50 rounded-md p-5 mb-4">
          <div className="flex items-baseline gap-3 flex-wrap">
            <h2 className="text-[17px] font-semibold text-foreground/90">
              <MetricChip slug="hotspot" onOpen={onOpenWiki}>
                Where the bugs live — churn × complexity
              </MetricChip>
            </h2>
            {metrics.top_churn_complex.length > 0 && (
              <span className="text-[12px] uppercase tracking-widest text-foreground/35 tabular-nums">
                top {metrics.top_churn_complex.length.toLocaleString("en-US")}
                {metrics.churn_total_files > 0 &&
                  ` of ${metrics.churn_total_files.toLocaleString("en-US")}`}
              </span>
            )}
            {hero.mutedCount > 0 && (
              <button
                onClick={() => setShowMuted((value) => !value)}
                className="text-[12px] text-foreground/35 hover:text-foreground/60 transition-colors"
              >
                {showMuted ? "hide muted" : `${hero.mutedCount} muted — show`}
              </button>
            )}
          </div>
          {cost ? (
            <p className="text-[13px] text-foreground/55 mt-1">
              {cost}
              {t.dashboard.costConcentrate}
            </p>
          ) : (
            <p className="text-[13px] text-foreground/45 mt-1">
              {metrics.churn_available
                ? "Not enough churn data yet."
                : "Churn unavailable — the project root has no readable git history."}
            </p>
          )}
          <div className="mt-3 space-y-px">
            {hero.visible.map(({ entry, inHead, key }) => (
              <div
                key={key}
                className={`flex items-center gap-3 rounded-md px-3 py-[6px] ${
                  inHead ? "bg-popover border-l-2 border-warn/70" : ""
                }`}
              >
                <button
                  onClick={() => {
                    const parent = entry.file?.split("/").slice(0, -1).join("/") ?? "";
                    onOpenModulesPath(parent);
                  }}
                  className="text-[13px] font-mono text-foreground/70 hover:text-primary truncate flex-1 text-left transition-colors"
                  title="Open in Modules"
                >
                  {entry.file}
                </button>
                <span className="text-[12px] tabular-nums text-foreground/45 shrink-0">
                  {entry.commits?.toLocaleString("en-US") ?? "?"} commits
                </span>
                {/* The score is commits × max complexity — dividing the
                 * factor back out shows both axes, not just the ranking. */}
                <span className="text-[12px] tabular-nums text-foreground/45 shrink-0 w-16 text-right">
                  cplx {entry.commits ? Math.round(entry.value / entry.commits).toLocaleString("en-US") : "?"}
                </span>
                <span
                  className="text-[12px] tabular-nums text-foreground/35 shrink-0 w-20 text-right"
                  title="churn × complexity score — orders the list"
                >
                  {entry.value.toLocaleString("en-US")}
                </span>
                <button
                  onClick={() => {
                    dismiss(key);
                    setMutedTick((tick) => tick + 1);
                  }}
                  className="text-[12px] text-foreground/25 hover:text-foreground/60 shrink-0 transition-colors"
                  title="Mute as intended — resurfaces if the score grows an order of magnitude"
                >
                  mute
                </button>
              </div>
            ))}
            {hero.visible.length === 0 && (
              <p className="text-[13px] text-foreground/40 py-2">
                Nothing here — either healthy, or everything is muted.
              </p>
            )}
          </div>
        </div>

        {/* ── Quality signals: toned, trended ───────────────────── */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-4">
          <QualityCard
            label="Call resolution"
            value={`${(resolvedShare * 100).toFixed(0)}%`}
            sub={`${certainty.usage.toLocaleString("en-US")} USAGE edges unproven`}
            tone={resolvedShare < 0.6 ? "warn" : undefined}
            trend={history.map((entry) => ({
              t: Date.parse(entry.indexed_at),
              v: 1 - entry.usage_share,
            }))}
            trendFormat={(v) => `${(v * 100).toFixed(0)}%`}
          />
          <QualityCard
            label={
              <MetricChip slug="dead-candidate" onOpen={onOpenWiki}>
                Dead candidates
              </MetricChip>
            }
            value={totals.dead.toLocaleString("en-US")}
            sub="no callers; entry/test/exported excluded"
            tone={totals.dead > 0 ? "warn" : undefined}
            trend={history.map((entry) => ({
              t: Date.parse(entry.indexed_at),
              v: entry.dead,
            }))}
          />
          <QualityCard
            label={
              <MetricChip slug="tested" onOpen={onOpenWiki}>
                Tested symbols
              </MetricChip>
            }
            value={`${(testedShare * 100).toFixed(0)}%`}
            sub={`${totals.tested_symbols.toLocaleString("en-US")} with TESTS edges`}
            trend={history.map((entry) => ({
              t: Date.parse(entry.indexed_at),
              v: entry.tested,
            }))}
          />
          <QualityCard
            label={
              <MetricChip slug="documented" onOpen={onOpenWiki}>
                Documented exports
              </MetricChip>
            }
            value={`${(docShare * 100).toFixed(0)}%`}
            sub={`${totals.documented_exported.toLocaleString("en-US")} of ${totals.exported.toLocaleString("en-US")} exported`}
            tone={docShare < 0.3 ? "warn" : undefined}
          />
        </div>

        {/* ── Complexity, with its takeaway ─────────────────────── */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 mb-4">
          <div className="bg-card border border-border/50 rounded-md p-4">
            <p className="text-[12px] uppercase tracking-widest text-foreground/40 flex items-baseline justify-between gap-2">
              <span>Complexity mix</span>
              <span className="text-foreground/30 tabular-nums shrink-0">
                {totals.callables.toLocaleString("en-US")} callables
              </span>
            </p>
            {takeaway && <p className="text-[13px] text-foreground/60 mt-1">{takeaway}</p>}
            {/* TODO(W2b): real log-binned histogram behind a disclosure — needs per-bin API data */}
            <div className="flex items-end gap-1.5 h-[84px] mt-3">
              {metrics.complexity_hist.map((value, index) => {
                const max = Math.max(1, ...metrics.complexity_hist);
                return (
                  <div
                    key={index}
                    className="flex-1 flex flex-col items-center justify-end gap-1 min-w-0"
                  >
                    <span className="text-[12px] tabular-nums text-foreground/45">
                      {value.toLocaleString("en-US")}
                    </span>
                    <div
                      className={`w-full rounded-sm ${index >= CPLX_TAIL_START ? "bg-warn/60" : "bg-primary/35"}`}
                      style={{ height: `${Math.max(2, (value / max) * 40)}px` }}
                    />
                    <span className="text-[12px] text-foreground/35">
                      {CPLX_BINS[index]?.label}
                    </span>
                  </div>
                );
              })}
            </div>
            <p className="text-[12px] text-foreground/35 mt-1.5">
              <span className="inline-block w-2 h-2 rounded-sm bg-warn/60 mr-1" />
              &gt;{CPLX_TAIL_MIN} flagged
            </p>
          </div>
          <TopList
            title="Most churned files (1y)"
            entries={metrics.top_churn}
            unit="commits"
            total={metrics.churn_total_files}
          />
        </div>

        {/* ── Drill-down lists ──────────────────────────────────── */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <TopList
            title={
              <MetricChip slug="complexity" onOpen={onOpenWiki}>
                Most complex
              </MetricChip>
            }
            entries={metrics.top_complex}
            unit="cyclo"
            total={totals.callables}
            onOpenSymbol={onOpenSymbol}
          />
          <TopList
            title="Hardest to follow"
            entries={metrics.top_cognitive}
            unit="cog"
            total={totals.callables}
            onOpenSymbol={onOpenSymbol}
          />
          <TopList
            title="Longest"
            entries={metrics.top_long}
            unit="lines"
            total={totals.callables}
            onOpenSymbol={onOpenSymbol}
          />
        </div>

        {/* ── Inventory footer — facts, untoned ─────────────────── */}
        <p className="text-[12px] text-foreground/35 tabular-nums border-t border-border/40 pt-3 mt-5">
          {totals.callables.toLocaleString("en-US")} callables ·{" "}
          {totals.files.toLocaleString("en-US")} files ·{" "}
          {totals.similar_edges.toLocaleString("en-US")} near-clone edges ·{" "}
          {totals.missed_files.toLocaleString("en-US")} files not fully indexed · computed from
          index {metrics.generated_from}
          {history.length > 1 && ` · trends across ${history.length} index runs`}
        </p>
      </div>
    </ScrollArea>
  );
}
