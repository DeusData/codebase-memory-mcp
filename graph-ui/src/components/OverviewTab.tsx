/* Overview — a findings feed on a landmark map. Four opinionated slots
 * answer "is anything wrong?"; findings are sentences with evidence, a next
 * click and a dismissal; the region map gives newcomers named landmarks;
 * reference material (hubs, entry points, boundaries) is one disclosure
 * away instead of shouting; inventory lives in the footer. */
import { useEffect, useMemo, useState } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  fetchArchitecture,
  fetchBridges,
  archRows,
  type ArchitectureJson,
  type BridgeRow,
} from "../lib/atlas";
import { fetchRegions } from "../hooks/useGraphData";
import {
  disambiguateRegionNames,
  lensRegionsPayload,
  isBuiltinQn,
  localizeRegionWhy,
} from "../lib/regions";
import { surprisingCouplings, suggestedQuestions } from "../lib/firstread";
import { kneeCount, findingKey, isDismissed, dismiss } from "../lib/findings";
import {
  QUESTION_FAMILIES,
  localizeQuestion,
  questionStatusCounts,
  type QuestionStatus,
  type QuestionTab,
} from "../lib/questions";
import { messages, useUiLanguage, useUiMessages } from "../lib/i18n";
import { AddToPromptButton } from "./PromptBasket";
import type { RegionsPayload } from "../lib/types";

interface OverviewTabProps {
  project: string;
  onOpenRegion: (regionId: number) => void;
  onOpenSymbol: (qn: string) => void;
  onOpenModules: () => void;
  onOpenFlows: () => void;
  onOpenDashboard: () => void;
  onOpenTab: (tab: QuestionTab) => void;
}

interface HotspotRow {
  qn: string;
  fan_in: number;
}
interface EntryRow {
  qn: string;
  file: string;
}
interface BoundaryRow {
  from: string;
  to: string;
  calls: number;
}
interface LanguageRow {
  language: string;
  files: number;
}

interface MetricsLite {
  generated_from: string;
  totals: { dead: number; missed_files: number; callables: number };
  top_churn_complex: { file?: string; value: number; commits?: number }[];
  churn_available: boolean;
  history: { indexed_at: string; dead: number; avg_complexity: number }[];
}

function shortQn(qn: string): string {
  return qn.split(".").slice(-2).join(".");
}

function Slot({
  label,
  value,
  sub,
  tone,
  onClick,
}: {
  label: string;
  value: string;
  sub?: string;
  tone?: "warn" | "crit" | "quiet";
  onClick?: () => void;
}) {
  const inner = (
    <>
      <p className="text-[12px] uppercase tracking-widest text-foreground/40">{label}</p>
      <p
        className={`font-semibold tabular-nums mt-1 truncate ${
          tone === "crit"
            ? "text-crit text-[22px]"
            : tone === "warn"
              ? "text-warn text-[22px]"
              : tone === "quiet"
                ? "text-foreground/60 text-[15px] mt-2"
                : "text-foreground/90 text-[22px]"
        }`}
      >
        {value}
      </p>
      {sub && <p className="text-[12px] text-foreground/40 mt-0.5 truncate">{sub}</p>}
    </>
  );
  return onClick ? (
    <button
      onClick={onClick}
      className="text-left bg-card border border-border/50 rounded-md p-4 min-w-0 hover:bg-surface-3 hover:border-primary/30 transition-all"
    >
      {inner}
    </button>
  ) : (
    <div className="bg-card border border-border/50 rounded-md p-4 min-w-0">{inner}</div>
  );
}

/* Status glyph for the question index — the shape carries the state
 * (filled / half / hollow), the color is redundant with the word chip,
 * never color-only. */
function StatusGlyph({ status }: { status: QuestionStatus }) {
  const tone =
    status === "answers"
      ? "text-good"
      : status === "partial"
        ? "text-warn"
        : "text-foreground/40";
  return (
    <svg
      width="10"
      height="10"
      viewBox="0 0 10 10"
      className={`shrink-0 ${tone}`}
      aria-hidden="true"
    >
      {status === "answers" ? (
        <circle cx="5" cy="5" r="4" fill="currentColor" />
      ) : status === "partial" ? (
        <>
          <circle cx="5" cy="5" r="3.5" fill="none" stroke="currentColor" />
          <path d="M5 1.5 A3.5 3.5 0 0 0 5 8.5 Z" fill="currentColor" />
        </>
      ) : (
        <circle cx="5" cy="5" r="3.5" fill="none" stroke="currentColor" />
      )}
    </svg>
  );
}

/* "What can Atlas answer?" — the 18 developer-question families as an
 * honest scorecard: answered, partially answered, or not answered yet.
 * Collapsed by default; the header summary is derived from the data. */
function QuestionIndex({ onOpenTab }: { onOpenTab: (tab: QuestionTab) => void }) {
  const lang = useUiLanguage();
  const t = messages[lang];
  const [open, setOpen] = useState(() => {
    try {
      return localStorage.getItem("cbm-questions-open") === "1";
    } catch {
      return false;
    }
  });
  const toggle = () => {
    setOpen((value) => {
      try {
        localStorage.setItem("cbm-questions-open", value ? "0" : "1");
      } catch {
        /* ignore */
      }
      return !value;
    });
  };
  const counts = questionStatusCounts(QUESTION_FAMILIES);
  const statusWord: Record<QuestionStatus, string> = {
    answers: t.questions.answers,
    partial: t.questions.partial,
    lacks: t.questions.lacks,
  };
  const statusTone: Record<QuestionStatus, string> = {
    answers: "text-good/90",
    partial: "text-warn/90",
    lacks: "text-foreground/40",
  };

  return (
    <div className="bg-card border border-border/50 rounded-md mb-5">
      <button
        onClick={toggle}
        className="flex items-center gap-3 w-full text-left px-4 py-2.5 group"
      >
        <p className="text-[12px] uppercase tracking-widest text-foreground/40 group-hover:text-foreground/60 transition-colors">
          {t.questions.title}
        </p>
        <span className="text-[12px] text-foreground/35 tabular-nums truncate ml-auto shrink-1">
          {t.questions.summary(counts.answers, counts.partial, counts.lacks)}
        </span>
        <span className="text-[11px] text-foreground/40 shrink-0">
          {open ? "▾" : "▸"}
        </span>
      </button>
      {open && (
        <div className="border-t border-border/30 py-1">
          {QUESTION_FAMILIES.map((family) => {
            const tab = family.tab;
            const loc = localizeQuestion(family, lang);
            const inner = (clickable: boolean) => (
              <>
                <span className="flex items-center gap-1.5 w-[76px] shrink-0 pt-[2px]">
                  <StatusGlyph status={family.status} />
                  <span className={`text-[11px] ${statusTone[family.status]}`}>
                    {statusWord[family.status]}
                  </span>
                </span>
                <span className="flex-1 min-w-0">
                  <span
                    className={`block text-[13px] text-foreground/85 ${
                      clickable
                        ? "group-hover/question:text-primary transition-colors"
                        : ""
                    }`}
                  >
                    {loc.question}
                  </span>
                  {(loc.hint || loc.gap) && (
                    <span className="block text-[12px] text-foreground/40 mt-0.5">
                      {loc.hint}
                      {loc.hint && loc.gap && " · "}
                      {loc.gap && `${t.questions.missingPrefix} ${loc.gap}`}
                    </span>
                  )}
                </span>
              </>
            );
            /* Only rows with a destination are buttons — no fake affordance;
             * for the rest the hint carries the "how". */
            return tab ? (
              <button
                key={family.id}
                onClick={() => onOpenTab(tab)}
                className="flex items-start gap-3 w-full text-left px-4 py-2 hover:bg-surface-3 transition-colors group/question"
              >
                {inner(true)}
              </button>
            ) : (
              <div key={family.id} className="flex items-start gap-3 px-4 py-2">
                {inner(false)}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

/* All displayed regions' cohesion values as an inline dot strip — a bare
 * value invites misreading (0.4 can be the tightest region on the map or
 * the loosest). This region's dot is emphasized; the tick is the median. */
function CohesionStrip({ values, own }: { values: number[]; own: number }) {
  const t = useUiMessages();
  if (values.length < 2) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  const median =
    sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  const x = (value: number) => 4 + Math.max(0, Math.min(1, value)) * 112;
  return (
    <svg
      width="120"
      height="14"
      viewBox="0 0 120 14"
      className="shrink-0"
      role="img"
      aria-label={t.overview.cohesionAria(own, values.length, median)}
    >
      <title>{`p50 = ${median.toFixed(2)}`}</title>
      <line
        x1={x(median)}
        y1="2"
        x2={x(median)}
        y2="12"
        stroke="currentColor"
        strokeOpacity="0.35"
        className="text-foreground"
      />
      {values.map((value, index) => (
        <circle
          key={index}
          cx={x(value)}
          cy="7"
          r="1.5"
          fill="currentColor"
          fillOpacity="0.25"
          className="text-foreground"
        />
      ))}
      <circle cx={x(own)} cy="7" r="2.5" fill="currentColor" className="text-primary" />
    </svg>
  );
}

export function OverviewTab({
  project,
  onOpenRegion,
  onOpenSymbol,
  onOpenModules,
  onOpenFlows,
  onOpenDashboard,
  onOpenTab,
}: OverviewTabProps) {
  const t = useUiMessages();
  const [arch, setArch] = useState<ArchitectureJson | null>(null);
  const [regions, setRegions] = useState<RegionsPayload | null>(null);
  const [metrics, setMetrics] = useState<MetricsLite | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [includeTests, setIncludeTests] = useState(false);
  const [dismissedTick, setDismissedTick] = useState(0);
  const [bridges, setBridges] = useState<BridgeRow[]>([]);

  /* Boundary spanners ride alongside the main load; best-effort, silent on
   * failure. (A true staleness banner needs a files-changed-since-indexing
   * signal the server does not expose yet — recorded as a follow-up rather
   * than shipped with wrong semantics.) */
  useEffect(() => {
    let cancelled = false;
    setBridges([]);
    fetchBridges(project)
      .then((payload) => {
        if (!cancelled) setBridges(payload.bridges ?? []);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [project]);

  useEffect(() => {
    let cancelled = false;
    setArch(null);
    setRegions(null);
    setMetrics(null);
    setError(null);
    Promise.allSettled([
      fetchArchitecture(project),
      fetchRegions(project),
      fetch(`/api/metrics?${new URLSearchParams({ project })}`).then((res) =>
        res.ok ? res.json() : Promise.reject(new Error(String(res.status))),
      ),
    ]).then(([archResult, regionsResult, metricsResult]) => {
      if (cancelled) return;
      if (archResult.status === "fulfilled") setArch(archResult.value);
      if (regionsResult.status === "fulfilled") setRegions(regionsResult.value);
      if (metricsResult.status === "fulfilled")
        setMetrics(metricsResult.value as MetricsLite);
      if (archResult.status === "rejected" && regionsResult.status === "rejected")
        setError(String(archResult.reason?.message ?? "failed to load"));
    });
    return () => {
      cancelled = true;
    };
  }, [project]);

  const lensed = useMemo(
    () => (regions ? lensRegionsPayload(regions, includeTests) : null),
    [regions, includeTests],
  );
  const displayRegions = useMemo(
    () => (lensed ? disambiguateRegionNames(lensed.regions) : []),
    [lensed],
  );
  const hiddenTestRegions =
    regions && lensed ? regions.regions.length - lensed.regions.length : 0;

  const couplings = useMemo(
    () => (lensed ? surprisingCouplings(lensed, 5, t.firstread) : []),
    [lensed, t],
  );
  const questions = useMemo(
    () =>
      lensed
        ? suggestedQuestions(lensed, { deadCount: metrics?.totals.dead }, 4, t.firstread)
        : [],
    [lensed, metrics, t],
  );

  /* Findings with dismissal (magnitude-bucketed keys re-alert on jumps). */
  const findings = useMemo(() => {
    void dismissedTick;
    return couplings
      .map((coupling) => ({
        coupling,
        key: findingKey(
          "coupling",
          `${coupling.source.name}⇄${coupling.target.name}`,
          coupling.weight,
        ),
      }))
      .filter((finding) => !isDismissed(finding.key));
  }, [couplings, dismissedTick]);
  const dismissedCount = couplings.length - findings.length;

  const hotspots = useMemo(
    () => archRows<HotspotRow>(arch, "hotspots").filter((row) => !isBuiltinQn(row.qn)),
    [arch],
  );
  const entries = useMemo(() => archRows<EntryRow>(arch, "entry_points"), [arch]);
  const boundaries = useMemo(() => archRows<BoundaryRow>(arch, "boundaries"), [arch]);
  const languages = useMemo(() => archRows<LanguageRow>(arch, "languages"), [arch]);
  const totalNodes =
    typeof arch?.total_nodes === "number" ? arch.total_nodes : regions?.total_nodes;
  const totalEdges = typeof arch?.total_edges === "number" ? arch.total_edges : undefined;

  /* The four slots. */
  const attention = useMemo(() => {
    const scores = (metrics?.top_churn_complex ?? []).map((entry) => entry.value);
    if (scores.length === 0) return null;
    return kneeCount(scores);
  }, [metrics]);
  const riskiest = metrics?.top_churn_complex?.[0] ?? null;
  const direction = useMemo(() => {
    const history = metrics?.history ?? [];
    if (history.length < 2) return null;
    const previous = history[history.length - 2];
    const current = history[history.length - 1];
    const deadDelta = current.dead - previous.dead;
    const cplxDelta = current.avg_complexity - previous.avg_complexity;
    const deadPart =
      deadDelta === 0 ? "dead →" : `dead ${deadDelta > 0 ? "+" : ""}${deadDelta}`;
    const cplxPart =
      Math.abs(cplxDelta) < 0.05 ? "complexity →" : `complexity ${cplxDelta > 0 ? "↑" : "↓"}`;
    return {
      text: `${deadPart} · ${cplxPart}`,
      worsening: deadDelta > 0 || cplxDelta > 0.05,
    };
  }, [metrics]);

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-crit/90 text-sm">{error}</p>
      </div>
    );
  }
  if (!arch && !regions) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-foreground/45 text-sm">{t.overview.loading}</p>
      </div>
    );
  }

  return (
    <ScrollArea className="h-full">
      <div className="max-w-[1200px] mx-auto px-6 py-6">
        <div className="flex justify-end mb-2">
          <a
            href={`/api/handout?${new URLSearchParams({ project })}`}
            target="_blank"
            rel="noreferrer"
            className="text-[12px] text-foreground/45 hover:text-primary transition-colors"
            title={t.overview.handoutTitle}
          >
            {t.overview.handout}
          </a>
        </div>

        {/* The scorecard: what can Atlas answer? */}
        <QuestionIndex onOpenTab={onOpenTab} />

        {/* The four slots: is anything wrong? */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-5">
          <Slot
            label={t.overview.needsAttention}
            value={
              attention === null
                ? metrics?.churn_available === false
                  ? "—"
                  : "…"
                : t.overview.filesCount(attention)
            }
            sub={
              attention === null
                ? t.overview.churnUnavailable
                : t.overview.concentrateRisk
            }
            tone={attention !== null && attention > 0 ? "warn" : undefined}
            onClick={onOpenDashboard}
          />
          <Slot
            label={t.overview.direction}
            value={direction ? direction.text : t.overview.firstIndex}
            sub={direction ? t.overview.sincePrevIndex : t.overview.trendsAfterReindex}
            tone={direction?.worsening ? "warn" : "quiet"}
          />
          <Slot
            label={t.overview.riskiestArea}
            value={riskiest?.file?.split("/").slice(-1)[0] ?? "—"}
            sub={
              riskiest
                ? t.overview.riskiestSub(riskiest.commits ?? "?", riskiest.file ?? "")
                : t.overview.noComplexChurn
            }
            tone={riskiest ? "crit" : undefined}
            onClick={onOpenDashboard}
          />
          <Slot
            label={t.overview.trust}
            value={t.overview.filesMissed(metrics?.totals.missed_files ?? 0)}
            sub={t.overview.trustSub(
              metrics?.generated_from?.slice(0, 10) ?? "…",
              regions?.unmapped_nodes ?? 0,
            )}
            tone="quiet"
          />
        </div>

        {/* Findings — sentences with evidence, a next click, a dismissal. */}
        {(findings.length > 0 || dismissedCount > 0) && (
          <div className="mb-5">
            <div className="flex items-baseline gap-3 mb-2">
              <p className="text-[12px] uppercase tracking-widest text-foreground/40">
                {t.overview.findings}
              </p>
              {dismissedCount > 0 && (
                <span className="text-[12px] text-foreground/30">
                  {t.overview.dismissed(dismissedCount)}
                </span>
              )}
            </div>
            <div className="space-y-1.5">
              {findings.map(({ coupling, key }) => (
                <div
                  key={key}
                  className="flex items-start gap-3 bg-card border border-border/50 rounded-md px-4 py-2.5"
                >
                  <div className="flex-1 min-w-0">
                    <p className="text-[14px] text-foreground/85">
                      <span className="font-medium">{coupling.source.name}</span>
                      <span className="text-foreground/40">{t.overview.couplingAnd}</span>
                      <span className="font-medium">{coupling.target.name}</span>
                      <span className="text-foreground/40">{t.overview.couplingSuffix}</span>
                    </p>
                    <p className="text-[12px] text-foreground/45 mt-0.5">
                      {coupling.reasons.join(" · ")}
                    </p>
                  </div>
                  <button
                    onClick={() => onOpenRegion(coupling.source.id)}
                    className="text-[12px] text-primary/80 hover:text-primary shrink-0 transition-colors"
                  >
                    {t.overview.inspect}
                  </button>
                  <button
                    onClick={() => {
                      dismiss(key);
                      setDismissedTick((tick) => tick + 1);
                    }}
                    className="text-[12px] text-foreground/30 hover:text-foreground/60 shrink-0 transition-colors"
                    title={t.overview.intendedTitle}
                  >
                    {t.overview.intended}
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Questions to hand your agent. */}
        {questions.length > 0 && (
          <div className="mb-5">
            <p className="text-[12px] uppercase tracking-widest text-foreground/40 mb-2">
              {t.overview.questionsHeading}
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
              {questions.map((question) => (
                <div
                  key={question.question}
                  className="flex items-start gap-2 bg-card border border-border/50 rounded-md px-3.5 py-2.5"
                >
                  <div className="flex-1 min-w-0">
                    <p className="text-[13px] text-foreground/80">{question.question}</p>
                    <p className="text-[12px] text-foreground/40 mt-0.5">{question.why}</p>
                  </div>
                  <AddToPromptButton
                    small
                    item={{
                      kind: "question",
                      question: question.question,
                      why: question.why,
                    }}
                  />
                </div>
              ))}
            </div>
          </div>
        )}

        {/* The landmark map. */}
        <div className="mb-5">
          <div className="flex items-baseline justify-between mb-2">
            <p className="text-[12px] uppercase tracking-widest text-foreground/40">
              {t.overview.regionsHeading(regions?.method ?? "…")}
            </p>
            {hiddenTestRegions > 0 && (
              <button
                onClick={() => setIncludeTests((value) => !value)}
                className="text-[12px] text-foreground/40 hover:text-foreground/70 transition-colors"
              >
                {includeTests
                  ? t.overview.hideTestCode
                  : t.overview.testRegionsHidden(hiddenTestRegions)}
              </button>
            )}
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
            {displayRegions.slice(0, 12).map((region) => (
              <button
                key={region.id}
                onClick={() => onOpenRegion(region.id)}
                className="text-left rounded-md border border-border/40 bg-card hover:bg-surface-3 hover:border-primary/30 p-3 transition-all group"
              >
                <div className="flex items-center gap-2 mb-1">
                  <span
                    className="w-2 h-2 rounded-full shrink-0"
                    style={{ backgroundColor: region.color }}
                  />
                  <span className="text-[13px] font-medium text-foreground/85 truncate group-hover:text-primary transition-colors">
                    {region.name}
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <p className="text-[12px] text-foreground/45 truncate tabular-nums flex-1 min-w-0">
                    {t.overview.regionMeta(region.members, region.cohesion)}
                  </p>
                  <CohesionStrip
                    values={displayRegions.map((r) => r.cohesion)}
                    own={region.cohesion}
                  />
                </div>
                {region.why && (
                  <p className="text-[12px] text-foreground/35 mt-1 line-clamp-2">
                    {localizeRegionWhy(region.why, t.regions)}
                  </p>
                )}
              </button>
            ))}
          </div>
          {displayRegions.length > 12 && (
            <button
              onClick={onOpenModules}
              className="mt-2 text-[13px] text-primary/80 hover:text-primary transition-colors"
            >
              {t.overview.allRegions(displayRegions.length)}
            </button>
          )}
        </div>

        {/* Boundary spanners — the seam-stitching metric, distinct from
         * degree hubs (which find utilities). */}
        {bridges.length > 0 && (
          <div className="bg-card border border-border/40 rounded-md p-4 mb-5">
            <p className="text-[12px] uppercase tracking-widest text-foreground/40 mb-2">
              {t.overview.bridgesHeading}
            </p>
            {bridges.map((bridge) => (
              <div key={bridge.id} className="flex items-center gap-2 py-[3px]">
                <button
                  onClick={() => bridge.qualified_name && onOpenSymbol(bridge.qualified_name)}
                  className="text-[13px] font-mono text-foreground/65 hover:text-primary truncate text-left transition-colors"
                  title={bridge.file_path}
                >
                  {bridge.name}
                </button>
                <span className="text-[12px] text-foreground/40 tabular-nums ml-auto shrink-0">
                  {t.overview.bridgeMeta(bridge.regions, bridge.cross_calls)}
                </span>
              </div>
            ))}
            <p className="text-[12px] text-foreground/35 mt-1.5">
              {t.overview.bridgesFootnote}
            </p>
          </div>
        )}

        {/* Reference — one disclosure away, never shouting. */}
        <details className="mb-5">
          <summary className="cursor-pointer text-[12px] uppercase tracking-widest text-foreground/40 hover:text-foreground/60 transition-colors select-none">
            {t.overview.referenceSummary}
          </summary>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-3">
            <div className="bg-card border border-border/40 rounded-md p-4">
              <p className="text-[12px] uppercase tracking-widest text-foreground/40 mb-2">
                {t.overview.hubsHeading}
              </p>
              {hotspots.slice(0, 8).map((hotspot) => (
                <div key={hotspot.qn} className="flex items-center gap-2 py-[3px]">
                  <button
                    onClick={() => onOpenSymbol(hotspot.qn)}
                    className="text-[13px] font-mono text-foreground/65 hover:text-primary truncate flex-1 text-left transition-colors"
                    title={hotspot.qn}
                  >
                    {shortQn(hotspot.qn)}
                  </button>
                  <span className="text-[12px] tabular-nums text-foreground/35 shrink-0">
                    {Number(hotspot.fan_in).toLocaleString("en-US")}
                  </span>
                </div>
              ))}
              {hotspots.length === 0 && (
                <p className="text-[13px] text-foreground/40">{t.overview.noHotspots}</p>
              )}
            </div>
            <div className="bg-card border border-border/40 rounded-md p-4">
              <p className="text-[12px] uppercase tracking-widest text-foreground/40 mb-2">
                {t.overview.entryPoints}
              </p>
              {entries.slice(0, 8).map((entry) => (
                <button
                  key={entry.qn}
                  onClick={() => onOpenSymbol(entry.qn)}
                  className="flex items-center gap-2 w-full text-left py-[3px] group/entry"
                  title={entry.qn}
                >
                  <span className="text-[13px] font-mono text-foreground/65 group-hover/entry:text-primary truncate transition-colors">
                    {shortQn(entry.qn)}
                  </span>
                  <span className="text-[12px] text-foreground/30 truncate ml-auto shrink-0 max-w-[45%]">
                    {entry.file}
                  </span>
                </button>
              ))}
              <button
                onClick={onOpenFlows}
                className="mt-2 text-[12px] text-primary/80 hover:text-primary transition-colors"
              >
                {t.overview.followFlows}
              </button>
            </div>
            <div className="bg-card border border-border/40 rounded-md p-4 md:col-span-2 overflow-x-auto">
              <p className="text-[12px] uppercase tracking-widest text-foreground/40 mb-2">
                {t.overview.boundariesHeading}
              </p>
              <table className="w-full text-[13px]">
                <tbody>
                  {boundaries.slice(0, 8).map((boundary, index) => (
                    <tr key={index} className="border-t border-border/30 first:border-t-0">
                      <td className="py-1 pr-4 font-mono text-foreground/60">
                        {boundary.from}
                      </td>
                      <td className="py-1 pr-4 font-mono text-foreground/60">{boundary.to}</td>
                      <td className="py-1 text-right tabular-nums text-foreground/40">
                        {Number(boundary.calls).toLocaleString("en-US")}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {boundaries.length === 0 && (
                <p className="text-[13px] text-foreground/40">{t.overview.noBoundaries}</p>
              )}
            </div>
          </div>
        </details>

        {/* Inventory footer — facts, untoned. */}
        <p className="text-[12px] text-foreground/35 tabular-nums border-t border-border/40 pt-3">
          {t.overview.inventory(
            totalNodes?.toLocaleString("en-US") ?? "…",
            totalEdges?.toLocaleString("en-US") ?? "…",
            `${regions?.regions.length ?? "…"}`,
          )}{" "}
          ·{" "}
          {languages
            .slice(0, 4)
            .map((lang) => lang.language)
            .join(" · ") || "…"}
        </p>
      </div>
    </ScrollArea>
  );
}
