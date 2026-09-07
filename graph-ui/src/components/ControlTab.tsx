import { useState, useEffect, useCallback } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { ProcessInfo } from "../lib/types";
import { useUiMessages } from "../lib/i18n";

/* ── Gauge component ────────────────────────────────────── */

function Gauge({ label, value, max, unit, color }: {
  label: string; value: number; max: number; unit: string; color: string;
}) {
  const pct = Math.min(100, (value / max) * 100);
  /* The bands are stated in the legend and glyphed beside the value, so the
   * bar's color is redundant, not the only carrier (WCAG 1.4.1). */
  const band = pct > 80 ? "crit" : pct > 50 ? "warn" : null;
  const bandColor = band === "crit" ? "#e05252" : "#eab308";
  return (
    <div className="flex-1 rounded-md border border-border/30 bg-card p-4">
      <p className="text-[12px] text-foreground/40 uppercase tracking-widest mb-2">{label}</p>
      <p className={`text-[20px] font-semibold tabular-nums ${color}`}>
        {value.toFixed(1)}<span className="text-[13px] text-foreground/45 ml-1">{unit}</span>
        {band && (
          <span
            className="text-[12px] ml-1.5 align-middle"
            style={{ color: bandColor }}
            title={`above ${(max * (band === "crit" ? 0.8 : 0.5)).toFixed(0)}${unit}`}
          >
            {band === "crit" ? "▲▲" : "▲"}
          </span>
        )}
      </p>
      <div className="mt-2 h-1.5 rounded-full bg-surface-3 overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{ width: `${pct}%`, backgroundColor: band === "crit" ? "#e05252" : band === "warn" ? "#eab308" : "#1DA27E" }}
        />
      </div>
      <p className="mt-1.5 text-[12px] text-foreground/30 tabular-nums">
        ▲ &gt;{(max * 0.5).toFixed(0)}{unit} · ▲▲ &gt;{(max * 0.8).toFixed(0)}{unit}
      </p>
    </div>
  );
}

/* ── Process card ───────────────────────────────────────── */

function ProcessCard({ proc, selected, onSelect }: {
  proc: ProcessInfo; selected: boolean;
  onSelect: () => void;
}) {
  const t = useUiMessages();
  return (
    <button
      onClick={onSelect}
      className={`w-full text-left rounded-md border p-4 transition-all ${
        selected
          ? "border-primary/40 bg-primary/5"
          : "border-border/30 bg-card hover:bg-surface-3"
      }`}
    >
      <div className="flex items-start justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className={`w-2 h-2 rounded-full ${proc.is_self ? "bg-primary animate-pulse" : "bg-emerald-400"}`} />
          <span className="text-[12px] font-semibold text-foreground/80">
            PID {proc.pid}
          </span>
          {proc.is_self && (
            <span className="text-[12px] px-1.5 py-0.5 rounded bg-primary/15 text-primary font-medium">{t.control.thisProcess}</span>
          )}
        </div>
      </div>

      <div className="grid grid-cols-3 gap-3 mb-2">
        <div>
          <p className="text-[12px] text-foreground/35 uppercase">CPU</p>
          <p className="text-[13px] font-semibold tabular-nums text-foreground/70">{proc.cpu.toFixed(1)}%</p>
        </div>
        <div>
          <p className="text-[12px] text-foreground/35 uppercase">RAM</p>
          <p className="text-[13px] font-semibold tabular-nums text-foreground/70">{proc.rss_mb.toFixed(0)} MB</p>
        </div>
        <div>
          <p className="text-[12px] text-foreground/35 uppercase">{t.control.uptime}</p>
          <p className="text-[13px] font-semibold tabular-nums text-foreground/70">{proc.elapsed}</p>
        </div>
      </div>

      <p className="text-[12px] text-foreground/30 font-mono truncate">{proc.command}</p>
    </button>
  );
}

/* ── Log viewer ─────────────────────────────────────────── */

function LogViewer() {
  const t = useUiMessages();
  const [lines, setLines] = useState<string[]>([]);

  useEffect(() => {
    const poll = setInterval(async () => {
      try {
        const res = await fetch("/api/logs?lines=200");
        const data = await res.json();
        setLines(data.lines ?? []);
      } catch { /* ignore */ }
    }, 2000);
    /* Initial fetch */
    fetch("/api/logs?lines=200").then(r => r.json()).then(d => setLines(d.lines ?? [])).catch(() => {});
    return () => clearInterval(poll);
  }, []);

  return (
    <div className="rounded-md border border-border/30 bg-black/30 overflow-hidden">
      <div className="px-4 py-2 border-b border-border/20">
        <span className="text-[13px] font-medium text-foreground/40">{t.control.processLogs}</span>
        <span className="text-[12px] text-foreground/30 ml-2">{lines.length} lines</span>
      </div>
      <ScrollArea className="h-[400px]">
        <div className="p-3 font-mono text-[12px] leading-relaxed">
          {lines.length === 0 ? (
            <p className="text-foreground/30 text-center py-8">{t.control.noLogs}</p>
          ) : (
            lines.map((line, i) => {
              const isErr = line.includes("level=error");
              const isWarn = line.includes("level=warn");
              return (
                <div
                  key={i}
                  className={`py-[1px] ${
                    isErr ? "text-red-400/70" : isWarn ? "text-yellow-400/60" : "text-foreground/45"
                  }`}
                >
                  {line}
                </div>
              );
            })
          )}
        </div>
      </ScrollArea>
    </div>
  );
}

/* ── Main Control Tab ───────────────────────────────────── */

export function ControlTab() {
  const t = useUiMessages();
  const [processes, setProcesses] = useState<ProcessInfo[]>([]);
  const [selfMetrics, setSelfMetrics] = useState({ rss_mb: 0, user_cpu: 0, sys_cpu: 0 });
  const [selectedPid, setSelectedPid] = useState<number | null>(null);

  const fetchProcesses = useCallback(async () => {
    try {
      const res = await fetch("/api/processes");
      const data = await res.json();
      setProcesses(data.processes ?? []);
      setSelfMetrics({
        rss_mb: data.self_rss_mb ?? 0,
        user_cpu: data.self_user_cpu_s ?? 0,
        sys_cpu: data.self_sys_cpu_s ?? 0,
      });
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    fetchProcesses();
    const interval = setInterval(fetchProcesses, 3000);
    return () => clearInterval(interval);
  }, [fetchProcesses]);

  /* Aggregates */
  const totalCpu = processes.reduce((s, p) => s + p.cpu, 0);
  const totalRam = processes.reduce((s, p) => s + p.rss_mb, 0);

  return (
    <ScrollArea className="h-full">
      <div className="p-8 max-w-4xl mx-auto">
        <h2 className="text-[15px] font-semibold text-foreground/80 mb-6">{t.control.panel}</h2>

        {/* Aggregate gauges */}
        <div className="flex gap-4 mb-8">
          <Gauge label={t.control.totalCpu} value={totalCpu} max={100 * processes.length || 100} unit="%" color="text-foreground/80" />
          <Gauge label={t.control.totalRam} value={totalRam} max={4096} unit="MB" color="text-foreground/80" />
          <Gauge label={t.control.processes} value={processes.length} max={10} unit="" color="text-primary" />
          <Gauge label={t.control.selfRam} value={selfMetrics.rss_mb} max={2048} unit="MB" color="text-primary" />
        </div>

        {/* Process grid */}
        <div className="mb-8">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-[13px] font-medium text-foreground/50">
              {t.control.activeProcesses}
            </h3>
            <button
              onClick={fetchProcesses}
              className="text-[13px] text-primary/60 hover:text-primary transition-colors"
            >
              {t.common.refresh}
            </button>
          </div>

          {processes.length === 0 ? (
            <p className="text-foreground/35 text-[12px] text-center py-8">{t.control.noProcesses}</p>
          ) : (
            <div className="grid grid-cols-2 gap-3">
              {processes.map((p) => (
                <ProcessCard
                  key={p.pid}
                  proc={p}
                  selected={selectedPid === p.pid}
                  onSelect={() => setSelectedPid(selectedPid === p.pid ? null : p.pid)}
                />
              ))}
            </div>
          )}
        </div>

        {/* Log viewer */}
        <LogViewer />
      </div>
    </ScrollArea>
  );
}
