/* ⌘K — one box, two engines: symbol names (search_graph) and code text
 * (search_code), grouped, keyboard-first. Enter opens, Esc closes. */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { callTool } from "../api/rpc";
import { searchGraph, type SearchRow } from "../lib/atlas";

interface CommandKProps {
  project: string | null;
  open: boolean;
  onClose: () => void;
  onOpenSymbol: (ref: { qn: string }) => void;
}

interface TextHit {
  file: string;
  line: number;
  text: string;
}

/* search_code(format:"json") shape is tool-versioned; decode defensively. */
function decodeTextHits(payload: unknown): TextHit[] {
  const out: TextHit[] = [];
  const root = payload as {
    matches?: { file?: string; line?: number; text?: string }[];
    groups?: { file?: string; rows?: (string | number | null)[][] }[];
  };
  if (Array.isArray(root.matches)) {
    for (const match of root.matches)
      if (match.file)
        out.push({ file: match.file, line: match.line ?? 0, text: match.text ?? "" });
  } else if (Array.isArray(root.groups)) {
    for (const group of root.groups)
      for (const row of group.rows ?? [])
        out.push({
          file: group.file ?? "?",
          line: Number(row[0] ?? 0),
          text: String(row[1] ?? ""),
        });
  }
  return out.slice(0, 12);
}

export function CommandK({ project, open, onClose, onOpenSymbol }: CommandKProps) {
  const [query, setQuery] = useState("");
  const [symbols, setSymbols] = useState<SearchRow[]>([]);
  const [textHits, setTextHits] = useState<TextHit[]>([]);
  const [active, setActive] = useState(0);
  const [busy, setBusy] = useState(false);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const seqRef = useRef(0);

  useEffect(() => {
    if (open) {
      setQuery("");
      setSymbols([]);
      setTextHits([]);
      setActive(0);
      setTimeout(() => inputRef.current?.focus(), 30);
    }
  }, [open]);

  useEffect(() => {
    if (!open || !project || query.trim().length < 2) {
      setSymbols([]);
      setTextHits([]);
      return;
    }
    const seq = ++seqRef.current;
    const timer = setTimeout(async () => {
      setBusy(true);
      const escaped = query.trim().replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const [nameResult, textResult] = await Promise.allSettled([
        searchGraph(project, { name_pattern: escaped, limit: 12 }),
        callTool("search_code", { project, query: query.trim(), limit: 12, format: "json" }),
      ]);
      if (seq !== seqRef.current) return;
      setSymbols(nameResult.status === "fulfilled" ? nameResult.value.rows : []);
      setTextHits(textResult.status === "fulfilled" ? decodeTextHits(textResult.value) : []);
      setActive(0);
      setBusy(false);
    }, 220);
    return () => clearTimeout(timer);
  }, [open, project, query]);

  const entries = useMemo(
    () =>
      [
        ...symbols.map((row) => ({ kind: "symbol" as const, row })),
        ...textHits.map((hit) => ({ kind: "text" as const, hit })),
      ],
    [symbols, textHits],
  );

  const activate = useCallback(
    (index: number) => {
      const entry = entries[index];
      if (!entry) return;
      if (entry.kind === "symbol") {
        onOpenSymbol({ qn: entry.row.qualified_name });
        onClose();
      }
      /* Text hits have no symbol identity; they stay informational. */
    },
    [entries, onOpenSymbol, onClose],
  );

  useEffect(() => {
    if (!open) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
      else if (event.key === "ArrowDown") {
        event.preventDefault();
        setActive((value) => Math.min(value + 1, entries.length - 1));
      } else if (event.key === "ArrowUp") {
        event.preventDefault();
        setActive((value) => Math.max(value - 1, 0));
      } else if (event.key === "Enter") {
        event.preventDefault();
        activate(active);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, entries.length, active, activate, onClose]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 bg-black/50 backdrop-blur-sm flex items-start justify-center pt-[12vh]"
      onClick={onClose}
    >
      <div
        className="w-[620px] max-w-[92vw] bg-card border border-border/60 rounded-md shadow-2xl overflow-hidden"
        onClick={(event) => event.stopPropagation()}
      >
        <input
          ref={inputRef}
          type="text"
          placeholder={project ? "Search symbols and code…" : "Select a project first"}
          disabled={!project}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          className="w-full bg-transparent px-4 py-3 text-[14px] text-foreground placeholder-foreground/25 outline-none border-b border-border/40"
        />
        <div className="max-h-[50vh] overflow-y-auto py-1">
          {busy && entries.length === 0 && (
            <p className="text-[13px] text-foreground/40 px-4 py-3">Searching…</p>
          )}
          {!busy && query.trim().length >= 2 && entries.length === 0 && (
            <p className="text-[13px] text-foreground/40 px-4 py-3">No matches.</p>
          )}
          {symbols.length > 0 && (
            <p className="px-4 pt-2 pb-1 text-[12px] uppercase tracking-widest text-foreground/40">
              Symbols
            </p>
          )}
          {symbols.map((row, index) => (
            <button
              key={row.qualified_name}
              onClick={() => activate(index)}
              onMouseEnter={() => setActive(index)}
              className={`flex items-center gap-2 w-full text-left px-4 py-[6px] transition-colors ${
                active === index ? "bg-primary/10" : ""
              }`}
            >
              <span className="text-[12px] font-mono text-foreground/75 truncate">{row.name}</span>
              <span className="text-[12px] text-foreground/45 shrink-0">{row.label}</span>
              <span className="text-[12px] text-foreground/35 truncate ml-auto max-w-[45%]">
                {row.file}
              </span>
            </button>
          ))}
          {textHits.length > 0 && (
            <p className="px-4 pt-2 pb-1 text-[12px] uppercase tracking-widest text-foreground/40">
              In code
            </p>
          )}
          {textHits.map((hit, index) => (
            <div
              key={`${hit.file}:${hit.line}:${index}`}
              className={`px-4 py-[5px] ${active === symbols.length + index ? "bg-primary/10" : ""}`}
            >
              <p className="text-[12px] font-mono text-foreground/50 truncate">
                {hit.file}:{hit.line}
              </p>
              <p className="text-[13px] font-mono text-foreground/60 truncate">{hit.text}</p>
            </div>
          ))}
        </div>
        <div className="px-4 py-2 border-t border-border/40 text-[12px] text-foreground/40">
          ↑↓ navigate · Enter opens the symbol · Esc closes
        </div>
      </div>
    </div>
  );
}
