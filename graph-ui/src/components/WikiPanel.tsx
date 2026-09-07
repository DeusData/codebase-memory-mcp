/* The metric wiki panel — a docked reader for one WikiEntry: the sentence,
 * why it earned its place, how it's computed (with named caps as warnings),
 * what it does not cover, and where it appears. Term chips inside the panel
 * push onto an internal navigation stack (back arrow pops, breadcrumb
 * jumps), so a reader can chase definitions without losing their place.
 * "Refused" entries render muted — they document what Atlas won't show. */
import { useEffect, useState } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { messages, useUiLanguage } from "../lib/i18n";
import { localizeEntry, wikiEntry } from "../wiki/entries";
import { MetricChip } from "./MetricChip";
import { TIER_TONE } from "./wikiTier";

interface WikiPanelProps {
  slug: string;
  onClose: () => void;
  onNavigate: (slug: string) => void;
}

function SectionHeading({ children }: { children: string }) {
  return (
    <p className="text-[12px] uppercase tracking-widest text-foreground/40 mb-1.5">
      {children}
    </p>
  );
}

export function WikiPanel({ slug, onClose, onNavigate }: WikiPanelProps) {
  const lang = useUiLanguage();
  const t = messages[lang];
  /* The navigation stack. The host owns the current slug; every push goes
   * through onNavigate and lands here via the prop effect, so chips outside
   * and inside the panel behave identically. */
  const [stack, setStack] = useState<string[]>([slug]);
  useEffect(() => {
    setStack((prev) => (prev[prev.length - 1] === slug ? prev : [...prev, slug]));
  }, [slug]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const back = () => {
    if (stack.length < 2) return;
    const next = stack.slice(0, -1);
    setStack(next);
    onNavigate(next[next.length - 1]);
  };
  const jumpTo = (index: number) => {
    const next = stack.slice(0, index + 1);
    setStack(next);
    onNavigate(next[next.length - 1]);
  };

  const entry = wikiEntry(slug);
  /* zh content with per-field English fallback; the term stays English. */
  const view = entry ? localizeEntry(entry, lang) : undefined;
  const refused = entry?.tier === "refused";

  return (
    <div className="fixed right-0 top-12 bottom-0 z-40 w-full sm:w-[400px] bg-card/95 border-l border-border/60 backdrop-blur-xl flex flex-col shadow-2xl">
      {/* Header: back + breadcrumb + close */}
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border/40 shrink-0">
        {stack.length > 1 && (
          <button
            onClick={back}
            className="text-foreground/45 hover:text-foreground/70 text-[14px] leading-none transition-colors shrink-0"
            title={t.wiki.back}
          >
            ←
          </button>
        )}
        <div className="flex items-baseline gap-1 min-w-0 flex-1 overflow-hidden">
          {stack.length > 1 &&
            stack.slice(0, -1).map((crumb, index) => (
              <span key={`${crumb}-${index}`} className="flex items-baseline gap-1 shrink-0">
                <button
                  onClick={() => jumpTo(index)}
                  className="text-[12px] text-foreground/40 hover:text-foreground/65 transition-colors"
                >
                  {wikiEntry(crumb)?.term ?? crumb}
                </button>
                <span className="text-[12px] text-foreground/25">›</span>
              </span>
            ))}
          <span className="text-[12px] font-semibold text-foreground/80 truncate">
            {entry?.term ?? slug}
          </span>
        </div>
        <button
          onClick={onClose}
          className="text-foreground/40 hover:text-foreground/60 text-[15px] leading-none transition-colors shrink-0"
          title={t.wiki.close}
        >
          ×
        </button>
      </div>

      {!entry || !view ? (
        <p className="text-[13px] text-foreground/40 px-4 py-6">
          {t.wiki.noEntry(slug)}
        </p>
      ) : (
        <ScrollArea className="flex-1 min-h-0">
          <div className="px-4 py-4">
            {/* Term + gloss + tier — the term is English-canonical; the zh
             * locale glosses it rather than replacing it. */}
            <div className="flex items-center gap-2.5 flex-wrap mb-2">
              <h3
                className={`text-[17px] font-semibold ${
                  refused ? "text-foreground/50" : "text-foreground/90"
                }`}
              >
                {entry.term}
              </h3>
              {view.gloss && (
                <span className="text-[13px] text-foreground/55">{view.gloss}</span>
              )}
              <span
                className={`px-2 py-0.5 rounded-md text-[12px] font-medium ${TIER_TONE[entry.tier]}`}
              >
                {entry.tier}
              </span>
            </div>

            {/* The one-sentence definition — always present. */}
            <p
              className={`text-[13.5px] leading-relaxed max-w-[60ch] ${
                refused ? "text-foreground/50" : "text-foreground/75"
              }`}
            >
              {view.sentence}
            </p>

            {view.why && (
              <div className="mt-4">
                <SectionHeading>{t.wiki.whyMatters}</SectionHeading>
                <p className="text-[13px] text-foreground/60 leading-relaxed max-w-[60ch]">
                  {view.why}
                </p>
              </div>
            )}

            {(view.computedParts || view.caps) && (
              <div className="mt-4">
                <SectionHeading>{t.wiki.howComputed}</SectionHeading>
                {(view.computedParts ?? []).map((part) => (
                  <p key={part} className="text-[13px] text-foreground/60 py-[2px]">
                    · {part}
                  </p>
                ))}
                {(view.caps ?? []).map((cap) => (
                  <p key={cap} className="text-[13px] text-amber-300/70 py-[2px]">
                    ⚠ {cap}
                  </p>
                ))}
              </div>
            )}

            {view.notCovered && (
              <div className="mt-4">
                <SectionHeading>{t.wiki.notCovered}</SectionHeading>
                <p className="text-[13px] text-foreground/60 leading-relaxed max-w-[60ch]">
                  {view.notCovered}
                </p>
              </div>
            )}

            {view.appearsIn.length > 0 && (
              <div className="mt-4">
                <SectionHeading>{t.wiki.whereAppears}</SectionHeading>
                {view.appearsIn.map((place) => (
                  <p key={place} className="text-[13px] text-foreground/60 py-[2px]">
                    {place}
                  </p>
                ))}
              </div>
            )}

            {/* Footer: id + paired metric + see-also chips */}
            <div className="border-t border-border/40 pt-3 mt-5 flex items-baseline gap-x-3 gap-y-1.5 flex-wrap">
              <span className="text-[12px] font-mono text-foreground/35">{entry.id}</span>
              {entry.pairedWith && (
                <span className="text-[13px] text-foreground/45">
                  {t.wiki.pairedWith}{" "}
                  <MetricChip slug={entry.pairedWith} onOpen={onNavigate} />
                </span>
              )}
              {(entry.seeAlso ?? []).map((other) => (
                <span key={other} className="text-[13px] text-foreground/45">
                  <MetricChip slug={other} onOpen={onNavigate} />
                </span>
              ))}
            </div>
          </div>
        </ScrollArea>
      )}
    </div>
  );
}
