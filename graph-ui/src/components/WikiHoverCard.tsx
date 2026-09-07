/* Hover preview of a wiki entry — the single-source sentence surfaced at the
 * point of curiosity, before the click. Appears after a dwell on the anchor
 * chip (configurable in Display settings) and follows WCAG 1.4.13:
 * dismissible (Esc closes just the card, not the panel underneath),
 * hoverable (a short grace lets the pointer travel into the card),
 * persistent (no auto-timeout while hovered or focused).
 *
 * The card is portaled to <body> — the same fix ProjectSwitcher needed: the
 * header's backdrop-filter makes it a stacking context, so anything left
 * inside paints as one atomic layer and gets dimmed by body-level overlays. */
import {
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { loadTooltipDelayMs } from "../lib/density";
import { messages, useUiLanguage } from "../lib/i18n";
import { localizeEntry, wikiEntry } from "../wiki/entries";
import { TIER_TONE } from "./wikiTier";

/* Leaving the chip closes after this grace — long enough to reach the card,
 * short enough that the card never lingers over unrelated content. */
const HOVER_GRACE_MS = 150;
const ANCHOR_GAP = 6;
const VIEWPORT_PAD = 8;

interface WikiHoverCardProps {
  slug: string;
  /* The chip the card anchors to; the component wires its own hover/focus
   * listeners so call sites stay one-line. */
  anchorRef: React.RefObject<HTMLElement | null>;
  /* Dwell before showing; defaults to the persisted display setting. */
  delayMs?: number;
}

export function WikiHoverCard({ slug, anchorRef, delayMs }: WikiHoverCardProps) {
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);
  const cardRef = useRef<HTMLDivElement | null>(null);
  const showTimer = useRef<number | null>(null);
  const hideTimer = useRef<number | null>(null);
  const cardId = useId();
  const lang = useUiLanguage();
  const t = messages[lang];
  const entry = wikiEntry(slug);
  const open = pos !== null;

  const cancelHide = useCallback(() => {
    if (hideTimer.current !== null) {
      clearTimeout(hideTimer.current);
      hideTimer.current = null;
    }
  }, []);
  const armHide = useCallback(() => {
    cancelHide();
    hideTimer.current = window.setTimeout(() => {
      hideTimer.current = null;
      setPos(null);
    }, HOVER_GRACE_MS);
  }, [cancelHide]);
  const dismiss = useCallback(() => {
    if (showTimer.current !== null) {
      clearTimeout(showTimer.current);
      showTimer.current = null;
    }
    cancelHide();
    setPos(null);
  }, [cancelHide]);

  /* Anchor wiring: dwell on hover AND focus (symmetric), grace on leave and
   * blur, immediate close on click — the click opens the full panel, so the
   * preview yields. The delay is read when the timer is armed, so a change
   * in the Display menu applies to the very next hover. */
  useEffect(() => {
    const anchor = anchorRef.current;
    if (!anchor || !entry) return;
    const show = () => {
      const rect = anchor.getBoundingClientRect();
      setPos({ top: rect.bottom + ANCHOR_GAP, left: rect.left });
    };
    const enter = () => {
      cancelHide();
      if (showTimer.current !== null) return;
      showTimer.current = window.setTimeout(() => {
        showTimer.current = null;
        show();
      }, delayMs ?? loadTooltipDelayMs());
    };
    const leave = () => {
      if (showTimer.current !== null) {
        clearTimeout(showTimer.current);
        showTimer.current = null;
      }
      armHide();
    };
    anchor.addEventListener("mouseenter", enter);
    anchor.addEventListener("mouseleave", leave);
    anchor.addEventListener("focus", enter);
    anchor.addEventListener("blur", leave);
    anchor.addEventListener("click", dismiss);
    return () => {
      anchor.removeEventListener("mouseenter", enter);
      anchor.removeEventListener("mouseleave", leave);
      anchor.removeEventListener("focus", enter);
      anchor.removeEventListener("blur", leave);
      anchor.removeEventListener("click", dismiss);
      dismiss();
    };
  }, [entry, delayMs, anchorRef, armHide, cancelHide, dismiss]);

  /* While open: Esc dismisses, scroll/resize would desync the fixed anchor
   * so they close, and the card describes its trigger for screen readers. */
  useEffect(() => {
    if (!open) return;
    const anchor = anchorRef.current;
    anchor?.setAttribute("aria-describedby", cardId);
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      /* Capture + stopPropagation: Esc dismisses ONLY the topmost hover
       * content, not the wiki panel listening for the same key beneath. */
      event.stopPropagation();
      dismiss();
    };
    const close = () => dismiss();
    document.addEventListener("keydown", onKey, true);
    window.addEventListener("scroll", close, true);
    window.addEventListener("resize", close);
    return () => {
      anchor?.removeAttribute("aria-describedby");
      document.removeEventListener("keydown", onKey, true);
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("resize", close);
    };
  }, [open, anchorRef, cardId, dismiss]);

  /* Clamp within the viewport once the rendered size is known — the card has
   * no fixed width (i18n), only a max-w, so measure rather than estimate. */
  useLayoutEffect(() => {
    if (!open) return;
    const card = cardRef.current;
    const anchor = anchorRef.current;
    if (!card || !anchor) return;
    const box = card.getBoundingClientRect();
    const at = anchor.getBoundingClientRect();
    let top = at.bottom + ANCHOR_GAP;
    let left = at.left;
    if (left + box.width > window.innerWidth - VIEWPORT_PAD)
      left = Math.max(VIEWPORT_PAD, window.innerWidth - VIEWPORT_PAD - box.width);
    if (
      top + box.height > window.innerHeight - VIEWPORT_PAD &&
      at.top - box.height - ANCHOR_GAP >= VIEWPORT_PAD
    )
      top = at.top - box.height - ANCHOR_GAP; /* flip above the chip */
    setPos((prev) =>
      prev && prev.top === top && prev.left === left ? prev : { top, left },
    );
  }, [open, anchorRef]);

  if (!entry || !pos) return null;

  return createPortal(
    <div
      ref={cardRef}
      id={cardId}
      role="tooltip"
      className="fixed z-50 max-w-[34ch] bg-popover/95 backdrop-blur border border-border/60 rounded-md px-3 py-2 text-xs shadow-xl"
      style={{ top: pos.top, left: pos.left }}
      onMouseEnter={cancelHide}
      onMouseLeave={armHide}
    >
      <div className="flex items-center gap-2 mb-1">
        <span className="text-foreground/90 font-medium">{entry.term}</span>
        <span
          className={`px-1.5 py-px rounded text-[11px] font-medium ${TIER_TONE[entry.tier]}`}
        >
          {entry.tier}
        </span>
      </div>
      <p className="text-foreground/70 leading-relaxed">
        {localizeEntry(entry, lang).sentence}
      </p>
      <p className="text-foreground/35 mt-1 text-[12px]">{t.wiki.clickForMore}</p>
    </div>,
    document.body,
  );
}
