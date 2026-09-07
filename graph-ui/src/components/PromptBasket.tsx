/* The prompt composer: a basket of cited entities (symbols, regions, flows,
 * questions) that becomes a precise prompt for the user's coding agent —
 * the bridge from what the human learned to what the agent is told. */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { composePrompt, basketKey, type BasketItem } from "../lib/composer";

interface BasketState {
  items: BasketItem[];
  add: (item: BasketItem) => void;
  remove: (key: string) => void;
  clear: () => void;
  open: boolean;
  setOpen: (open: boolean) => void;
}

const BasketContext = createContext<BasketState | null>(null);

const STORAGE_KEY = "cbm-atlas-basket";

export function PromptBasketProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<BasketItem[]>(() => {
    try {
      const raw = sessionStorage.getItem(STORAGE_KEY);
      return raw ? (JSON.parse(raw) as BasketItem[]) : [];
    } catch {
      return [];
    }
  });
  const [open, setOpen] = useState(false);

  useEffect(() => {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(items));
    } catch {
      /* session storage unavailable — the basket still works in memory */
    }
  }, [items]);

  const add = useCallback((item: BasketItem) => {
    setItems((prev) =>
      prev.some((existing) => basketKey(existing) === basketKey(item))
        ? prev
        : [...prev, item],
    );
    setOpen(true);
  }, []);
  const remove = useCallback((key: string) => {
    setItems((prev) => prev.filter((item) => basketKey(item) !== key));
  }, []);
  const clear = useCallback(() => setItems([]), []);

  const value = useMemo(
    () => ({ items, add, remove, clear, open, setOpen }),
    [items, add, remove, clear, open],
  );
  return <BasketContext.Provider value={value}>{children}</BasketContext.Provider>;
}

export function usePromptBasket(): BasketState {
  const ctx = useContext(BasketContext);
  if (!ctx) throw new Error("usePromptBasket outside PromptBasketProvider");
  return ctx;
}

/* The standard "cite this" affordance. */
export function AddToPromptButton({ item, small }: { item: BasketItem; small?: boolean }) {
  const basket = usePromptBasket();
  const inBasket = basket.items.some((existing) => basketKey(existing) === basketKey(item));
  return (
    <button
      onClick={() => basket.add(item)}
      disabled={inBasket}
      className={`${small ? "px-1.5 py-0.5 text-[12px]" : "px-2.5 py-1 text-[13px]"} rounded-md font-medium transition-colors ${
        inBasket
          ? "bg-popover text-foreground/40 cursor-default"
          : "bg-primary/15 text-primary hover:bg-primary/25"
      }`}
      title={inBasket ? "Already in the prompt" : "Cite this in the prompt composer"}
    >
      {inBasket ? "✓ cited" : "+ prompt"}
    </button>
  );
}

function itemLabel(item: BasketItem): string {
  switch (item.kind) {
    case "symbol":
      return item.qualified_name ?? item.name;
    case "region":
      return `region ${item.name}`;
    case "flow":
      return `flow ${item.label}`;
    case "question":
      return item.question;
  }
}

/* The drawer: goal text + cited items + the composed prompt with copy. */
export function PromptBasketDrawer({ project }: { project: string | null }) {
  const basket = usePromptBasket();
  const [goal, setGoal] = useState("");
  const [copied, setCopied] = useState(false);
  const prompt = useMemo(
    () => composePrompt(project ?? "?", goal, basket.items),
    [project, goal, basket.items],
  );

  useEffect(() => {
    if (!copied) return;
    const timer = setTimeout(() => setCopied(false), 1500);
    return () => clearTimeout(timer);
  }, [copied]);

  if (!basket.open) {
    return (
      <button
        onClick={() => basket.setOpen(true)}
        className="fixed bottom-4 right-4 z-40 px-3 py-2 rounded-full bg-primary/20 text-primary text-[12px] font-medium border border-primary/30 backdrop-blur-md hover:bg-primary/30 transition-colors shadow-lg"
      >
        Prompt {basket.items.length > 0 ? `(${basket.items.length})` : ""}
      </button>
    );
  }

  return (
    <div className="fixed bottom-0 right-0 z-40 w-full sm:w-[440px] max-h-[70vh] bg-card/95 border-l border-t border-border/60 rounded-tl-xl backdrop-blur-xl flex flex-col shadow-2xl">
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-border/40">
        <span className="text-[12px] font-semibold text-foreground/80">
          Prompt composer <span className="text-foreground/45">({basket.items.length} cited)</span>
        </span>
        <div className="flex gap-2 items-center">
          {basket.items.length > 0 && (
            <button
              onClick={basket.clear}
              className="text-[13px] text-foreground/45 hover:text-foreground/60 transition-colors"
            >
              clear
            </button>
          )}
          <button
            onClick={() => basket.setOpen(false)}
            className="text-foreground/40 hover:text-foreground/60 text-[15px] leading-none transition-colors"
          >
            ×
          </button>
        </div>
      </div>

      <div className="px-4 py-2.5 border-b border-border/30">
        <input
          type="text"
          placeholder="What do you want your agent to do?"
          value={goal}
          onChange={(event) => setGoal(event.target.value)}
          className="w-full bg-popover border border-border rounded-md px-3 py-1.5 text-[12px] text-foreground placeholder-foreground/25 outline-none focus:border-primary/40 transition-all"
        />
      </div>

      <div className="px-4 py-2 border-b border-border/30 max-h-[120px] overflow-y-auto">
        {basket.items.length === 0 ? (
          <p className="text-[13px] text-foreground/40 py-1">
            Cite symbols, regions, flows or questions with “+ prompt” anywhere in Atlas.
          </p>
        ) : (
          basket.items.map((item) => (
            <div key={basketKey(item)} className="flex items-center gap-2 py-[3px]">
              <span className="text-[12px] uppercase tracking-wider text-foreground/40 w-14 shrink-0">
                {item.kind}
              </span>
              <span className="text-[13px] font-mono text-foreground/60 truncate flex-1">
                {itemLabel(item)}
              </span>
              <button
                onClick={() => basket.remove(basketKey(item))}
                className="text-foreground/35 hover:text-foreground/50 text-[12px] transition-colors"
              >
                ×
              </button>
            </div>
          ))
        )}
      </div>

      <pre className="flex-1 min-h-[120px] overflow-auto px-4 py-3 text-[12px] leading-relaxed font-mono text-foreground/70 whitespace-pre-wrap">
        {prompt}
      </pre>

      <div className="px-4 py-2.5 border-t border-border/40 flex justify-end">
        <button
          onClick={() => {
            navigator.clipboard?.writeText(prompt).then(() => setCopied(true));
          }}
          className="px-3 py-1.5 rounded-md bg-primary/20 text-primary text-[12px] font-medium hover:bg-primary/30 transition-colors"
        >
          {copied ? "Copied ✓" : "Copy prompt"}
        </button>
      </div>
    </div>
  );
}
