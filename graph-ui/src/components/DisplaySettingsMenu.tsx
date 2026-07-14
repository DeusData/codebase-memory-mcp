import { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  DEFAULT_DISPLAY_SETTINGS,
  DISPLAY_LIMITS,
  type DisplaySettings,
} from "../lib/density";

interface DisplaySettingsMenuProps {
  settings: DisplaySettings;
  onChange: (next: DisplaySettings) => void;
}

interface SliderRowProps {
  label: string;
  hint: string;
  value: number;
  min: number;
  max: number;
  onChange: (value: number) => void;
}

function SliderRow({ label, hint, value, min, max, onChange }: SliderRowProps) {
  return (
    <label className="block">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[11px] text-foreground/70">{label}</span>
        <span className="text-[10px] font-mono text-cyan-300/70 tabular-nums">
          {value.toFixed(2)}×
        </span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        step={0.05}
        value={value}
        onChange={(e) => onChange(parseFloat(e.target.value))}
        className="w-full accent-cyan-400 cursor-pointer"
        aria-label={`${label} (${hint})`}
      />
      <p className="text-[9px] text-foreground/30 mt-0.5">{hint}</p>
    </label>
  );
}

/* Contrast / brightness controls for the 3D graph. These ride on top of the
 * automatic density compensation — the defaults already adapt to graph size,
 * so 1.00× is "auto"; the sliders let the user push it. */
export function DisplaySettingsMenu({
  settings,
  onChange,
}: DisplaySettingsMenuProps) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  /* Close on outside click / Escape */
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const set = (patch: Partial<DisplaySettings>) =>
    onChange({ ...settings, ...patch });

  const isDefault =
    settings.edgeBrightness === DEFAULT_DISPLAY_SETTINGS.edgeBrightness &&
    settings.nodeGlow === DEFAULT_DISPLAY_SETTINGS.nodeGlow &&
    settings.bloom === DEFAULT_DISPLAY_SETTINGS.bloom;

  return (
    <div ref={rootRef} className="relative">
      <Button
        variant="outline"
        size="sm"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        aria-haspopup="dialog"
        title="Contrast & brightness"
      >
        Display{!isDefault && <span className="ml-1 text-cyan-300">•</span>}
      </Button>

      {open && (
        <div
          role="dialog"
          aria-label="Display settings"
          className="absolute top-10 right-0 w-64 p-4 rounded-lg border border-border/60 bg-[#0b1920]/95 backdrop-blur-md shadow-xl z-20 space-y-3.5"
        >
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-medium text-foreground/50 uppercase tracking-widest">
              Contrast
            </span>
            <button
              onClick={() => onChange(DEFAULT_DISPLAY_SETTINGS)}
              className="text-[10px] text-primary/70 hover:text-primary transition-colors disabled:opacity-30"
              disabled={isDefault}
            >
              Reset
            </button>
          </div>

          <SliderRow
            label="Edge brightness"
            hint="Dim the web of links on dense graphs"
            value={settings.edgeBrightness}
            min={DISPLAY_LIMITS.edgeBrightness.min}
            max={DISPLAY_LIMITS.edgeBrightness.max}
            onChange={(edgeBrightness) => set({ edgeBrightness })}
          />
          <SliderRow
            label="Node glow"
            hint="Halo boost around each node"
            value={settings.nodeGlow}
            min={DISPLAY_LIMITS.nodeGlow.min}
            max={DISPLAY_LIMITS.nodeGlow.max}
            onChange={(nodeGlow) => set({ nodeGlow })}
          />
          <SliderRow
            label="Bloom"
            hint="Overall glow bloom strength"
            value={settings.bloom}
            min={DISPLAY_LIMITS.bloom.min}
            max={DISPLAY_LIMITS.bloom.max}
            onChange={(bloom) => set({ bloom })}
          />

          <p className="text-[9px] text-foreground/30 pt-1 border-t border-border/30">
            1.00× follows the automatic density compensation. Lower the
            edge/glow/bloom values when a large graph washes out to white.
          </p>
        </div>
      )}
    </div>
  );
}
