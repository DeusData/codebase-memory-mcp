import { ScrollArea } from "@/components/ui/scroll-area";
import { useUiMessages } from "../lib/i18n";
import { localizeRegionWhy } from "../lib/regions";
import type { Region } from "../lib/types";

interface RegionPanelProps {
  region: Region;
  onOpen: (region: Region) => void;
  onClose: () => void;
}

/* Detail panel for a selected region body: what it is, why it got its name,
 * its representative symbols, and the door into its full-detail layout. */
export function RegionPanel({ region, onOpen, onClose }: RegionPanelProps) {
  const t = useUiMessages();
  return (
    <div className="w-full bg-card/95 backdrop-blur-xl flex flex-col h-full min-h-0 overflow-hidden">
      <div className="px-4 pt-4 pb-3 border-b border-border/30">
        <div className="flex items-start justify-between gap-2 mb-2">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 mb-1.5">
              <span
                className="w-2.5 h-2.5 rounded-full shrink-0"
                style={{ backgroundColor: region.color }}
              />
              <h3 className="text-[13px] font-semibold text-foreground truncate">
                {region.name}
              </h3>
            </div>
            <span className="inline-block px-2 py-0.5 rounded-md text-[12px] font-medium bg-surface-3 text-foreground/60">
              Region
            </span>
          </div>
          <button
            onClick={onClose}
            className="text-foreground/35 hover:text-foreground/50 transition-colors text-[16px] leading-none p-1"
          >
            ×
          </button>
        </div>

        {region.why && (
          <p className="text-[13px] text-foreground/40 mt-2 leading-relaxed">
            {localizeRegionWhy(region.why, t.regions)}
          </p>
        )}

        <button
          onClick={() => onOpen(region)}
          className="mt-3 px-2.5 py-1 rounded-md bg-primary/15 text-primary text-[13px] font-medium hover:bg-primary/25 transition-colors"
        >
          Open region →
        </button>

        <div className="flex gap-5 mt-3">
          {[
            { label: "Symbols", value: region.members.toLocaleString("en-US") },
            { label: "Files", value: region.files.toLocaleString("en-US") },
            { label: "Cohesion", value: region.cohesion.toFixed(2) },
          ].map((stat) => (
            <div key={stat.label}>
              <p className="text-[12px] text-foreground/40 uppercase tracking-widest">
                {stat.label}
              </p>
              <p className="text-[16px] font-semibold tabular-nums text-foreground">
                {stat.value}
              </p>
            </div>
          ))}
        </div>
      </div>

      <ScrollArea className="flex-1 min-h-0">
        <div className="px-4 py-3">
          <p className="text-[13px] font-medium text-foreground/40 mb-2">
            Most connected
          </p>
          <div className="space-y-px">
            {region.top_nodes.map((name) => (
              <p
                key={name}
                className="px-2 py-[4px] text-[13px] font-mono text-foreground/55 truncate"
              >
                {name}
              </p>
            ))}
            {region.top_nodes.length === 0 && (
              <p className="text-[12px] text-foreground/35 py-4 text-center">
                No callable members
              </p>
            )}
          </div>
        </div>
      </ScrollArea>
    </div>
  );
}
